package cachewb

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"
)

const AsyncInsertLatency  = 5

type CacheContainer struct {
	storage        storage
	config         Config
	name           string
	uniqueIdentity string
	lockUpdate     bool
	isView         bool
	itemType       interface{}
	items          map[interface{}]interface{}
	queryIndex     map[string]map[string]*cacheIndexEntry
	chanUpdates    chan interface{}
	chanInserts    chan interface{}
	mu             sync.RWMutex
	muIndex        sync.RWMutex
}

func newContainer(containerName string, cfg Config, containerType interface{}) *CacheContainer {
	var m CacheContainer
	t := reflect.TypeOf(containerType)
	if t.NumField() == 0 || t.Field(0).Name != "EmbedME" {
		panic(fmt.Sprintf("container:%s. couldn't find 'EmbedME' in %s. Please Add 'cachewb.EmbedME' at top of %s", containerName, t.Name(), t.Name()))
	}

	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if tag := f.Tag.Get("uniqueIdentity"); tag != "" {
			m.uniqueIdentity = f.Name
		}
	}
	if m.uniqueIdentity == ""{
		panic(fmt.Sprintf("container:%s. couldn't find 'uniqueIdentity' field. Please tagged a unique field as 'uniqueIdentity'. " +
			"\nFor example: " +
			"\n   Id       int64     `storage:\"id\" uniqueIdentity:\"1\"`", containerName))
	}
	m.itemType = containerType
	m.config = cfg
	// set default values of config
	if m.config.AsyncInsertLatency == 0{
		m.config.AsyncInsertLatency = AsyncInsertLatency
	}
	m.name = containerName
	m.storage = newStorage(containerName, "", cfg, containerType)
	m.items = make(map[interface{}]interface{})
	m.queryIndex = make(map[string]map[string]*cacheIndexEntry)
	m.chanUpdates = make(chan interface{}, 1000)
	m.chanInserts = make(chan interface{}, 10000)
	m.setManager()
	return &m
}

func newViewContainer(containerName string, viewQuery string , cfg Config, containerType interface{}) *CacheContainer {
	var m CacheContainer
	t := reflect.TypeOf(containerType)
	if t.NumField() == 0 || t.Field(0).Name != "EmbedME" {
		panic(fmt.Sprintf("viewContainer:%s. coundn't find 'EmbedME' in %s. Please Add 'cachewb.EmbedME' at top of %s", containerName, t.Name(), t.Name()))
	}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if tag := f.Tag.Get("uniqueIdentity"); tag != "" {
			m.uniqueIdentity = f.Name
		}
	}
	if m.uniqueIdentity == ""{
		panic(fmt.Sprintf("viewContainer:%s. couldn't find 'uniqueIdentity' field. Please tagged a unique field as 'uniqueIdentity'. " +
			"\nFor example: " +
			"\n   Id       int64     `storage:\"id\" uniqueIdentity:\"1\"`", containerName))
	}
	m.itemType = containerType
	m.config = cfg
	// set default values of config
	if m.config.AsyncInsertLatency == 0{
		m.config.AsyncInsertLatency = AsyncInsertLatency
	}
	m.name = containerName
	m.isView = true
	m.storage = newStorage("", viewQuery, cfg, containerType)
	m.items = make(map[interface{}]interface{})
	m.queryIndex = make(map[string]map[string]*cacheIndexEntry)
	m.chanUpdates = make(chan interface{}, 1000)
	m.chanInserts = make(chan interface{}, 10000)
	m.setManager()
	return &m
}

func (c *CacheContainer) setManager() {
	if !c.isView{
		go c.workerConsumerUpdater()
		go c.workerInserts()
	}
	go c.workerMaintainer()
	go c.workerQueryIndexMaintainer()
}

func (c *CacheContainer) addToChanUpdates(a interface{}) error {
	select {
	case c.chanUpdates <- a:
		return nil
	default:
		return errors.New(fmt.Sprintf("channel of updates is full, so following item will be lost! %s", a))
	}
}

func (c *CacheContainer) addToChanInserts(a interface{}) error {
	select {
	case c.chanInserts <- a:
		return nil
	default:
		return errors.New(fmt.Sprintf("Channel of inserts is full, So following item will be lost! %s", a))
	}
}

func (c *CacheContainer) workerConsumerUpdater() {
	for {
		select {
		case item := <-c.chanUpdates:
			fmt.Println("Hello Worker. I want update:", item)
			reflect.ValueOf(item).MethodByName("UpdateStorage").Call([]reflect.Value{})
		}
	}
}

func (c *CacheContainer) workerMaintainer() {
	for {
		t := time.After(time.Second * time.Duration(c.config.Interval))
		select {
		case <-t:
			func() {
				defer func() {
					if err := recover(); err != nil {
						fmt.Println("Error in worker") // TODO remind for more developing
					}
				}()
				c.mu.Lock()
				defer c.mu.Unlock()
				for n, item := range c.items {
					val := reflect.ValueOf(item)
					elem := val.Elem()

					eme := elem.FieldByName("EmbedME")
					if eme.IsValid() {
						embedMe := eme.Interface().(EmbedME)
						if ! c.isView {
							if embedMe.updates > c.config.CacheWriteLatencyCount {
								e := c.addToChanUpdates(item)
								if e != nil {
									// TODO  handle error
								}

							} else if embedMe.updates > 0 &&
								time.Since(embedMe.lastUpdate).Seconds() > float64(c.config.CacheWriteLatencyTime) {
								e := c.addToChanUpdates(item)
								if e != nil {
									// TODO  handle error
								}
							}
						}
						if embedMe.ttlReached() {
							fmt.Println("TTL Reached")
							c.RemoveFromCache(n)
						}
					}
				}
			}()
		}
	}
}

func (c *CacheContainer) workerQueryIndexMaintainer() {
	for {
		t := time.After(time.Second * time.Duration(c.config.Interval)) // TODO change time
		select {
		case <-t:
			func() {
				defer func() {
					if err := recover(); err != nil {
						fmt.Println("Error in worker") // TODO remind for more developing
					}
				}()
				c.muIndex.Lock()
				defer c.muIndex.Unlock()
				for _, item := range c.queryIndex {
					//fmt.Println("----", idxQueryName, item)
					for idxQueryValue, idxQueryResultIds := range item{
						//fmt.Println("-------", idxQueryValue, idxQueryResultIds.values)
						if time.Since(idxQueryResultIds.lastAccess).Seconds() > float64(c.config.AccessQueryIndexTTL) {		// TODO
							fmt.Println("****** Deleting", idxQueryValue)
							delete(item, idxQueryValue)					// todo use lock
						}
					}
				}
			}()
		}
	}
}

func (c *CacheContainer) workerInserts() {
	var buffer []interface{}
	buffer = make([]interface{}, 0)
	ft := time.Now()
	for {
		if len(buffer) > 0 && time.Since(ft).Seconds() > 1 {
			res, e := c.storage.insert(buffer...)
			fmt.Println(fmt.Sprintf("workerInserts  found %d items. res:%s , error:%s", len(buffer), res, e))
			buffer = make([]interface{}, 0)

		}

		t := time.After(time.Second * time.Duration(c.config.Interval))
		select {
		case <-t:
			if len(buffer) > 0 {
				res, e := c.storage.insert(buffer...)
				fmt.Println(fmt.Sprintf("workerInserts found %d items. res:%s , error:%s", len(buffer), res, e))
				buffer = make([]interface{}, 0)
			}

		case item := <-c.chanInserts:
			buffer = append(buffer, item.([]interface{})...)
			if len(buffer) == 1 {
				ft = time.Now()
			}
			if len(buffer) >= c.storage.getInsertLimit() {
				res, e := c.storage.insert(buffer...)
				fmt.Println(fmt.Sprintf("workerInserts found %d items. res:%s , error:%s", len(buffer), res, e))
				buffer = make([]interface{}, 0)
			}
		}
	}
}


func (c *CacheContainer) Flush(withLock bool) error{
	if c.isView{
		return errors.New(fmt.Sprintf("container of '%s' is view and views are read only, so there isn't permission for any write actions", c.name))
	}
	c.mu.Lock()
	defer func() {
		if withLock {
			c.lockUpdate = false
		}
		c.mu.Unlock()
	}()
	if withLock {
		c.lockUpdate = true
	}
	for _, item := range c.items {
		val := reflect.ValueOf(item)
		elem := val.Elem()
		f1 := elem.FieldByName("updates")

		if f1.Int() > 0 {
			val.MethodByName("UpdateStorage").Call([]reflect.Value{}) // TODO may this line need go
		}
	}
	return nil
}

func (c *CacheContainer) getFromIndex(idxQueryName string, idxQueryValue string) (*cacheIndexEntry, bool) {
	c.muIndex.Lock()
	defer c.muIndex.Unlock()
	if a, ok := c.queryIndex[idxQueryName]; ok {
		if b, okok := a[idxQueryValue]; okok {
			return b, true
		} else {
			return nil, false
		}
	} else {
		c.queryIndex[idxQueryName] = make(map[string]*cacheIndexEntry)
		return nil, false
	}
}

func (c *CacheContainer) getByLock(value interface{}) (interface{}, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	r, ok := c.items[value]
	return r, ok
}

func (c *CacheContainer) getValueStr(a ...interface{}) string {
	var s []string
	for _, item := range a {
		s = append(s, fmt.Sprintf("%v", item))
	}
	return strings.Join(s, "-")
}

func (c *CacheContainer) getKeysValues(m map[string]interface{}) (keys []string, values []interface{})  {
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		values = append(values, m[k])
	}
	return
}
func (c *CacheContainer) getIndexQuery(args ...string) (r string){

	if len(args) > 0 {
		r = strings.Join(args, "-")
	}else{
		r = args[0]
	}
	return r
}

// Return an object from cache. This method check cache first,
// and then look in the storage, if not found.
// It return result for uniqueIdentityValue, if it was one, if there were
// more than one result, you should use Get()
func (c *CacheContainer) GetOne(uniqueIdentityValue interface{}) (interface{}, error) {
	r, ee := c.Get(map[string]interface{}{c.uniqueIdentity: uniqueIdentityValue})
	if ee != nil {
		return nil, ee
	}
	switch len(r) {
	case 1:
		return r[0], nil
	case 0:
		return nil, nil
	default:
		return nil, errors.New(fmt.Sprintf("there are many result for %s=%s. expected 1 result, found %d." +
			" make sure uniqueIdentity of %s is unique" ,
			c.uniqueIdentity, uniqueIdentityValue, len(r), c.uniqueIdentity))
	}
}

func (c *CacheContainer) GetNEW(m ...interface{}) ([]interface{}, error) {
	return nil, nil
}

func (c *CacheContainer) findInCache(idxQueryName string, idxQueryValue string) ([]interface{}, bool) {
	if item, ok := c.getFromIndex(idxQueryName, idxQueryValue); ok {
		var a []interface{}
		item.lastAccess = time.Now()
		for _, jj := range item.values {
			if v, ok := c.getByLock(jj); ok {
				a = append(a, v)
			} else {
				return nil, false
			}
		}
		return a, true
	}
	return nil, false
}

func (c *CacheContainer) normalizeAndSaveInCache(idxQueryName string, idxQueryValue string,  d []interface{}) ([]interface{}, error) {
	var a []interface{}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.muIndex.Lock()
	defer c.muIndex.Unlock()
	var idxQueryResultIds []string
	for _, item := range d {
		idString := ""
		elem := reflect.ValueOf(item).Elem()
		if elem.Kind() == reflect.Struct {
			usedId := elem.FieldByName(c.uniqueIdentity)
			if usedId.IsValid() {
				idString = fmt.Sprintf("%v", usedId.Interface())
				idxQueryResultIds = append(idxQueryResultIds, idString)
			}
			eme := elem.FieldByName("EmbedME")
			if eme.IsValid() {
				embedMe := eme.Interface().(EmbedME)
				embedMe.lastAccess = time.Now()
				embedMe.container = c
				embedMe.parent = item
				eme.Set(reflect.ValueOf(embedMe))
			}
		}
		if idString != "" {
			if existedItem, ok := c.items[idString]; ok {
				a = append(a, existedItem)
			} else {
				a = append(a, item)
				c.items[idString] = item
			}
		}
	}
	c.queryIndex[idxQueryName][idxQueryValue] = &cacheIndexEntry{values: idxQueryResultIds, lastAccess: time.Now()}
	return a, nil
}

func (c *CacheContainer) Get(m map[string]interface{}) ([]interface{}, error) {
	keys, values := c.getKeysValues(m)
	idxQueryName := c.getIndexQuery(keys...)
	idxQueryValue := c.getValueStr(values...)

	if res, ok := c.findInCache(idxQueryName, idxQueryValue); ok{
		return res, nil
	}
	res, e := c.storage.getOld(keys, values)
	if e == nil && len(res) > 0{
		return c.normalizeAndSaveInCache(idxQueryName, idxQueryValue, res)
	}
	return nil, nil
}

func (c *CacheContainer) GetBySquirrel(squirrelArgs ...interface{}) ([]interface{}, error) {
	idxQueryName := c.getIndexQuery(squirrelArgs[0].(string))
	idxQueryValue := c.getValueStr(squirrelArgs[1].([]interface{})...)
	if res, ok := c.findInCache(idxQueryName, idxQueryValue); ok{
		return res, nil
	}
	res, e := c.storage.getBySquirrel(squirrelArgs...)
	if e == nil && len(res) > 0{
		return c.normalizeAndSaveInCache(idxQueryName, idxQueryValue, res)
	}
	return nil, nil
}

//func (c *CacheContainer) GetOlddddddd(m map[string]interface{}) ([]interface{}, error) {
//	keys, values := c.getKeysValues(m)
//	idxQueryName := c.getIndexQuery(keys...)
//	idxQueryValue := c.getValueStr(values...)
//
//	if item, ok := c.getFromIndex(idxQueryName, idxQueryValue); ok {
//		//fmt.Println("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", "got from cache", idxQueryName, idxQueryValue)
//		var a []interface{}
//		gg := true
//		// extend live time
//		item.lastAccess = time.Now()
//		for _, jj := range item.values {
//			if v, ok := c.getByLock(jj); ok {
//				a = append(a, v)
//			} else {
//				gg = false
//				break
//			}
//		}
//		if gg {
//			return a, nil
//		}
//	}
//	//fmt.Println("BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB", "got from storage", idxQueryName, idxQueryValue)
//	var a []interface{}
//	if len(res) > 0 {
//		if e == nil {
//			c.mu.Lock()
//			defer c.mu.Unlock()
//			c.muIndex.Lock()
//			defer c.muIndex.Unlock()
//			var idxQueryResultIds []string
//			for _, item := range res {
//				idString := ""
//
//				elem := reflect.ValueOf(item).Elem()
//				if elem.Kind() == reflect.Struct {
//					usedId := elem.FieldByName(c.uniqueIdentity)
//					if usedId.IsValid() {
//						idString = fmt.Sprintf("%v", usedId.Interface())
//						idxQueryResultIds = append(idxQueryResultIds, idString)
//
//					}
//					eme := elem.FieldByName("EmbedME")
//					if eme.IsValid() {
//						embedMe := eme.Interface().(EmbedME)
//						embedMe.lastAccess = time.Now()
//						embedMe.container = c
//						embedMe.parent = item
//						eme.Set(reflect.ValueOf(embedMe))
//					}
//				}
//				if idString != "" {
//					if existedItem, ok := c.items[idString]; ok {
//						a = append(a, existedItem)
//					} else {
//						a = append(a, item)
//						c.items[idString] = item
//					}
//				}
//			}
//			c.queryIndex[idxQueryName][idxQueryValue] = &cacheIndexEntry{values: idxQueryResultIds, lastAccess: time.Now()}
//		}
//	} else {
//		// TODO      handle empty query
//	}
//	return a, e
//}

// Insert object(s) to container. the object will add to database synchronously,
func (c *CacheContainer) Insert(in ...interface{}) (interface{}, error) {
	if c.isView{
		return nil, errors.New(fmt.Sprintf("container of '%s' is view and views are read only, so there isn't permission for any write actions", c.name))
	}
	if c.lockUpdate {
		return nil, errors.New(fmt.Sprintf("Updates are locked in container of '%s', Please try later", c.name))
	}
	return c.storage.insert(in...)
}

// Asynchronously insert object(s) to container. the object will bulk insert to database.
func (c *CacheContainer) InsertAsync(in ...interface{}) error {
	if c.isView{
		return errors.New(fmt.Sprintf("container of '%s' is view and views are read only, so there isn't permission for any write actions", c.name))
	}
	go c.addToChanInserts(in)
	return nil
}

// Remove from cache and storage by any keys (caution: This method may have some overload on storage)
// Unlike method `Remove`, You can use `RemoveIndirect` to remove from cache and storage by any keys.
// First, RemoveIndirect call storage.Get() by keys and values arguments, internally, to find uniqueIdentities.
// And then remove then by uniqueIdentities through the `Remove` method
func (c *CacheContainer) RemoveIndirect(keys []string, values[]interface{}) (interface{}, error) {
	if c.isView{
		return nil, errors.New(fmt.Sprintf("container of '%s' is view and views are read only, so there isn't permission for any write actions", c.name))
	}
	if c.lockUpdate {
		return nil, errors.New(fmt.Sprintf("Updates are locked in container of '%s', Please try later", c.name))
	}
	res, e := c.storage.getOld(keys, values)
	var uniqueIdentities [] interface{}
	if len(res) > 0 {
		if e == nil {
			for _, item := range res {
				elem := reflect.ValueOf(item).Elem()
				if elem.Kind() == reflect.Struct {
					usedId := elem.FieldByName(c.uniqueIdentity)
					if usedId.IsValid() {
						uniqueIdentities = append(uniqueIdentities,  usedId.Interface())
					}
				}
			}
		}
	}
	if len(uniqueIdentities) > 0 {
		c.RemoveFromCache(uniqueIdentities...)
		return c.storage.remove(uniqueIdentities...)
	}else {
		return map[string]interface{}{"LastInsertId": 0, "RowsAffected": 0}, nil
	}
}

// Remove from cache and storage just by uniqueIdentities
func (c *CacheContainer) Remove(uniqueIdentities ...interface{}) (interface{}, error) {
	if c.isView{
		return nil, errors.New(fmt.Sprintf("container of '%s' is view and views are read only, so there isn't permission for any write actions", c.name))
	}
	if c.lockUpdate {
		return nil, errors.New(fmt.Sprintf("Updates are locked in container of '%s', Please try later", c.name))
	}
	c.RemoveFromCache(uniqueIdentities...)
	return c.storage.remove(uniqueIdentities...)
}

func (c *CacheContainer) RemoveFromCache(uniqueIdentities ...interface{}) {
	valStr := c.getValueStr(uniqueIdentities...)
	if c.lockUpdate {
		fmt.Println(fmt.Sprintf("Updates are locked in container of '%s', Please try later", c.name))
	}
	delete(c.items, valStr)
}

type EmbedME struct {
	container  *CacheContainer
	parent     interface{}
	updates    int
	lastUpdate time.Time
	lastAccess time.Time
	mu         sync.RWMutex
}

func (c *EmbedME) IncUpdate() error {
	if c.container.isView{
		return errors.New(fmt.Sprintf("container of '%s' is view and views are read only, so there isn't permission for any write actions", c.container.name))
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.container.lockUpdate {
		fmt.Println(fmt.Sprintf("Updates are locked in container of '%s', Please try later", c.container.name))
		return errors.New(fmt.Sprintf("Updates are locked in container of '%s', Please try later", c.container.name))
	}

	c.updates ++
	c.lastUpdate = time.Now()
	c.lastAccess = time.Now()
	if c.updates >= c.container.config.CacheWriteLatencyCount {
		c.container.chanUpdates <- c.parent
	}
	return nil
}

func (c *EmbedME) UpdateStorage() error{
	if c.container.isView{
		return errors.New(fmt.Sprintf("container of '%s' is view and views are read only, so there isn't permission for any write actions", c.container.name))
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.updates > 0 {
		fmt.Println("Let update, updates= ", c.updates)
		c.container.storage.update(c.parent)
		c.updates = 0
	}
	return nil
}

func (c *EmbedME) ttlReached() bool {
	if c.container.config.AccessTTL != 0 &&
		int(time.Since(c.lastAccess).Seconds()) > c.container.config.AccessTTL &&
		c.updates == 0 {
		return true
	} else {
		return false
	}
}

type cacheIndexEntry struct {
	values []string
	lastAccess time.Time
}