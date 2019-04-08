package cachewb

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"
)

type CacheContainer struct {
	storage         storage
	config          Config
	name            string // table name
	lockUpdate      bool
	itemType        interface{}
	items           map[interface{}]interface{}
	itemsGroupIndex map[interface{}][]string
	chanUpdates     chan interface{}
	chanInserts     chan interface{}
	mu              sync.RWMutex
	muIndex         sync.RWMutex
	muQ             sync.RWMutex
}

func newContainer(tbl string, cfg Config, containerType interface{}) *CacheContainer {
	var m CacheContainer
	t := reflect.TypeOf(containerType)
	if t.NumField() == 0 || t.Field(0).Name != "EmbedME" {
		panic(fmt.Sprintf("Coundn't find 'EmbedME' in %s. Please Add 'cachewb.EmbedME' at top of %s", t.Name(), t.Name()))
	}
	m.itemType = containerType
	m.config = cfg
	m.name = tbl
	m.storage = newStorage(tbl, cfg, containerType)
	m.items = make(map[interface{}]interface{})
	m.itemsGroupIndex = make(map[interface{}][]string)
	m.chanUpdates = make(chan interface{}, 1000)
	m.chanInserts = make(chan interface{}, 10000)
	m.setManager()
	return &m
}

func (c *CacheContainer) setManager() {
	go c.workerConsumerUpdater()
	go c.workerMaintainer()
	go c.workerInserts()
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
						//fmt.Println("Hello morteza Lass Access", embedMe.lastAccess)
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

func (c *CacheContainer) workerInserts() {
	var buffer []interface{}
	buffer = make([]interface{}, 0)
	for {
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
			if len(buffer) >= c.storage.getInsertLimit() {
				res, e := c.storage.insert(buffer...)
				fmt.Println(fmt.Sprintf("workerInserts found %d items. res:%s , error:%s", len(buffer), res, e))
				buffer = make([]interface{}, 0)
			}
		}
	}
}

func (c *CacheContainer) Flush(l bool) {
	c.mu.Lock()
	defer func() {
		if l {
			c.lockUpdate = false
		}
		c.mu.Unlock()
	}()
	if l {
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
}

func (c *CacheContainer) getByLock(value interface{}) (interface{}, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	r, ok := c.items[value]
	return r, ok
}

func (c *CacheContainer) getByLockFromGroupIndex(value interface{}) ([]string, bool) {
	c.muIndex.Lock()
	defer c.muIndex.Unlock()
	r, ok := c.itemsGroupIndex[value]
	return r, ok
}

func (c *CacheContainer) getValueStr(a ...interface{}) string {
	var s []string
	for _, item := range a {
		s = append(s, fmt.Sprintf("%v", item))
	}
	return strings.Join(s, "-")
}

func (c *CacheContainer) Get(values ...interface{}) (interface{}, error) {
	valStr := c.getValueStr(values...)
	if item, ok := c.getByLock(valStr); ok {
		elem := reflect.ValueOf(item).Elem()
		if elem.Kind() == reflect.Struct {
			eme := elem.FieldByName("EmbedME")
			if eme.IsValid() {
				embedMe := eme.Interface().(EmbedME)
				embedMe.lastAccess = time.Now()
				eme.Set(reflect.ValueOf(embedMe))
			}
		}
		return item, nil
	} else {
		res, e := c.storage.get(values...)
		if e == nil {
			elem := reflect.ValueOf(res).Elem()
			if elem.Kind() == reflect.Struct {
				eme := elem.FieldByName("EmbedME")
				if eme.IsValid() {
					embedMe := eme.Interface().(EmbedME)
					embedMe.lastAccess = time.Now()
					embedMe.container = c
					embedMe.parent = res
					eme.Set(reflect.ValueOf(embedMe))
				}
			}
			c.mu.Lock()
			defer c.mu.Unlock()
			c.items[valStr] = res
		}
		return res, e
	}
}

func (c *CacheContainer) GetList(values ...interface{}) ([]interface{}, error) {
	valStr := c.getValueStr(values...)
	if item, ok := c.getByLockFromGroupIndex(valStr); ok {
		var a []interface{}
		for _, jj := range item {
			if v, ok := c.getByLock(jj); ok {
				a = append(a, v)
			}
		}
		return a, nil
	} else {
		var a []interface{}
		var b []string
		res, e := c.storage.getList(values...)
		if e == nil {
			c.mu.Lock()
			defer c.mu.Unlock()
			for i, item := range res {
				elem := reflect.ValueOf(item).Elem()
				if elem.Kind() == reflect.Struct {
					eme := elem.FieldByName("EmbedME")
					if eme.IsValid() {
						embedMe := eme.Interface().(EmbedME)
						embedMe.lastAccess = time.Now()
						embedMe.container = c
						embedMe.parent = item
						eme.Set(reflect.ValueOf(embedMe))
					}
				}
				a = append(a, item)
				k := fmt.Sprintf("%s-multiRec-%d", valStr, i)
				c.items[k] = item
				b = append(b, k)
			}
			c.itemsGroupIndex[valStr] = b
		}
		return a, e
	}
}

func (c *CacheContainer) Insert(in ...interface{}) (interface{}, error) {
	if c.lockUpdate {
		return nil, errors.New(fmt.Sprintf("Updates are locked in container of '%s', Please try later", c.name))
	}
	return c.storage.insert(in...)
}

func (c *CacheContainer) InsertAsync(in ...interface{}) {
	go c.addToChanInserts(in)
}

func (c *CacheContainer) Remove(values ...interface{}) (interface{}, error) {
	//valStr := c.getValueStr(values...)
	if c.lockUpdate {
		return nil, errors.New(fmt.Sprintf("Updates are locked in container of '%s', Please try later", c.name))
	}
	c.RemoveFromCache(values...)
	return c.storage.remove(values...)
}

func (c *CacheContainer) RemoveFromCache(values ...interface{}) {
	valStr := c.getValueStr(values...)
	if c.lockUpdate {
		fmt.Println(fmt.Sprintf("Updates are locked in container of '%s', Please try later", c.name))
	}
	if g, ok := c.getByLockFromGroupIndex(valStr); ok {
		for _, m := range g {
			delete(c.items, m)
		}
		delete(c.itemsGroupIndex, valStr)

	} else {
		delete(c.items, valStr)
	}
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

func (c *EmbedME) UpdateStorage() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.updates > 0 {
		fmt.Println("Let update, updates= ", c.updates)
		c.container.storage.update(c.parent)
		c.updates = 0
	}
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
