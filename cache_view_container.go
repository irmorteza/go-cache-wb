package cachewb

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"
)

type CacheViewContainer struct {
	storage         storage
	config          Config
	name            string 				// table name
	itemType        interface{}
	items           map[interface{}]interface{}
	itemsGroupIndex map[interface{}][]string
	mu              sync.RWMutex
	muIndex         sync.RWMutex
}

func newViewContainer(name string, viewQuery string, cfg Config, containerType interface{}) *CacheViewContainer {
	var m CacheViewContainer
	t := reflect.TypeOf(containerType)
	if t.NumField() == 0 || t.Field(0).Name != "EmbedMEOnView" {
		panic(fmt.Sprintf("Coundn't find 'EmbedMEOnView' in %s. Please Add 'cachewb.EmbedMEOnView' at top of %s", t.Name(), t.Name()))
	}
	m.itemType = containerType
	m.config = cfg
	// set default values of config
	m.name = name
	m.storage = newStorage("", viewQuery, cfg, containerType)
	m.items = make(map[interface{}]interface{})
	m.itemsGroupIndex = make(map[interface{}][]string)
	m.setManager()
	return &m
}

func (c *CacheViewContainer) setManager() {
	go c.workerMaintainer()
}

func (c *CacheViewContainer) workerMaintainer() {
	for {
		t := time.After(time.Second * time.Duration(c.config.Interval))
		select {
		case <-t:
			func() {
				//defer func() {
				//	if err := recover(); err != nil {
				//		fmt.Println("Error in worker") // TODO remind for more developing
				//	}
				//}()
				c.mu.Lock()
				defer c.mu.Unlock()
				for n, item := range c.items {
					val := reflect.ValueOf(item)
					elem := val.Elem()

					eme := elem.FieldByName("EmbedMEOnView")
					fmt.Println(n)
					if eme.IsValid() {
						embedMe := eme.Interface().(EmbedMEOnView)
						fmt.Println(embedMe)
						if embedMe.ttlReached() {
					//		fmt.Println("TTL Reached")
					//		c.RemoveFromCache(n)
						}
					}
				}
			}()
		}
	}
}

func (c *CacheViewContainer) getByLock(value interface{}) (interface{}, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	r, ok := c.items[value]
	return r, ok
}

func (c *CacheViewContainer) getByLockFromGroupIndex(value interface{}) ([]string, bool) {
	c.muIndex.Lock()
	defer c.muIndex.Unlock()
	r, ok := c.itemsGroupIndex[value]
	return r, ok
}

func (c *CacheViewContainer) getValueStr(a ...interface{}) string {
	var s []string
	for _, item := range a {
		s = append(s, fmt.Sprintf("%v", item))
	}
	return strings.Join(s, "-")
}

// Return an object from cache. This method check cache first,
// and then look in the database if not found
// It return result for values, if it was one, if there were
// more than one result, you should use GetList()
func (c *CacheViewContainer) Get(values ...interface{}) (interface{}, error) {
	valStr := c.getValueStr(values...)
	if item, ok := c.getByLock(valStr); ok {
		elem := reflect.ValueOf(item).Elem()
		if elem.Kind() == reflect.Struct {
			eme := elem.FieldByName("EmbedMEOnView")
			if eme.IsValid() {
				embedMe := eme.Interface().(EmbedMEOnView)
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
				eme := elem.FieldByName("EmbedMEOnView")
				if eme.IsValid() {
					embedMe := eme.Interface().(EmbedMEOnView)
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

// like Get, but it return array of object
func (c *CacheViewContainer) GetList(values ...interface{}) ([]interface{}, error) {
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
					eme := elem.FieldByName("EmbedMEOnView")
					if eme.IsValid() {
						embedMe := eme.Interface().(EmbedMEOnView)
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

func (c *CacheViewContainer) RemoveFromCache(values ...interface{}) {
	valStr := c.getValueStr(values...)
	if g, ok := c.getByLockFromGroupIndex(valStr); ok {
		for _, m := range g {
			delete(c.items, m)
		}
		delete(c.itemsGroupIndex, valStr)

	} else {
		delete(c.items, valStr)
	}
}

type EmbedMEOnView struct {
	container  *CacheViewContainer
	parent     interface{}
	lastAccess time.Time
}

func (c *EmbedMEOnView) ttlReached() bool {
	if c.container.config.AccessTTL != 0 &&
		int(time.Since(c.lastAccess).Seconds()) > c.container.config.AccessTTL {
		return true
	} else {
		return false
	}
}
