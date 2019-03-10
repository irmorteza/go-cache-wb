package cachewb

import (
	"fmt"
	"reflect"
	"sync"
	"time"
)

type CacheContainer struct {
	storage  Storage
	config   Config
	name     string // table name
	TempTime time.Time
	itemType interface{}
	items    map[interface{}]interface{}
	wu       chan interface{}
	sync.RWMutex
}

func newContainer(tbl string, cfg Config, containerType interface{}) *CacheContainer {
	var m CacheContainer
	m.itemType = containerType
	m.config = cfg
	m.name = tbl
	m.storage = newStorage(tbl, cfg, containerType)
	m.items = make(map[interface{}]interface{})
	m.wu = make(chan interface{})
	m.setManager()
	return &m
}

func (c *CacheContainer)setManager(){
	go c.workerConsumerUpdater()
	go c.workerMaintainer()
}

func (c *CacheContainer)workerConsumerUpdater() {
	for {
		select {
		case item := <-c.wu:
			fmt.Println("Hello Worker. I want update:" , item)
			reflect.ValueOf(item).MethodByName("UpdateStorage").Call([]reflect.Value{})
		}
	}
}

func (c *CacheContainer)workerMaintainer() {
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
				c.Lock()
				defer c.Unlock()
				for n, item := range c.items {
					val := reflect.ValueOf(item)
					elem := val.Elem()
					f1 := elem.FieldByName("updates")
					f2 := elem.FieldByName("LastUpdate")

					if f1.Int() > int64(c.config.CacheWriteLatencyCount) {
						val.MethodByName("UpdateStorage").Call([]reflect.Value{})	  // TODO may this line need go
					} else if f1.Int() > 0 &&
						time.Since(f2.Interface().(time.Time)).Seconds() > float64(c.config.CacheWriteLatencyTime) {
						val.MethodByName("UpdateStorage").Call([]reflect.Value{})	  // TODO may this line need go
					}

					res := val.MethodByName("TTLReached").Call([]reflect.Value{})
					if res[0].Bool(){
						fmt.Println("TTL Reached")
						c.RemoveFromCache(n)
					}
				}
			}()
		}
	}
}

func (c *CacheContainer) Flush(value interface{}) {
	
}

func (c *CacheContainer) getByLock(value interface{}) (interface{}, bool) {
	c.Lock()
	defer c.Unlock()
	r, ok := c.items[value]
	return r, ok
}

func (c *CacheContainer)Get(value interface{})interface{} {
	if item, ok := c.getByLock(value); ok {
		return item
	} else {
		res := c.storage.Get(value)
		elem := reflect.ValueOf(res).Elem()

		if elem.Kind() == reflect.Struct {
			f := elem.FieldByName("Container")
			if f.IsValid() && f.CanSet() {
				if f.Kind() == reflect.Ptr {
					f.Set(reflect.ValueOf(c))
				}
			}
			p := elem.FieldByName("Parent")
			if p.IsValid() && p.CanSet() {
				p.Set(reflect.ValueOf(res))
			}

			ex := elem.FieldByName("LastAccess")
			if ex.IsValid() && ex.CanSet() {
				ex.Set(reflect.ValueOf(time.Now()))
			}
		}
		c.Lock()
		defer c.Unlock()
		c.items[value] = res
		return res
	}
}

func (c *CacheContainer)Insert(in interface{})interface{} {
	res := c.storage.Insert(in)
	return res
}

func (c *CacheContainer)Remove(value interface{}) interface{}{
	return c.storage.Remove(value)
}

func (c *CacheContainer)RemoveFromCache(name interface{}) {
	delete(c.items, name)
}

type EmbedME struct {
	Container  *CacheContainer
	Parent     interface{}
	updates    int
	LastUpdate time.Time
	LastAccess time.Time
	sync.RWMutex
}

func (c *EmbedME)Inc(a interface{}){
	c.Lock()
	defer c.Unlock()
	c.updates ++
	c.LastUpdate = time.Now()
	c.LastAccess = time.Now()
	if c.updates > c.Container.config.CacheWriteLatencyCount {
		c.Container.wu <- a
	}
}

func (c *EmbedME)SetAccess() {
	c.Lock()
	defer c.Unlock()
	c.LastAccess = time.Now()
}

func (c *EmbedME)UpdateStorage() {
	c.Lock()
	defer c.Unlock()
	if c.updates > 0 {
		fmt.Println("yesssssssssssssssss  Let update")
		c.Container.storage.Update(c.Parent)
		c.updates = 0
	}
}

func (c *EmbedME)TTLReached() bool {
	if c.Container.config.AccessTTL != 0 &&
		int(time.Since(c.LastAccess).Seconds()) > c.Container.config.AccessTTL &&
		c.updates == 0 {
		return true
	} else {
		return false
	}
}
