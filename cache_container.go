package cachewb

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"
)

type CacheContainer struct {
	storage      Storage
	config       Config
	name         string // table name
	lockUpdate   bool
	UpdatedCount uint // TODO temp
	itemType     interface{}
	items        map[interface{}]interface{}
	workerChan   chan interface{}
	mu           sync.RWMutex
}

func newContainer(tbl string, cfg Config, containerType interface{}) *CacheContainer {
	var m CacheContainer
	t := reflect.TypeOf(containerType)
	if t.NumField() == 0 || t.Field(0).Name != "EmbedME"{
		panic(fmt.Sprintf("Coundn't find 'EmbedME' in %s. Please Add 'cachewb.EmbedME' at top of %s" , t.Name(), t.Name()))
	}
	m.itemType = containerType
	m.config = cfg
	m.name = tbl
	m.storage = newStorage(tbl, cfg, containerType)
	m.items = make(map[interface{}]interface{})
	m.workerChan = make(chan interface{})
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
		case item := <-c.workerChan:
			//fmt.Println("Hello Worker. I want update:" , item)
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
				c.mu.Lock()
				defer c.mu.Unlock()
				for n, item := range c.items {
					val := reflect.ValueOf(item)
					elem := val.Elem()
					f1 := elem.FieldByName("updates")
					f2 := elem.FieldByName("LastUpdate")

					if f1.Int() > int64(c.config.CacheWriteLatencyCount) {
						c.workerChan <- item
					} else if f1.Int() > 0 &&
						time.Since(f2.Interface().(time.Time)).Seconds() > float64(c.config.CacheWriteLatencyTime) {
						c.workerChan <- item
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
		c.mu.Lock()
		defer c.mu.Unlock()
		c.items[value] = res
		return res
	}
}

func (c *CacheContainer)Insert(in interface{})interface{} {
	if c.lockUpdate{
		fmt.Println(fmt.Sprintf("Updates are locked in container of '%s', Please try later", c.name))
		return nil
	}
	res := c.storage.Insert(in)
	return res
}

func (c *CacheContainer)Remove(value interface{}) interface{}{
	if c.lockUpdate{
		fmt.Println(fmt.Sprintf("Updates are locked in container of '%s', Please try later", c.name))
		return nil
	}
	c.RemoveFromCache(value)
	return c.storage.Remove(value)
}

func (c *CacheContainer)RemoveFromCache(name interface{}) {
	if c.lockUpdate{
		fmt.Println(fmt.Sprintf("Updates are locked in container of '%s', Please try later", c.name))
	}
	delete(c.items, name)
}

type EmbedME struct {
	Container  *CacheContainer
	Parent     interface{}
	updates    int
	LastUpdate time.Time
	LastAccess time.Time
	mu         sync.RWMutex
}

func (c *EmbedME)Inc(a interface{}) error{
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.Container.lockUpdate{
		fmt.Println(fmt.Sprintf("Updates are locked in container of '%s', Please try later", c.Container.name))
		return errors.New(fmt.Sprintf("Updates are locked in container of '%s', Please try later", c.Container.name))
	}

	c.updates ++
	c.LastUpdate = time.Now()
	c.LastAccess = time.Now()
	if c.updates >= c.Container.config.CacheWriteLatencyCount {
		c.Container.workerChan <- a
	}
	return nil
}

func (c *EmbedME)SetAccess() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.LastAccess = time.Now()
}

func (c *EmbedME)UpdateStorage() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.updates > 0 {
		//fmt.Println("Let update, updates= ", c.updates)
		c.Container.storage.Update(c.Parent)
		c.Container.UpdatedCount += uint(c.updates)
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
