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
				c.mu.Lock()
				defer c.mu.Unlock()
				for n, item := range c.items {
					val := reflect.ValueOf(item)
					elem := val.Elem()

					eme := elem.FieldByName("EmbedME")
					if eme.IsValid() {
						embedMe := eme.Interface().(EmbedME)
						//fmt.Println("Hello morteeza Lass Access", embedMe.lastAccess)
						if embedMe.updates > c.config.CacheWriteLatencyCount {
							c.workerChan <- item
						} else if embedMe.updates > 0 &&
							time.Since(embedMe.lastUpdate).Seconds() > float64(c.config.CacheWriteLatencyTime) {
							c.workerChan <- item
						}
						if embedMe.TTLReached() {
							fmt.Println("TTL Reached")
							c.RemoveFromCache(n)
						}
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
func (c *CacheContainer)getValueStr(a ...interface{})  string{
	var s []string
	for _, item := range a{
		s = append(s, fmt.Sprintf("%v", item))
	}
	return strings.Join(s, "-")
}

func (c *CacheContainer)Get(values ...interface{})interface{} {
	valStr := c.getValueStr(values...)
	if item, ok := c.getByLock(valStr); ok {
		return item
	} else {
		res := c.storage.Get(values...)
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
	container  *CacheContainer
	parent     interface{}
	updates    int
	lastUpdate time.Time
	lastAccess time.Time
	mu         sync.RWMutex
}

func (c *EmbedME)Inc(a interface{}) error{
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.container.lockUpdate{
		fmt.Println(fmt.Sprintf("Updates are locked in container of '%s', Please try later", c.container.name))
		return errors.New(fmt.Sprintf("Updates are locked in container of '%s', Please try later", c.container.name))
	}

	c.updates ++
	fmt.Println("irmorteza", time.Now())
	c.lastUpdate = time.Now()
	c.lastAccess = time.Now()
	if c.updates >= c.container.config.CacheWriteLatencyCount {
		c.container.workerChan <- a
	}
	return nil
}

func (c *EmbedME)SetAccess() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastAccess = time.Now()
}

func (c *EmbedME)UpdateStorage() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.updates > 0 {
		fmt.Println("Let update, updates= ", c.updates)
		c.container.storage.Update(c.parent)
		c.container.UpdatedCount += uint(c.updates)
		c.updates = 0
	}
}

func (c *EmbedME)TTLReached() bool {
	if c.container.config.AccessTTL != 0 &&
		int(time.Since(c.lastAccess).Seconds()) > c.container.config.AccessTTL &&
		c.updates == 0 {
		return true
	} else {
		return false
	}
}
