package cachewb

import (
	"fmt"
	"reflect"
	"sync"
	"time"
)

type CacheContainer struct {
	storage   Storage
	config    Config
	tableName string
	TempTime time.Time
	itemType  interface{}
	items     map[string]interface{}
	w         chan interface{}
	sync.RWMutex
}

func newContainer(tbl string, cfg Config, containerType interface{}) *CacheContainer {
	var m CacheContainer
	m.config = cfg
	m.storage = newStorage(cfg)
	m.tableName = tbl
	m.w = make(chan interface{})
	m.items = make(map[string]interface{})
	m.itemType = containerType
	m.setManager()
	return &m
}

func (cls *CacheContainer)setManager(){
	go cls.workerConsumer()
	go cls.workerMaintainer()
}

func (cls *CacheContainer)workerConsumer() {
	for {
		select {
		case item := <-cls.w:
			fmt.Println("Hello Worker. I want update:" , item)
			reflect.ValueOf(item).MethodByName("UpdateStorage").Call([]reflect.Value{})
		}
	}
}

func (cls *CacheContainer)workerMaintainer() {
	for {
		c := time.After(time.Second * time.Duration(cls.config.Interval))
		select {
		case <-c:
			func() {
				defer func() {
					if err := recover(); err != nil {
						fmt.Println("Error in worker") // TODO remind for more developing
					}
				}()
				cls.Lock()
				defer cls.Unlock()
				for _, item := range cls.items {
					val := reflect.ValueOf(item)
					elem := val.Elem()
					f1 := elem.FieldByName("updates")
					f2 := elem.FieldByName("LastUpdate")

					if f1.Int() > int64(cls.config.CacheWriteLatencyCount) {
						val.MethodByName("UpdateStorage").Call([]reflect.Value{})	  // TODO may this line need go
					} else if f1.Int() > 0 &&
						time.Since(f2.Interface().(time.Time)).Seconds() > float64(cls.config.CacheWriteLatencyTime) {
						val.MethodByName("UpdateStorage").Call([]reflect.Value{})	  // TODO may this line need go
					}
				}
			}()
		}
	}
}

func (cls *CacheContainer) getByLock(value string) (interface{}, bool) {
	cls.Lock()
	defer cls.Unlock()
	r, ok := cls.items[value]
	return r, ok
}

func (cls *CacheContainer)Get(value string)(interface{}) {
	if item, ok := cls.getByLock(value); ok {
		return item
	} else {
		val := reflect.New(reflect.TypeOf(cls.itemType))
		obj := val.Interface()

		elem := val.Elem()
		if elem.Kind() == reflect.Struct {
			f := elem.FieldByName("Container")
			if f.IsValid() && f.CanSet() {
				if f.Kind() == reflect.Ptr {
					f.Set(reflect.ValueOf(cls))
				}
			}
			p := elem.FieldByName("Parent")
			if p.IsValid() && p.CanSet() {
				p.Set(reflect.ValueOf(obj))
			}
		}
		cls.storage.Get(cls.tableName, value, obj)
		cls.Lock()
		defer cls.Unlock()
		cls.items[value] = obj
		return obj
	}
}

type EmbedME struct {
	Container   *CacheContainer
	Parent   interface{}
	updates int
	LastUpdate time.Time
	sync.RWMutex
}

func (cls *EmbedME)Inc(a interface{}){
	cls.Lock()
	defer cls.Unlock()
	cls.updates ++
	cls.LastUpdate = time.Now()
	if cls.updates > cls.Container.config.CacheWriteLatencyCount {
		cls.Container.w <- a
	}
}

func (cls *EmbedME)UpdateStorage() {
	cls.Lock()
	defer cls.Unlock()
	if cls.updates > 0 {
		fmt.Println("yesssssssssssssssss  Let update")
		cls.Container.storage.Update(cls.Container.tableName, "id", cls.Parent)
		cls.updates = 0
	}
}