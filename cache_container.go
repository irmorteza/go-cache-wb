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

func (cls *CacheContainer)setManager(){
	go cls.workerConsumerUpdater()
	go cls.workerMaintainer()
}

func (cls *CacheContainer)workerConsumerUpdater() {
	for {
		select {
		case item := <-cls.wu:
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
				for n, item := range cls.items {
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

					res := val.MethodByName("TTLReached").Call([]reflect.Value{})
					if res[0].Bool(){
						fmt.Println("TTL Reached")
						cls.RemoveFromCache(n)
					}
				}
			}()
		}
	}
}

func (cls *CacheContainer) getByLock(value interface{}) (interface{}, bool) {
	cls.Lock()
	defer cls.Unlock()
	r, ok := cls.items[value]
	return r, ok
}

func (cls *CacheContainer)Get(value interface{})interface{} {
	if item, ok := cls.getByLock(value); ok {
		return item
	} else {
		res := cls.storage.Get(value)
		elem := reflect.ValueOf(res).Elem()

		if elem.Kind() == reflect.Struct {
			f := elem.FieldByName("Container")
			if f.IsValid() && f.CanSet() {
				if f.Kind() == reflect.Ptr {
					f.Set(reflect.ValueOf(cls))
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
		cls.Lock()
		defer cls.Unlock()
		cls.items[value] = res
		return res
	}
}

func (cls *CacheContainer)Insert(in interface{})interface{} {
	res := cls.storage.Insert(in)
	return res
}

func (cls *CacheContainer)Remove(key string, value interface{}) interface{}{
	return cls.storage.Remove(key, value)
}

func (cls *CacheContainer)RemoveFromCache(name interface{}) {
	delete(cls.items, name)
}

type EmbedME struct {
	Container  *CacheContainer
	Parent     interface{}
	updates    int
	LastUpdate time.Time
	LastAccess time.Time
	sync.RWMutex
}

func (cls *EmbedME)Inc(a interface{}){
	cls.Lock()
	defer cls.Unlock()
	cls.updates ++
	cls.LastUpdate = time.Now()
	cls.LastAccess = time.Now()
	if cls.updates > cls.Container.config.CacheWriteLatencyCount {
		cls.Container.wu <- a
	}
}

func (cls *EmbedME)SetAccess() {
	cls.Lock()
	defer cls.Unlock()
	cls.LastAccess = time.Now()
}

func (cls *EmbedME)UpdateStorage() {
	cls.Lock()
	defer cls.Unlock()
	if cls.updates > 0 {
		fmt.Println("yesssssssssssssssss  Let update")
		cls.Container.storage.Update(cls.Parent)
		cls.updates = 0
	}
}

func (cls *EmbedME)TTLReached() bool {
	if cls.Container.config.AccessTTL != 0 &&
		int(time.Since(cls.LastAccess).Seconds()) > cls.Container.config.AccessTTL &&
		cls.updates == 0 {
		return true
	} else {
		return false
	}
}

//func (cls *EmbedME)Expired() bool {
//	fmt.Println("LastAccess ", cls.LastAccess, time.Since(cls.LastAccess))
//	if time.Since(cls.LastAccess) > 10  && cls.updates == 0{
//		return true
//	}else{
//		return false
//	}
//}