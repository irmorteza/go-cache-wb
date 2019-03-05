package cachewb

import (
	"fmt"
	"reflect"
	"sync"
	"time"
)

type CacheContainer struct {
	storage   *MySQL
	config    Config
	tableName string
	itemType  interface{}
	items     map[string]interface{}
	w         chan interface{}
	sync.RWMutex
}

func newContainer(tbl string, containerType interface{}) *CacheContainer {
	var m CacheContainer
	m.tableName = tbl
	m.w = make(chan interface{})
	m.items = make(map[string]interface{})
	m.itemType = containerType
	m.setManager()
	return &m
}

func (cls *CacheContainer)setManager(){
	go cls.Worker()
	//go cls.Worker2()
}

func (cls *CacheContainer)Worker() {
	for {
		select {
		case item := <-cls.w:
			fmt.Println("Hello Worker. I want update:" , item)
			reflect.ValueOf(item).MethodByName("UpdateStorage").Call([]reflect.Value{})
		}
	}
}
func (cls *CacheContainer)Worker2() {
	for {
		c := time.After(time.Second*1)
		select {
		case <-c:
			func() {
				defer func() {
					if err := recover(); err != nil {
						fmt.Println("Error in worker")	// TODO remind for more developing
					}
				}()
				//for _, item:= range cls.items{
				//	fmt.Println("worker", item)
				//}
				//fmt.Println("Hello Worker 22. I want update:", cls.items)
			}()

		}
	}
}


func (cls *CacheContainer)Get(value string)(interface{}) {
	if item, ok := cls.items[value]; ok {
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
		cls.items[value] = obj
		return obj
	}
}

type EmbedME struct {
	Container   *CacheContainer
	Parent   interface{}
	updates int
}

func (cls *EmbedME)Inc(a interface{}){
	cls.updates ++
	if cls.updates > cls.Container.config.CacheWriteLatencyCount {
		cls.Container.w <- a
	}
}

func (cls *EmbedME)UpdateStorage() {
	//fmt.Println("Let update object")
	//elem := reflect.ValueOf(cls.Parent).Elem()
	//fmt.Println("UpdateStorage", elem.FieldByName("Name"))

	cls.Container.storage.Update(cls.Container.tableName, "id", cls.Parent)
}