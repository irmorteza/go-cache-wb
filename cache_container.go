package cachewb

import (
	"fmt"
	"reflect"
	"sync"
)

type CacheContainer struct {
	storage   *MySQL
	tableName string
	items     map[string]interface{}
	w         chan interface{}
	sync.RWMutex
}

func newContainer(tbl string) *CacheContainer {
	var m CacheContainer
	m.tableName = tbl
	m.w = make(chan interface{})
	m.setManager()
	return &m
}

func (cls *CacheContainer)setManager(){
	go cls.Worker()
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

func (cls *CacheContainer)Get(value string, o interface{}){
	elem := reflect.ValueOf(o).Elem()
	if elem.Kind() == reflect.Struct {
		f := elem.FieldByName("Container")
		if f.IsValid() && f.CanSet() {
			if f.Kind() == reflect.Ptr {
				f.Set(reflect.ValueOf(cls))
			}
		}
		p := elem.FieldByName("Parent")
		if p.IsValid() && p.CanSet() {
			p.Set(reflect.ValueOf(o))
		}
	}
	cls.storage.Get(cls.tableName, value, o)
}

type EmbedME struct {
	Container   *CacheContainer
	Parent   interface{}
	updates int
}

func (cls *EmbedME)Inc(a interface{}){
	cls.updates ++
	if cls.updates > 2 {
		cls.Container.w <- a 		//TODO enable me
	}
}

func (cls *EmbedME)UpdateStorage() {
	//fmt.Println("Let update object")
	elem := reflect.ValueOf(cls.Parent).Elem()
	fmt.Println("UpdateStorage", elem.FieldByName("Name"))
	cls.Container.storage.Update(cls.Container.tableName, "id", cls.Parent)
}