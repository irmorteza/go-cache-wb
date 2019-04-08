package cachewb

import "fmt"

type CacheWB struct {
	containers map[string]*CacheContainer
	config     Config
}

func (c *CacheWB) GetContainer(tableName string, objType interface{}) *CacheContainer {
	if item, ok := c.containers[tableName]; ok {
		return item
	} else {
		m := newContainer(tableName, c.config, objType)
		c.containers[tableName] = m
		return m
	}
}

func (c *CacheWB) FlushAll(l bool) {
	for _, item := range c.containers {
		item.Flush(l)
	}
}

func (c *CacheWB) GracefulShutdown() bool {
	fmt.Println("Start Graceful Shutdown")
	for _, item := range c.containers {
		fmt.Println("Graceful Shutdown, Flushing ", item.name)
		item.lockUpdate = true
		item.Flush(false)
	}
	fmt.Println("Graceful Shutdown Completed")
	return true
}

type Config struct {
	Interval               int
	CacheWriteLatencyTime  int
	CacheWriteLatencyCount int
	StorageName            StorageKind
	Database               interface{}
	AccessTTL              int
	RestPort               int
}

func NewCacheWB(cfg Config) *CacheWB {
	s := &CacheWB{}
	s.config = cfg
	s.containers = make(map[string]*CacheContainer)
	return s
}
