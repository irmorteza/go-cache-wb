package cachewb

import "fmt"

type CacheWB struct {
	containers map[string]*CacheContainer
	config     Config
}


// Get the container of given table name, with type of objType,
// objType is a variable of an structure that will contain a row of table
func (c *CacheWB) GetContainer(tableName string, objType interface{}) *CacheContainer {
	if item, ok := c.containers[tableName]; ok {
		return item
	} else {
		m := newContainer(tableName, c.config, objType)
		c.containers[tableName] = m
		return m
	}
}

func (c *CacheWB) GetViewContainer(viewName string, viewQuery string, objType interface{}) *CacheContainer{
	if item, ok := c.containers[viewName]; ok {
		return item
	} else {
		m := newViewContainer(viewName, viewQuery, c.config, objType)
		c.containers[viewName] = m
		return m
	}
}

// Flush all data remained in cache (for all containers), but not flushed to database yet.
// withLock cause to avoid any updates on container (while the container is flushing)
func (c *CacheWB) FlushAll(withLock bool) {
	for _, item := range c.containers {
		if !item.isView {
			item.Flush(withLock)
		}
	}
}

// GracefulShutdown avoid all new update in all container and flush all containers too.
// After calling it, no update accepts.
func (c *CacheWB) GracefulShutdown() bool {
	fmt.Println("Start Graceful Shutdown")
	for _, item := range c.containers {
		if !item.isView {
			fmt.Println("Graceful Shutdown, Flushing ", item.name)
			item.lockUpdate = true
			item.Flush(false)
		}
	}
	fmt.Println("Graceful Shutdown Completed")
	return true
}

type Config struct {
	//
	Interval               int
	CacheWriteLatencyTime  int
	// For a object in container, it represent maximum cache update
	// in time of CacheWriteLatencyTime need to make flush object
	CacheWriteLatencyCount int
	// Type of database, that cache should support
	StorageName            StorageKind
	// Config of database is using
	Database               interface{}
	// Maximum time, an object keep in cache after last access
	AccessTTL              int
	AsyncInsertLatency     int
}

// Get an CacheWB variable
func NewCacheWB(cfg Config) *CacheWB {
	s := &CacheWB{}
	s.config = cfg
	s.containers = make(map[string]*CacheContainer)
	return s
}
