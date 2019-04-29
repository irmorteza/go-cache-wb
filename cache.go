package cachewb

import "fmt"

type CacheWB struct {
	containers map[string]*CacheContainer
	//config     Config
}

// Get the container of given table name, with type of objType,
// objType is a variable of an structure that will contain a row of table
func (c *CacheWB) GetContainer(tableName string, cfg Config, objType interface{}) *CacheContainer {
	if item, ok := c.containers[tableName]; ok {
		return item
	} else {
		m := newContainer(tableName, cfg, objType)
		c.containers[tableName] = m
		return m
	}
}

func (c *CacheWB) GetViewContainer(viewName string, viewQuery string, cfg Config, objType interface{}) *CacheContainer{
	if item, ok := c.containers[viewName]; ok {
		return item
	} else {
		m := newViewContainer(viewName, viewQuery, cfg, objType)
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
	// Interval of maintainer worker.
	// The worker check items of cache for theirs update and time to live status
	// Default is 10 seconds
	IntervalWorkerMaintainer int
	// Interval of QueryIndexMaintainer worker.
	// QueryIndexMaintainer check items of QueryIndex in cache for theirs time to live status
	// Default is 500 seconds
	IntervalWorkerQueryIndexMaintainer int
	// Maximum time, Insert worker wait for new update to make batch insert.
	// This parameter is corresponding to InsertAsync
	// Default is 1 second
	CacheInsertAsyncLatency int
	// For an item in cache: Maximum time after last update, that updates flush to storage
	// Default is 30 seconds
	CacheFlushUpdatesLatencyTime int
	// For an item in cache: Maximum updates count, that updates flush to storage
	// Default is 10 seconds
	CacheFlushUpdatesLatencyCount int
	// Type of database
	StorageName StorageKind
	// Config of database is using
	Database interface{}
	// Maximum time an item will remain in cache without any access. Then it will be removed
	// Default is 10 seconds
	AccessTTLItems int
	// Maximum time an QueryIndex will remain in cache without any access. Then it will be removed
	// Default is 10 seconds
	AccessTTLQueryIndex int
	// Enable Log
	// Default is false
	Log           bool
	// Enable gathering statistics
	// Default is false
	Statistic           bool
}

const (
	IntervalWorkerMaintainer = 10
	IntervalWorkerQueryIndexMaintainer = 500
	CacheInsertAsyncLatency = 1
	CacheFlushUpdatesLatencyTime = 30
	CacheFlushUpdatesLatencyCount = 10
	AccessTTLItems = 10
	AccessTTLQueryIndex = 10
)

func (c *Config)checkDefaults() {
	if c.IntervalWorkerMaintainer == 0 {
		c.IntervalWorkerMaintainer = IntervalWorkerMaintainer
	}
	if c.IntervalWorkerQueryIndexMaintainer == 0 {
		c.IntervalWorkerQueryIndexMaintainer = IntervalWorkerQueryIndexMaintainer
	}
	if c.CacheInsertAsyncLatency == 0 {
		c.CacheInsertAsyncLatency = CacheInsertAsyncLatency
	}
	if c.CacheFlushUpdatesLatencyTime == 0 {
		c.CacheFlushUpdatesLatencyTime = CacheFlushUpdatesLatencyTime
	}
	if c.CacheFlushUpdatesLatencyCount == 0 {
		c.CacheFlushUpdatesLatencyCount = CacheFlushUpdatesLatencyCount
	}
	if c.AccessTTLItems == 0 {
		c.AccessTTLItems = AccessTTLItems
	}
	if c.AccessTTLQueryIndex == 0 {
		c.AccessTTLQueryIndex = AccessTTLQueryIndex
	}
}

// Get an CacheWB variable
func NewCacheWB() *CacheWB {
	s := &CacheWB{}
	s.containers = make(map[string]*CacheContainer)
	return s
}
