package cachewb

type Cache struct {
	containers map[string]*CacheContainer
	config     Config
}

func (c *Cache) GetObject(tableName string, objType interface{}) *CacheContainer{
	if item, ok:= c.containers[tableName]; ok {
		return item
	}else {
		m := newContainer(tableName, c.config, objType)
		c.containers[tableName] = m
		return m
	}
}

type Config struct {
	Interval               int
	CacheWriteLatencyTime  int
	CacheWriteLatencyCount int
	StorageName            StorageKind
	Database               interface{}
	AccessTTL				int
}

func NewCacheWB(cfg Config) *Cache  {
	s := &Cache{}
	s.config = cfg
	s.containers = make(map[string]*CacheContainer)
	return s
}
