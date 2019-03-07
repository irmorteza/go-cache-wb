package cachewb

type Cache struct {
	containers map[string]*CacheContainer
	config     Config
}

func (cls *Cache) GetObject(tableName string, objType interface{}) *CacheContainer{
	if item, ok:= cls.containers[tableName]; ok {
		return item
	}else {
		m := newContainer(tableName, cls.config, objType)
		cls.containers[tableName] = m
		return m
	}
}

type Config struct {
	Interval               int
	CacheWriteLatencyTime  int
	CacheWriteLatencyCount int
	StorageName            StorageKind
	Database               interface{}
}

func NewCacheWB(cfg Config) *Cache  {
	s := &Cache{}
	s.config = cfg
	s.containers = make(map[string]*CacheContainer)
	return s
}
