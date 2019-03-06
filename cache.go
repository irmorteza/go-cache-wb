package cachewb

type Cache struct {
	containers map[string]*CacheContainer
	config     Config
	storage    *MySQL
}

func (cls *Cache) GetObject(tableName string, objType interface{}) *CacheContainer{
	if item, ok:= cls.containers[tableName]; ok {
		return item
	}else {
		m := newContainer(tableName, objType)
		m.storage = cls.storage
		m.config = cls.config
		cls.containers[tableName] = m
		return m
	}
}

type Config struct {
	Interval               int
	Host                   string
	Username               string
	Password               string
	Port                   int
	DBName                 string
	MaxOpenConnection      int
	CacheWriteLatencyTime  int
	CacheWriteLatencyCount int
}

func NewCacheWB(cfg Config) *Cache  {
	s := &Cache{}
	s.config = cfg
	s.containers = make(map[string]*CacheContainer)
	s.storage = newMySQL(cfg)
	return s
}
