package cachewb

import "time"
type Cache struct {
	containers map[string]*CacheContainer
	config     Config
	storage    *MySQL
	Manager    CacheManager
}

func (cls *Cache) GetObject(tableName string) *CacheContainer{
	if item, ok:= cls.containers[tableName]; ok {
		return item
	}else {
		m := newContainer(tableName)
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
	//stor = newMySQL(cfg)
	s.Manager.interval = time.Second * time.Duration(cfg.Interval)
	return s
}
