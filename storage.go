package cachewb

type Storage interface {
	Get(tableName string, key string, o interface{})
	Update(tableName string, key string, in interface{})
}

type StorageKind uint

const (
	Invalid StorageKind = iota
	MYSQL
	MONGODB
	SQL
)

func newStorage(cfg Config) Storage {
	if cfg.StorageName == MYSQL {
		cfgMysql, ok := cfg.Database.(ConfigMysql)
		if !ok {
			panic("MySQL configs, are not correct, Please check configs")
		}
		return &MySQL{cfg: cfgMysql}
	}else{
		panic("Unknown Storage !")
	}

}