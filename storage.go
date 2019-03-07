package cachewb

type Storage interface {
	Get(key string) interface{}
	Update(in interface{})
	Insert(in interface{}) interface{}
}

type StorageKind uint

const (
	Invalid StorageKind = iota
	MYSQL
	MONGODB
	SQL
)

func newStorage(tableName string, cfg Config, itemTemplate interface{}) Storage {
	if cfg.StorageName == MYSQL {
		cfgMysql, ok := cfg.Database.(ConfigMysql)
		if !ok {
			panic("MySQL configs, are not correct, Please check configs")
		}

		return newMySQL(tableName, cfgMysql, itemTemplate)
	}else{
		panic("Unknown Storage !")
	}

}