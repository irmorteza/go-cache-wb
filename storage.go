package cachewb

type storage interface {
	get(key ...interface{}) (interface{}, error)
	getList(key ...interface{}) ([]interface{}, error)
	update(in interface{})
	insert(in interface{}) interface{}
	remove(v ...interface{}) (interface{}, error)
}

type StorageKind uint

const (
	Invalid StorageKind = iota
	MYSQL
	MONGODB
	SQL
)

func newStorage(tableName string, cfg Config, itemTemplate interface{}) storage {
	if cfg.StorageName == MYSQL {
		cfgMysql, ok := cfg.Database.(ConfigMysql)
		if !ok {
			panic("mySQL configs, are not correct, Please check configs")
		}

		return newMySQL(tableName, cfgMysql, itemTemplate)
	}else{
		panic("Unknown Storage !")
	}

}