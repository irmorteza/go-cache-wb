package cachewb

type storage interface {
	//get(key ...interface{}) (interface{}, error)
	//getList(key ...interface{}) ([]interface{}, error)
	get(keys []string, values[]interface{}) ([]interface{}, error)
	update(in interface{}) (interface{}, error)
	insert(in ...interface{}) (interface{}, error)
	remove(v ...interface{}) (interface{}, error)
	getInsertLimit() int
}

type StorageKind uint

const (
	Invalid StorageKind = iota
	MYSQL
	MONGODB
	SQL
)

func newStorage(tableName string, viewQuery string, cfg Config, itemTemplate interface{}) storage {
	if cfg.StorageName == MYSQL {
		cfgMysql, ok := cfg.Database.(ConfigMysql)
		if !ok {
			panic("mySQL config is not correct, please check configs")
		}

		return newMySQL(tableName, viewQuery, cfgMysql, itemTemplate)
	} else {
		panic("Unknown Storage !")
	}
}
