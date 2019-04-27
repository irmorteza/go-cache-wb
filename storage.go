package cachewb

type storage interface {
	get(keys []string, values[]interface{}) ([]interface{}, error)
	getBySquirrel(args ...interface{}) ([]interface{}, error)
	update(in interface{}) (map[string]int64, error)
	insert(in ...interface{}) (map[string]int64, error)
	remove(v ...interface{}) (map[string]int64, error)
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
