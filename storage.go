package cachewb

type Storage interface {
	get(key ...interface{}) interface{}
	getList(key ...interface{}) []interface{}
	update(in interface{})
	insert(in interface{}) interface{}
	remove(v interface{}) interface{}
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
			panic("mySQL configs, are not correct, Please check configs")
		}

		return newMySQL(tableName, cfgMysql, itemTemplate)
	}else{
		panic("Unknown Storage !")
	}

}