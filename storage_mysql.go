package cachewb

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"reflect"
	"strconv"
	"strings"
)

type ConfigMysql struct {
	Host                   string
	Username               string
	Password               string
	Port                   int
	DBName                 string
	MaxOpenConnection      int
}

type MySQL struct {
	mysqlDB           *sql.DB
	itemTemplate      interface{}
	fieldsMap         map[string]string
	tableName         string
	updateQuery       string
	updateQueryFields []string
	insertQuery       string
	insertQueryFields []string
	cfg               ConfigMysql
}

func newMySQL(tableName string, cfg ConfigMysql, itemTemplate interface{})  *MySQL{
	m := &MySQL{cfg: cfg, tableName:tableName}
	m.itemTemplate = itemTemplate
	m.ParseTemplate()
	return m
}

func (cls *MySQL) ParseTemplate() {
	// create map field
	setClause := ""
	whereClause := ""
	whereFieldName := ""
	val1 := ""
	val2 := ""
	cls.fieldsMap = make(map[string]string)
	t := reflect.TypeOf(cls.itemTemplate)
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if tag := f.Tag.Get("storage"); tag != "" {
			cls.fieldsMap[tag] = f.Name

			if f.Tag.Get("updateKey") == "1" {
				whereFieldName = f.Name
				whereClause = fmt.Sprintf("%s = ?", tag)
			} else if f.Tag.Get("update") != "0" {
				cls.updateQueryFields = append(cls.updateQueryFields, f.Name)
				setClause = fmt.Sprintf("%s, %s = ?", setClause, tag)
			}

		} //else {		if had not storage
		//	cls.fieldsMap[f.Name] = f.Name
		//}
	}

	// create update query
	if whereFieldName == ""{
		panic("Can't find updateKey")  		// TODO fix message
	}

	cls.updateQueryFields = append(cls.updateQueryFields, whereFieldName)
	if len(setClause) > 0 && strings.HasPrefix(setClause, ", "){
		setClause = setClause [2:]
	}
	if len(val1) > 0 && strings.HasPrefix(val1, ", "){
		val1 = val1 [2:]
		val2 = val2 [2:]
	}
	cls.updateQuery = fmt.Sprintf("UPDATE %s SET %s WHERE %s;", cls.tableName, setClause, whereClause)
	cls.insertQuery = fmt.Sprintf("INSERT INTO %s (%s) values (%s);", cls.tableName, val1, val2)

	fmt.Println(cls.updateQuery)
	fmt.Println(cls.updateQueryFields)
	fmt.Println(cls.insertQuery)
	fmt.Println(cls.insertQueryFields)

}

func (cls *MySQL) CheckConnection() {
	if cls.mysqlDB == nil {
		qs := cls.cfg.Username + ":" + cls.cfg.Password + "@tcp(" + cls.cfg.Host + ":" + strconv.Itoa(cls.cfg.Port) + ")/" + cls.cfg.DBName + "?parseTime=true"
		var err error
		cls.mysqlDB, err = sql.Open("mysql", qs)
		if err != nil {
			panic(err.Error()) // Just for example purpose. You should use proper error handling instead of panic
		}
		cls.mysqlDB.SetMaxOpenConns(cls.cfg.MaxOpenConnection)
	}
}

func (cls *MySQL) Get(key string) interface{}{
	val := reflect.New(reflect.TypeOf(cls.itemTemplate))
	elem := val.Elem()
	cls.CheckConnection()

	stmt, err := cls.mysqlDB.Prepare(fmt.Sprintf("SELECT * from %s where name = ? ;", cls.tableName))
	if err != nil {
		panic(err)
	}
	defer stmt.Close()
	rows, err := stmt.Query(key)
	if err != nil {
		panic(err)
	}

	columns, _ := rows.Columns()
	count := len(columns)
	values := make([]interface{}, count)
	valuePtrs := make([]interface{}, count)

	for rows.Next() {

		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		rows.Scan(valuePtrs...)

		for i, col := range columns {
			var v interface{}
			val := values[i]
			b, ok := val.([]byte)
			if ok {
				v = string(b)
			} else {
				v = val
			}

			if elem.Kind() == reflect.Struct {
				if c2, ok := cls.fieldsMap[col]; ok {
					f := elem.FieldByName(c2)
					if f.IsValid() && f.CanSet() {
						f.Set(reflect.ValueOf(v))
					}
				}
			}
		}
	}
	return val.Interface()
}

func (cls *MySQL) Update(in interface{}) {
	elem := reflect.ValueOf(in).Elem()

	valuePtrs := make([]interface{}, 0)

	for _, n:=range cls.updateQueryFields {
		zz := elem.FieldByName(n)
		if zz.IsValid(){
			valuePtrs = append(valuePtrs, zz.Interface())
		}
	}
	cls.CheckConnection()
	stmt, err := cls.mysqlDB.Prepare(cls.updateQuery)
	if err != nil {
		panic(err)
	}
	defer stmt.Close()
	_, err = stmt.Exec(valuePtrs...)
	if err != nil {
		panic(err)
	}
}
