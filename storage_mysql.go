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
	mysqlDB  *sql.DB
	cfg      ConfigMysql

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

func (cls *MySQL) Get(tableName string, key string, o interface{}) {
	m := make(map[string]string)
	t := reflect.TypeOf(o).Elem()
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if tag := f.Tag.Get("mysql"); tag != "" {
			m[tag] = f.Name
		} else {
			m[f.Name] = f.Name
		}
	}
	elem := reflect.ValueOf(o).Elem()

	cls.CheckConnection()

	stmt, err := cls.mysqlDB.Prepare(fmt.Sprintf("SELECT * from %s where name = ? ;", tableName))

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
				if c2, ok := m[col]; ok {
					//fmt.Println("***", c2)
					f := elem.FieldByName(c2)
					if f.IsValid() && f.CanSet() {
						f.Set(reflect.ValueOf(v))
					}
				}
			}
		}
	}
}

func (cls *MySQL) Update(tableName string, key string, in interface{}) {
	t := reflect.TypeOf(in).Elem()
	elem := reflect.ValueOf(in).Elem()

	setStr := ""
	condField := ""
	var condVal interface{}
	valuePtrs := make([]interface{}, 0)

	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.Tag.Get("cwb") == "1" && f.Tag.Get("cwbKey") == "1"{
			panic("can't have both cwb and cwbKey")  		// TODO fix message
		}
		if f.Tag.Get("cwb") == "1" {
			fName := f.Name
			if tag := f.Tag.Get("mysql"); tag != "" {
				fName = tag
			}
			zz := elem.FieldByName(f.Name)
			if zz.IsValid(){
				valuePtrs = append(valuePtrs, zz.Interface())
			}
			setStr = fmt.Sprintf("%s, %s = ?", setStr, fName)
		}else if f.Tag.Get("cwbKey") == "1" {
			fName := f.Name
			if tag := f.Tag.Get("mysql"); tag != "" {
				fName = tag
			}
			condField = fName
			zz := elem.FieldByName(f.Name)
			if zz.IsValid(){
				condVal = zz.Interface()
			}
		}
	}

	if len(setStr) > 0 && strings.HasPrefix(setStr, ", "){
		setStr = setStr [2:]
	}

	valuePtrs = append(valuePtrs, condVal)
	////fmt.Println("**********", setStr)
	//fmt.Println("**********", valuePtrs)
	////fmt.Println("**********", condField, condVal)

	if condField == ""{
		panic("can't find cwbKey")  		// TODO fix message
	}

	cls.CheckConnection()
	q := fmt.Sprintf("UPDATE %s SET %s WHERE %s = ?", tableName, setStr, condField)
	stmt, err := cls.mysqlDB.Prepare(q)
	if err != nil {
		panic(err)
	}
	defer stmt.Close()
	_, err = stmt.Exec(valuePtrs...)
	if err != nil {
		panic(err)
	}
}
