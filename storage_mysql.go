package cachewb

import (
	"database/sql"
	"errors"
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

type mySQL struct {
	mysqlDB              *sql.DB
	itemTemplate         interface{}
	fieldsMap            map[string]string
	tableName            string
	selectQuery          string
	updateQuery          string
	updateQueryFields    []string
	//insertQuery          string
	insertManyQueryPart1 string
	insertManyQueryPart2 string
	insertQueryFields    []string
	deleteQuery          string
	whereFieldName       []string
	cfg                  ConfigMysql
	insertManyLimit      int
}

func newMySQL(tableName string, cfg ConfigMysql, itemTemplate interface{})  *mySQL {
	m := &mySQL{cfg: cfg, tableName: tableName}
	m.itemTemplate = itemTemplate
	m.parseTemplate()
	m.insertManyLimit = 1000
	return m
}
func (c *mySQL) getInsertLimit() int {
	return c.insertManyLimit
}

func (c *mySQL) parseTemplate() {
	setClause := ""
	selectClause := ""
	whereClause := ""
	//whereFieldName := ""
	val1 := ""
	val2 := ""
	c.fieldsMap = make(map[string]string)
	t := reflect.TypeOf(c.itemTemplate)
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if tag := f.Tag.Get("storage"); tag != "" {
			c.fieldsMap[tag] = f.Name
			//fmt.Println("%%%%%%%%%%%%%%%%%%%", f.Type)
			if len(selectClause) > 0 {
				selectClause = fmt.Sprintf("%s, %s", selectClause, tag)
			}else{
				selectClause = fmt.Sprintf("%s", tag)
			}
			if f.Tag.Get("key") == "1" {
				//whereFieldName = f.Name
				c.whereFieldName = append(c.whereFieldName, f.Name)
				if len(whereClause) > 0 {
					whereClause = fmt.Sprintf("%s and %s = ?", whereClause, tag)
				}else {
					whereClause = fmt.Sprintf("%s = ?", tag)
				}
			} else if f.Tag.Get("update") != "0" && f.Tag.Get("autoInc") != "1" {
				c.updateQueryFields = append(c.updateQueryFields, f.Name)
				if len(setClause) > 0 {
					setClause = fmt.Sprintf("%s, %s = ?", setClause, tag)
				}else{
					setClause = fmt.Sprintf("%s = ?", tag)
				}
			}
			if f.Tag.Get("insert") != "0" {
				if f.Tag.Get("autoInc") != "1" {
					c.insertQueryFields = append(c.insertQueryFields, f.Name)
					if len(val1) > 0 {
						val1 = fmt.Sprintf("%s, %s", val1, tag)
						val2 = fmt.Sprintf("%s, ?", val2)
					}else {
						val1 = fmt.Sprintf("%s", tag)
						val2 = fmt.Sprintf("?")
					}
				}
			}
		}
	}

	if len(c.whereFieldName) == 0{
		panic("Can't find Key")  		// TODO fix message
	}

	c.updateQueryFields = append(c.updateQueryFields, c.whereFieldName...)

	c.selectQuery = fmt.Sprintf("SELECT %s FROM %s WHERE %s;", selectClause, c.tableName, whereClause)
	c.deleteQuery = fmt.Sprintf("DELETE FROM %s WHERE %s;", c.tableName, whereClause)
	c.updateQuery = fmt.Sprintf("UPDATE %s SET %s WHERE %s;", c.tableName, setClause, whereClause)
	//c.insertQuery = fmt.Sprintf("INSERT INTO %s (%s) values (%s);", c.tableName, val1, val2)
	c.insertManyQueryPart1 = fmt.Sprintf("INSERT INTO %s (%s) values ", c.tableName, val1)
	c.insertManyQueryPart2 = fmt.Sprintf("(%s)", val2)

	//fmt.Println(c.selectQuery)
	//fmt.Println(c.deleteQuery)
	//fmt.Println(c.updateQuery)
	//fmt.Println(c.updateQueryFields)
	//fmt.Println(c.insertQuery)
	//fmt.Println(c.insertQueryFields)
}

func (c *mySQL) checkConnection() {
	if c.mysqlDB == nil {
		qs := c.cfg.Username + ":" + c.cfg.Password + "@tcp(" + c.cfg.Host + ":" + strconv.Itoa(c.cfg.Port) + ")/" + c.cfg.DBName + "?parseTime=true"
		var err error
		c.mysqlDB, err = sql.Open("mysql", qs)
		if err != nil {
			panic(err.Error()) // Just for example purpose. You should use proper error handling instead of panic
		}
		c.mysqlDB.SetMaxOpenConns(c.cfg.MaxOpenConnection)
	}
}

func (c *mySQL) get(args ...interface{}) (interface{}, error) {
	if len(c.whereFieldName) != len(args){
		return nil, errors.New(fmt.Sprintf("expected %d arguments, got %d", len(c.whereFieldName), len(args)))
	}

	val := reflect.New(reflect.TypeOf(c.itemTemplate))
	elem := val.Elem()
	c.checkConnection()

	stmt, err := c.mysqlDB.Prepare(c.selectQuery)
	if err != nil {
		panic(err)
	}
	defer stmt.Close()
	rows, err := stmt.Query(args...)
	if err != nil {
		panic(err)
	}
	columns, _ := rows.Columns()
	count := len(columns)
	values := make([]interface{}, count)
	valuePtrs := make([]interface{}, count)

	cnt := 1
	for rows.Next() {
		if cnt > 1 {
			return nil, errors.New("there are more than one row. please use GetList")
		}
		cnt ++
		for i := range columns {
			valuePtrs[i] = &values[i]
		}
		rows.Scan(valuePtrs...)
		//fmt.Println(values)
		for i, col := range columns {
			val := values[i]
			resByte , okByte := val.([]byte)
			if elem.Kind() == reflect.Struct {
				if c2, ok := c.fieldsMap[col]; ok {
					f := elem.FieldByName(c2)
					if f.IsValid() && f.CanSet() {
						//fmt.Println("##### Consider me ", c2, f.Kind(), reflect.TypeOf(val), val)
						if f.Kind() == reflect.Float64 {
							if okByte {
								//(float64) supported mysql data types : decimal
								r, _ := strconv.ParseFloat(string(resByte), 64)
								f.SetFloat(r)
							}else {
								//(float64) supported mysql data types : double, real
								f.Set(reflect.ValueOf(val))
							}
						} else if f.Kind() == reflect.Slice {
							// ([]byte) supported mysql data types : binary, tinyblob
							f.Set(reflect.ValueOf(val))
						} else if f.Kind() == reflect.String {
							// (string) supported mysql data types :varchar, varbinary, tinytext
							if okByte{
								f.Set(reflect.ValueOf(string(resByte)))
							}
						} else {
							f.Set(reflect.ValueOf(val))
						}
					}
				}
			}
		}
	}
	return val.Interface(), nil
}

func (c *mySQL) getList(args ...interface{}) ([]interface{}, error) {
	if len(c.whereFieldName) != len(args){
		return nil, errors.New(fmt.Sprintf("expected %d arguments, got %d", len(c.whereFieldName), len(args)))
	}

	var resArr []interface{}
	c.checkConnection()

	stmt, err := c.mysqlDB.Prepare(c.selectQuery)
	if err != nil {
		panic(err)
	}
	defer stmt.Close()
	rows, err := stmt.Query(args...)
	if err != nil {
		panic(err)
	}

	columns, _ := rows.Columns()
	count := len(columns)
	values := make([]interface{}, count)
	valuePtrs := make([]interface{}, count)

	for rows.Next() {
		val := reflect.New(reflect.TypeOf(c.itemTemplate))
		elem := val.Elem()
		for i := range columns {
			valuePtrs[i] = &values[i]
		}
		rows.Scan(valuePtrs...)
		//fmt.Println(values)
		for i, col := range columns {
			val := values[i]
			resByte , okByte := val.([]byte)
			if elem.Kind() == reflect.Struct {
				if c2, ok := c.fieldsMap[col]; ok {
					f := elem.FieldByName(c2)
					if f.IsValid() && f.CanSet() {
						//fmt.Println("##### Consider me ", c2, f.Kind(), reflect.TypeOf(val), val)
						if f.Kind() == reflect.Float64 {
							if okByte {
								//(float64) supported mysql data types : decimal
								r, _ := strconv.ParseFloat(string(resByte), 64)
								f.SetFloat(r)
							}else {
								//(float64) supported mysql data types : double, real
								f.Set(reflect.ValueOf(val))
							}
						} else if f.Kind() == reflect.Slice {
							// ([]byte) supported mysql data types : binary, tinyblob
							f.Set(reflect.ValueOf(val))
						} else if f.Kind() == reflect.String {
							// (string) supported mysql data types :varchar, varbinary, tinytext
							if okByte{
								f.Set(reflect.ValueOf(string(resByte)))
							}
						} else {
							f.Set(reflect.ValueOf(val))
						}
					}
				}
			}
		}
		resArr = append(resArr, val.Interface())
	}
	return resArr, nil
}

func (c *mySQL) update(in interface{}) {
	elem := reflect.ValueOf(in).Elem()

	valuePtrs := make([]interface{}, 0)

	for _, n:=range c.updateQueryFields {
		zz := elem.FieldByName(n)
		if zz.IsValid(){
			valuePtrs = append(valuePtrs, zz.Interface())
		}
	}
	c.checkConnection()
	stmt, err := c.mysqlDB.Prepare(c.updateQuery)
	if err != nil {
		panic(err)
	}
	defer stmt.Close()
	_, err = stmt.Exec(valuePtrs...)
	if err != nil {
		panic(err)
	}
}

func (c *mySQL) insert(args ...interface{}) (interface{}, error) {
	if len(args) > c.insertManyLimit{
		return nil, errors.New(fmt.Sprintf("unable to insert more than limit %d, got %d", c.insertManyLimit, len(args)))
	}
	valuePtrs := make([]interface{}, 0)
	var valueStrs []string

	for _, d := range args {
		valueStrs = append(valueStrs, c.insertManyQueryPart2)
		elem := reflect.ValueOf(d)
		for _, n := range c.insertQueryFields {
			zz := elem.FieldByName(n)
			if zz.IsValid() {
				valuePtrs = append(valuePtrs, zz.Interface())
			}
		}
	}

	c.checkConnection()
	q := fmt.Sprintf("%s %s", c.insertManyQueryPart1, strings.Join(valueStrs, ","))
	stmt, err := c.mysqlDB.Prepare(q)
	if err != nil {
		panic(err)
	}
	defer stmt.Close()
	res, err := stmt.Exec(valuePtrs...)
	if err != nil {
		panic(err)
	}
	m := make(map[string]interface{})
	m["LastInsertId"], _ = res.LastInsertId()
	m["RowsAffected"], _ = res.RowsAffected()
	return m, nil
}

func (c *mySQL) remove(args ...interface{}) (interface{}, error){
	if len(c.whereFieldName) != len(args){
		return nil, errors.New(fmt.Sprintf("expected %d arguments, got %d", len(c.whereFieldName), len(args)))
	}
	c.checkConnection()
	q := fmt.Sprintf(c.deleteQuery)
	stmt, err := c.mysqlDB.Prepare(q)
	if err != nil {
		panic(err)
	}
	defer stmt.Close()
	res, err := stmt.Exec(args...)
	if err != nil {
		panic(err)
	}
	m := make(map[string]interface{})
	m["LastInsertId"], _ = res.LastInsertId()
	m["RowsAffected"], _ = res.RowsAffected()
	return m, nil
}