
# Golang Cache with write-behind strategy
The go-cache-wb is the cache with write behind strategy

Please read following links, to know more about caching strategies
- https://codeahoy.com/2017/08/11/caching-strategies-and-how-to-choose-the-right-one/
- https://www.infoq.com/articles/write-behind-caching

# Caution: This package is under developing

## Download cacheWB
```
go get -u github.com/irmorteza/go-cache-wb
```

## dependencies
```
github.com/go-sql-driver/mysql
```

## How it works
CacheWB is the cache work in front of the storage, and does its CRUD actions on cache. To use the cachewb, you need do following steps: 
1. **Create favor struct**: The way, how CacheWB know about the storage structure, obtain from Struct fields and its Tags. Tags are used to clarifing, detail of storage to CacheWB. Tags are:    
   - storage: Any field of struct who need to point a field in storage, should have storage tag with value of its real name in storage
      `storage:"credit"`
   - key: The key field od struct. It should be unique in storage. All CRUD action triger base of key field. `key:"1"`
   - update: The fields will updates after an change on struct value. Its default is 1 (enable). `update:"1"`
   - autoInc: Incremental field in storage. autoInc are required if you want to use insert and update. this field doesn't involve (change) in update. `autoInc:"1"`
2. **Add EmbedME:** Embed build-in struct `cachewb.EmbedME` to your struct  
3. **Handle update actions by Create functions (property)**: For specific action (for example add to a field) create an function, in that function, first call `c.EmbedME.Inc(c)`. It would inform cachewb the change on one of its item.


### Supported storage (databases) 
  - [x] MySQL  
  - [ ] SQL Server  
  - [ ] MongoDB  

### Complete example:
``` go
import (
	"github.com/irmorteza/cachewb"
	"fmt"
	"time"
)



type Members struct {
	cachewb.EmbedME
	Id       int64     `storage:"id" autoInc:"1"`
	Name     string    `storage:"name" key:"1"`
	Credit   int64     `storage:"credit"`
	Age      int64     `storage:"age" update:"0"`
	LastTime time.Time `storage:"lastTime"`
}

func (c *Members) AddCredit(a int64)  {
	e := c.EmbedME.Inc(c)
	if e != nil {
		fmt.Println(e)
		return
	}
	c.Credit += a
}

func main() {
	cfg := cachewb.Config{
		Interval:               2,
		CacheWriteLatencyCount: 2,
		CacheWriteLatencyTime:  10,
		AccessTTL:              5,
		StorageName:            cachewb.MYSQL,
		Database: cachewb.ConfigMysql{
			Username:          "irmorteza",
			Password:          "123",
			Port:              3306,
			Host:              "localhost",
			DBName:            "hss_db",
			MaxOpenConnection: -1},
	}

	c := cachewb.NewCacheWB(cfg)
	f := c.GetContainer("members", Members{})
	out := f.Get("Morteza").(*Members)
	fmt.Println(out)

}
```
