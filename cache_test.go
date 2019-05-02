package cachewb

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"
)

var cfg = Config{
	IntervalWorkerMaintainer:      2,
	CacheFlushUpdatesLatencyCount: 10,
	CacheFlushUpdatesLatencyTime:  10,
	Log:                           false,
	Statistic:                     true,
	AccessTTLItems:                10,
	StorageName:                   MYSQL,
	Database: ConfigMysql{
		Username:          "root",
		Password:          "123",
		Port:              3306,
		Host:              "localhost",
		DBName:            "hss_db",
		MaxOpenConnection: -1},
}

type Members struct {
	/*
		CREATE TABLE hss_db.members (
		  id int(11) UNSIGNED NOT NULL AUTO_INCREMENT,
		  name varchar(50) DEFAULT NULL,
		  credit int(11) DEFAULT NULL,
		  age smallint(6) DEFAULT NULL,
		  lastTime datetime DEFAULT NULL,
		  PRIMARY KEY (id)
		)
	*/
	EmbedME
	Id       int64     `storage:"id" autoInc:"1" updateKey:"1" uniqueIdentity:"1"`
	Name     string    `storage:"name" unique:"0"`
	Credit   int64     `storage:"credit"`
	Age      int64     `storage:"age"`
	Category int64     `storage:"category"`
	LastTime time.Time `storage:"lastTime"`
}

func (m * Members)AddCredit(a int64){
	m.Credit += a
	m.EmbedME.IncUpdate()
}

func printStatistic(cc * CacheContainer)  {
	//defer func() {
	//	fmt.Println(fmt.Sprintf("-----------------------------------------------------------------------------------------------------------------------"))
	//}()
	fmt.Println(fmt.Sprintf("____________________________________________________________________________________________________"))
	fmt.Println(fmt.Sprintf("      |        Cache              |       Storage              |       Performance                  |"))
	fmt.Println(fmt.Sprintf("------|---------------------------|----------------------------|------------------------------------|"))
	fmt.Println(fmt.Sprintf("------|%-8s %-8s %-8s | %-8s %-8s %-8s | %-8s %-8s %-8s %-8s|", "Select%", "Update%", "Insert%", "Select", "Update", "Insert", "Select", "Update", "Insert", "UPI"))
	fmt.Println(fmt.Sprintf("------|---------------------------|----------------------------|------------------------------------|"))
	var f int64
	for true {
		r := cc.GetStatistic()
		fmt.Println(fmt.Sprintf("------|%-8d %-8d %-8d | %-8d %-8d %-8d | %-8s %-8s %-8s %-8s|",
			r["cache"]["Selects"],
			r["cache"]["Updates"],
			r["cache"]["Inserts"],
			r["storage"]["Selects"],
			r["storage"]["Updates"],
			r["storage"]["Inserts"],
			r["Efficiency"]["Select"],
			r["Efficiency"]["Update"],
			r["Efficiency"]["Insert"],
			r["Efficiency"]["UPI"],
		))

		if f > 0 && math.Mod(float64(f),10) == 0{
			fmt.Println(fmt.Sprintf("------|---------------------------|----------------------------|------------------------------------|"))
			fmt.Println(fmt.Sprintf("Total |%-8d %-8d %-8d | %-8d %-8d %-8d | %-8s %-8s %-8s %-8s|",
				r["cache"]["TotalSelects"],
				r["cache"]["TotalUpdates"],
				r["cache"]["TotalInserts"],
				r["storage"]["TotalSelects"],
				r["storage"]["TotalUpdates"],
				r["storage"]["TotalInserts"],
				r["Efficiency"]["TotalSelect"],
				r["Efficiency"]["TotalUpdate"],
				r["Efficiency"]["TotalInsert"],
				r["Efficiency"]["TotalUPI"],
			))
			fmt.Println(fmt.Sprintf("------|---------------------------|----------------------------|------------------------------------|"))
			fmt.Println(fmt.Sprintf("------|%-8s %-8s %-8s | %-8s %-8s %-8s | %-8s %-8s %-8s %-8s|", "Select", "Update", "Insert", "Select", "Update", "Insert", "Select", "Update", "Insert", "UPI"))
			fmt.Println(fmt.Sprintf("------|---------------------------|----------------------------|------------------------------------|"))
		}
		f++
		time.Sleep(time.Second * 1)

	}
	//fmt.Println(fmt.Sprintf("______________________________________________________________________________________________________________________________________________"))
	//fmt.Println(fmt.Sprintf("Cache |%-12s %-12s %-8s %-8s %-8s | Storage %-12s %-12s %-8s %-8s %-8s | %-8s %-8s|", "TotalInsert", "TolalUpdate", "Select", "Insert", "Update", "TotalInsert", "TolalUpdate", "Select", "Insert", "Update", "eUpdate", "eInsert"))
	//fmt.Println(fmt.Sprintf("------|-----------------------------------------------------|--------------------------------------------------------------|------------------|"))
	//for true {
	//	time.Sleep(time.Second * 1)
	//	r := cc.GetStatistic()
	//	fmt.Println(fmt.Sprintf("      |%-12d %-12d %-8d %-8d %-8d |         %-12d %-12d %-8d %-8d %-8d | %-8s %-8s|",
	//		r["cache"]["TotalInserts"],
	//		r["cache"]["TotalUpdates"],
	//		r["cache"]["Selects"],
	//		r["cache"]["Inserts"],
	//		r["cache"]["Updates"],
	//		r["storage"]["TotalInserts"],
	//		r["storage"]["TotalUpdates"],
	//		r["storage"]["Selects"],
	//		r["storage"]["Inserts"],
	//		r["storage"]["Updates"],
	//		fmt.Sprintf("%d%%",r["Efficiency"]["Update"]),
	//		fmt.Sprintf("%d%%",r["Efficiency"]["Insert"])))
	//}
}

func TestCRUD(t *testing.T)  {
	t.Skip("Skipping")
	cwb := NewCacheWB()
	c := cwb.GetContainer("members", cfg, Members{})
	testMemberName := "Smith"

	// Preparing database for test. removeByUniqueIdentity Smiths
	_, e5 := c.RemoveIndirect(map[string]interface{}{"name": testMemberName})
	if e5 != nil{
		t.Fatal(e5)
	}

	// insert Smith to Members
	m := Members{Name:testMemberName, Age:10, Credit:100}
	resInsert, e0 := c.Insert(m)
	if e0 != nil{
		t.Fatal(e0)
	}
	if resInsert["RowsAffected"] <= 0{
		t.Fatalf("Unable to add user %s to storage", testMemberName)
	}

	// get Smith from cache
	r1, e := c.Get(map[string]interface{}{"name": testMemberName})
	if e != nil{
		t.Fatal(e)
	}
	if r1 == nil || len(r1) == 0{
		t.Fatalf("%s not found" , testMemberName)
	}
	if len(r1) > 1{
		t.Fatalf("There are more than 1 %s in database. found: %d" , testMemberName, len(r1))
	}
	r2 := r1[0].(*Members)
	if r2.Name != testMemberName {
		t.Errorf("%s not found", testMemberName)
	}
	// update Smith
	r2.AddCredit(10)
	time.Sleep(time.Second * time.Duration(cfg.CacheFlushUpdatesLatencyTime + 5))
	if r2.EmbedME.updates > 0{
		t.Fatalf("There is problem in update.")
	}
	// check storage for update
	r10, e10:= c.storage.get([]string{"name"}, []interface{}{testMemberName})
	if e10 == nil && r10 != nil && len(r10) == 1{
		if r10[0].(*Members).Credit != r2.Credit {
			t.Fatalf("Update of %s didn't flushed to storage, may sleep time is not enough", testMemberName)
		}
	}else {
		t.Fatalf("Ubable to get %s from storage" , testMemberName)
	}

	// delete Smith from cache
	resRemove, e3 := c.Remove(r2.Id)
	if e3 != nil{
		t.Fatal(e3)
	}
	if resRemove["RowsAffected"] <= 0{
		t.Fatalf("Unable to removeByUniqueIdentity user %s to storage", testMemberName)
	}
}

func TestPerformance(t *testing.T) {
	t.Skip("Skipping")
	cwb := NewCacheWB()
	c := cwb.GetContainer("members", cfg, Members{})
	// Preparing database for test. removeByUniqueIdentity old data
	go printStatistic(c)

	_, eee := c.RemoveIndirect(map[string]interface{}{"category": 100})
	if eee != nil{
		t.Fatal(eee)
	}

	Num := 100000
	var updatedCredit int64
	// insert Smith to Members
	for i := 0; i < Num; i++ {
		name := fmt.Sprintf("Jack%d", i)
		m := Members{Name: name, Age: 150, Credit: 0, Category: 100}
		c.InsertAsync(m)
	}

	// waiting for flush InsertAsync(s) records to storage
	for true {
		time.Sleep(time.Second * 1)
		if len(c.insertABuffer) == 0{
			break
		}
	}

	ch := make(chan bool)
	go func() {
		for true {
			tAfter := time.After(time.Millisecond * 1)
			select {
			case <-tAfter:
				name := fmt.Sprintf("Jack%d", rand.Intn(1000))
				r, _ := c.Get(map[string]interface{}{"name": name})
				r[0].(*Members).AddCredit(1)
				updatedCredit += 1
				//fmt.Println(r, e)
				//time.Sleep(time.Millisecond * 100)
			case <-ch:
				return
			}
		}
	}()

	time.Sleep(time.Second*30)
	ch <- true
	// delete Smith from cache
	c.Flush(true)

	cwb2 := NewCacheWB()
	ccc := cwb2.GetContainer("members", cfg, Members{})
	var finalCredit int64

	ccc.Get(map[string]interface{}{"category": 100})
	for _, item := range ccc.items {
		finalCredit += item.(*Members).Credit
	}
	//fmt.Println(updatedCredit, finalCredit)
	if updatedCredit != finalCredit{
		t.Fatalf("Expect increase total credit %d, got:%d", updatedCredit, finalCredit)
	}

	resRemove, e3 := c.RemoveIndirect(map[string]interface{}{"category": 100})
	if e3 != nil{
		t.Fatal(e3)
	}
	t.Logf(fmt.Sprintf("%d cache updates flushed by %d updates to storage", updatedCredit, c.statistic.storage.updates))
	if resRemove["RowsAffected"] != int64(Num){
		t.Fatalf("Expect to removeByUniqueIdentity %d, removed %d", Num, resRemove["RowsAffected"])
	}
}
