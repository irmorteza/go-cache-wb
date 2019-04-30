package cachewb

import (
	"fmt"
	"sync"
)

type statisticContainer struct {
	enabled bool
	storage struct {
		updates     int64
		inserts     int64
		selects     int64
		lastUpdates int64
		lastInserts int64
		lastSelects int64
	}
	cache struct {
		updates         int64
		inserts         int64
		selects         int64
		lastUpdates     int64
		lastInserts     int64
		lastSelects     int64
	}
	mu sync.RWMutex
}

func (c *statisticContainer) incStorageInserts()  {
	if ! c.enabled{
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.storage.inserts ++
}

func (c *statisticContainer) incStorageUpdates()  {
	if ! c.enabled{
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.storage.updates ++
}

func (c *statisticContainer) incStorageSelect()  {
	if ! c.enabled{
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.storage.selects ++
}

func (c *statisticContainer) incCacheInserts()  {
	if ! c.enabled{
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cache.inserts ++
}

func (c *statisticContainer) incCacheUpdates()  {
	if ! c.enabled{
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cache.updates ++
}

func (c *statisticContainer) incCacheSelect()  {
	if ! c.enabled{
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cache.selects ++
}

func (c *statisticContainer) getStatistic()  map[string]map[string]interface{} {
	c.mu.Lock()
	defer c.mu.Unlock()

	difStorageInserts := c.storage.inserts - c.storage.lastInserts
	difStorageUpdates := c.storage.updates - c.storage.lastUpdates
	difStorageSelects := c.storage.selects - c.storage.lastSelects
	c.storage.lastInserts = c.storage.inserts
	c.storage.lastUpdates = c.storage.updates
	c.storage.lastSelects= c.storage.selects

	difCacheInserts := c.cache.inserts - c.cache.lastInserts
	difCacheUpdates := c.cache.updates - c.cache.lastUpdates
	difCacheSelects := c.cache.selects- c.cache.lastSelects
	c.cache.lastInserts = c.cache.inserts
	c.cache.lastUpdates = c.cache.updates
	c.cache.lastSelects = c.cache.selects
	rUpdate := "-"
	rInsert := "-"
	rSelect := "-"
	rTotalUpdate := "-"
	rTotalInsert := "-"
	rTotalSelect := "-"
	rUPI := "0"
	rTotalUPI := "0"

	if difCacheUpdates > 0 {
		rUpdate = fmt.Sprintf("%.2f", 100 - (float64(difStorageUpdates * 100) / float64(difCacheUpdates)))
	}
	if difCacheInserts > 0 {
		rInsert = fmt.Sprintf("%.2f", 100 - (float64(difStorageInserts * 100) / float64(difCacheInserts)))
	}
	if difCacheSelects > 0 {
		rSelect = fmt.Sprintf("%.2f", 100 - (float64(difStorageSelects * 100) / float64(difCacheSelects)))
	}

	if c.cache.updates > 0 {
		rTotalUpdate = fmt.Sprintf("%.2f", 100 - (float64(c.storage.updates * 100) / float64(c.cache.updates)))
	}
	if c.cache.inserts > 0 {
		rTotalInsert = fmt.Sprintf("%.2f", 100 - (float64(c.storage.inserts * 100) / float64(c.cache.inserts)))
	}
	if c.cache.selects > 0 {
		rTotalSelect = fmt.Sprintf("%.2f", 100 - (float64(c.storage.selects * 100) / float64(c.cache.selects)))
	}
	if difStorageUpdates > 0 {
		rUPI = fmt.Sprintf("%.2f", float64(difCacheUpdates) / float64(difStorageUpdates))
	}
	if c.storage.updates > 0 {
		rTotalUPI = fmt.Sprintf("%.2f", float64(c.cache.updates) / float64(c.storage.updates))
	}


	m := map[string]map[string]interface{}{
		"cache": map[string]interface{}{
			"TotalInserts": c.cache.inserts,
			"TotalUpdates": c.cache.updates,
			"TotalSelects": c.cache.selects,
			"Inserts":      difCacheInserts,
			"Updates":      difCacheUpdates,
			"Selects":      difCacheSelects,
		},
		"storage": map[string]interface{}{
			"TotalInserts": c.storage.inserts,
			"TotalUpdates": c.storage.updates,
			"TotalSelects": c.storage.selects,
			"Inserts":      difStorageInserts,
			"Updates":      difStorageUpdates,
			"Selects":       difStorageSelects,
		},
		"Efficiency": map[string]interface{}{
			"Update":  rUpdate,
			"Insert":  rInsert,
			"Select":  rSelect,
			"TotalUpdate":  rTotalUpdate,
			"TotalInsert":  rTotalInsert,
			"TotalSelect":  rTotalSelect,
			"UPI":  rUPI,
			"TotalUPI":  rTotalUPI,
		},
	}
	return m
}
