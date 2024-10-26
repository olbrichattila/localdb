// Package localdb is a local file database implementation, for built in database management
package localdb

import "fmt"

// New creates a new database manager object
func New() Manager {
	return &db{
		tableCreator: newTableCreator(),
		inserter:     newInserter(),
		stater:       newStat(),
		fetcher:      newFetcher(),
		deleter:      newDeleter(),
	}
}

// Manager is the abstracted manger interface
type Manager interface {
	Create(tableName string, tableStruct *FieldDef) error
	Open(tableName string) (*currentTable, error)
	Close(c *currentTable) error
	Insert(*currentTable, map[string]interface{}) (*currentTable, error)
	RecCount(c *currentTable) (int64, error)
	First(c *currentTable) error
	Last(c *currentTable) error
	Fetch(c *currentTable, recNo int64) (map[string]interface{}, bool, error)
	Next(c *currentTable) (map[string]interface{}, bool, error)
	Prev(c *currentTable) (map[string]interface{}, bool, error)
	Locate(c *currentTable, fieldName string, value interface{}) (map[string]interface{}, error)
	Delete(c *currentTable, recNo int64) error
	Use(c *currentTable, indexName string) error
	// Add insert
	// Add recNo
	// Add seek (index later)
	// Add update
}

type db struct {
	tableCreator tableCreator
	inserter     inserter
	stater       stater
	fetcher      fetcher
	deleter      deleter
}

func (d *db) Create(tableName string, tableStruct *FieldDef) error {
	return d.tableCreator.Create(tableName, tableStruct)
}

func (*db) Open(tableName string) (*currentTable, error) {
	return newTableOpener(tableName)
}

func (*db) Close(c *currentTable) error {
	return c.Close()
}

func (d *db) Insert(c *currentTable, data map[string]interface{}) (*currentTable, error) {
	return d.inserter.Insert(c, data)
}

func (d *db) RecCount(c *currentTable) (int64, error) {
	return d.stater.RecCount(c)
}

func (d *db) First(c *currentTable) error {
	return d.fetcher.First(c)
}

func (d *db) Last(c *currentTable) error {
	return d.fetcher.Last(c)
}

func (d *db) Fetch(c *currentTable, recNo int64) (map[string]interface{}, bool, error) {
	return d.fetcher.Fetch(c, recNo)
}

func (d *db) Next(c *currentTable) (map[string]interface{}, bool, error) {
	return d.fetcher.Next(c)
}

func (d *db) Prev(c *currentTable) (map[string]interface{}, bool, error) {
	return d.fetcher.Prev(c)
}

func (d *db) Locate(c *currentTable, fieldName string, value interface{}) (map[string]interface{}, error) {
	return d.fetcher.Locate(c, fieldName, value)
}

func (d *db) Delete(c *currentTable, recNo int64) error {
	return d.deleter.Delete(c, recNo)
}

func (d *db) Use(c *currentTable, indexName string) error {
	for _, field := range c.fieldDef.Fields {
		if field.Indexes != nil {
			for _, index := range field.Indexes {
				if index.Name == indexName {
					c.usedIndex = index.index
					return nil
				}
			}
		}
	}

	return fmt.Errorf("index '%s' does not exists, cannot use it", indexName)
}
