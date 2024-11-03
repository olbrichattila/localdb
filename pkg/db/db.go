// Package localdb is a local file database implementation, for built in database management
package localdb

import "fmt"

// New creates a new database manager object
func New() Manager {
	return &db{
		tableCreator: newTableCreator(),
		inserter:     newInserter(),
		fetcher:      newFetcher(),
		deleter:      newDeleter(),
	}
}

// Manager is the abstracted manger interface
type Manager interface {
	Create(tableName string, tableStruct *FieldDef) error
	Open(tableName string) (*CurrentTable, error)
	Struct(c *CurrentTable) *FieldDef
	Close(c *CurrentTable) error
	Insert(*CurrentTable, map[string]interface{}) (*CurrentTable, error)
	RecCount(c *CurrentTable) (int64, error)
	First(c *CurrentTable) error
	Last(c *CurrentTable) error
	Fetch(c *CurrentTable, recNo int64) (map[string]interface{}, bool, bool, error)
	FetchCurrent(c *CurrentTable) (map[string]interface{}, bool, bool, error)
	Next(c *CurrentTable) (bool, error)
	Prev(c *CurrentTable) (bool, error)
	Locate(c *CurrentTable, fieldName string, value interface{}) (map[string]interface{}, error)
	Seek(c *CurrentTable, value interface{}) error
	Delete(c *CurrentTable, recNo int64) error
	Use(c *CurrentTable, indexName string) error
	// Add recNo
	// Add update
}

type db struct {
	tableCreator tableCreator
	inserter     inserter
	fetcher      fetcher
	deleter      deleter
}

// Create creates a database with it's structure
func (d *db) Struct(c *CurrentTable) *FieldDef {
	return c.Struct()
}

// Create creates a database with it's structure
func (d *db) Create(tableName string, tableStruct *FieldDef) error {
	return d.tableCreator.Create(tableName, tableStruct)
}

// Open is opening a new table wit it's indexes
func (*db) Open(tableName string) (*CurrentTable, error) {
	return newTableOpener(tableName)
}

// Close closes the table and it's indexes
func (*db) Close(c *CurrentTable) error {
	return c.Close()
}

// Insert adds a new row to the table, update indexes
func (d *db) Insert(c *CurrentTable, data map[string]interface{}) (*CurrentTable, error) {
	return d.inserter.Insert(c, data)
}

// RecCount returns with the number of records in the table
func (d *db) RecCount(c *CurrentTable) (int64, error) {
	return c.recCount()
}

// First moves the table or index if in use to the first position, returns first value
func (d *db) First(c *CurrentTable) error {
	return d.fetcher.First(c)
}

// Last moves the table or index if in use to the last position, returns last value
func (d *db) Last(c *CurrentTable) error {
	return d.fetcher.Last(c)
}

// Fetch gets the row by it's record number (no index used)
func (d *db) Fetch(c *CurrentTable, recNo int64) (map[string]interface{}, bool, bool, error) {
	return d.fetcher.Fetch(c, recNo)
}

// FetchCurrent gets the row where the cursor was moved last time
func (d *db) FetchCurrent(c *CurrentTable) (map[string]interface{}, bool, bool, error) {
	return d.fetcher.FetchCurrent(c)
}

// Next moves the table, or index cursor the the next element (if index is in use)
func (d *db) Next(c *CurrentTable) (bool, error) {
	return d.fetcher.Next(c)
}

// Prev moves the table, or index cursor the the previous element (if index is in use)
func (d *db) Prev(c *CurrentTable) (bool, error) {
	return d.fetcher.Prev(c)
}

// Locate tries to find the row by the provided value, if index is in use, it uses the index to get the value, then returns the element
func (d *db) Locate(c *CurrentTable, fieldName string, value interface{}) (map[string]interface{}, error) {
	return d.fetcher.Locate(c, fieldName, value)
}

// Seek tries to set the index cursor to the closest element in the tree
func (d *db) Seek(c *CurrentTable, value interface{}) error {
	return d.fetcher.Seek(c, value)
}

// Delete deletes / mark as deleted the record (index not used, record id needs to be provided)
func (d *db) Delete(c *CurrentTable, recNo int64) error {
	return d.deleter.Delete(c, recNo)
}

// Use will set an index to be used for locate, seek, next, prior, first, last
func (d *db) Use(c *CurrentTable, indexName string) error {
	// Empty string resets using no index
	if indexName == "" {
		c.userIndex = nil
		return nil
	}

	for _, field := range c.fieldDef.Fields {
		if field.Indexes != nil {
			for _, index := range field.Indexes {
				if index.Name == indexName {
					c.userIndex = index.index
					return nil
				}
			}
		}
	}

	return fmt.Errorf("index '%s' does not exists, cannot use it", indexName)
}
