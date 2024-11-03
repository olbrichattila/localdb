package localdb

import (
	"encoding/json"
	"fmt"
	"godb/pkg/btree"
	filemanager "godb/pkg/file"
	"os"
	"strings"
)

// todo add index defs maybe for FileDef but probably not due to possible combined indexes
// todo add transactions

// CurrentTable holds the table info
type CurrentTable struct {
	tableName    string
	fieldDef     FieldDef
	recordNo     int64
	recordCount  int64
	fileHandlers fileHandlers
	filer        filemanager.Filer
	recordSize   int
	userIndex    *btree.BTree
}

type fileHandlers struct {
	dat *os.File
	rpt *os.File
}

// Field types
const (
	FtText FieldType = iota + 1
	FtBool
	FtInt
	FtReal
)

// FieldDef holds a struct of a new fields
type FieldDef struct {
	Fields []Field
}

// Field is the definition of a field
type Field struct {
	Type     FieldType
	Name     string
	Length   int
	Required bool
	Indexes  []IndexDef
}

// IndexDef of the table index
type IndexDef struct {
	Type  string
	Name  string
	index *btree.BTree // in future it may go to different indexes or interface and resolve by Type later
}

// CursorPos returns the current cursor position
func (c *CurrentTable) CursorPos() int64 {
	return c.recordNo
}

// CursorCount Return the number of cursors (rows)
func (c *CurrentTable) CursorCount() int64 {
	if c.fileHandlers.rpt == nil {
		return 0
	}

	stat, err := c.fileHandlers.rpt.Stat()
	if err != nil {
		return 0
	}

	// File size / File pointer + deleted flag
	return stat.Size() / (filemanager.Int64Length + 1)
}

// Close closes the file handles in the table
func (c *CurrentTable) Close() error {
	errors := make([]string, 0)
	err := c.fileHandlers.dat.Close()
	if err != nil {
		errors = append(errors, err.Error())
	}

	err = c.fileHandlers.rpt.Close()
	if err != nil {
		errors = append(errors, err.Error())
	}

	// close indexes
	for _, field := range c.fieldDef.Fields {
		if field.Indexes != nil {
			for _, index := range field.Indexes {
				index := *index.index
				err := index.Close()
				if err != nil {
					errors = append(errors, err.Error())
				}
			}
		}
	}

	if len(errors) == 0 {
		return nil
	}

	return fmt.Errorf("errors closing files : %s", strings.Join(errors, ", "))
}

func newTableOpener(tableName string) (*CurrentTable, error) {
	o := &CurrentTable{
		tableName: tableName,
		filer:     filemanager.New(),
	}
	table, err := o.init()
	if err != nil {
		return nil, err
	}
	return table, nil
}

func (c *CurrentTable) init() (*CurrentTable, error) {
	var err error
	c.recordCount, err = c.recCount()
	if err != nil {
		return c, err
	}
	var fDef FieldDef
	fileName := c.tableName + defFileExt
	fullPath := c.filer.GetFullFilePath(fileName)

	data, err := os.ReadFile(fullPath)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(data, &fDef)
	if err != nil {
		return nil, fmt.Errorf("error parsing table definition: %s", err.Error())
	}

	// Set some defaults
	c.fieldDef = fDef
	c.recordNo = 0
	err = c.openPointerFile()
	if err != nil {
		return nil, err
	}

	err = c.openDatFile()
	if err != nil {
		return nil, err
	}

	rs, err := c.calculateRecordSize()
	if err != nil {
		return nil, err
	}

	c.recordSize = rs

	err = c.openIndexes()
	if err != nil {
		return nil, err
	}

	return c, nil
}

// Struct returns with the structure of the table
func (c *CurrentTable) Struct() *FieldDef {
	return &c.fieldDef
}

func (c *CurrentTable) openIndexes() error {
	for x, field := range c.fieldDef.Fields {
		intIndex := false
		if field.Type == FtInt {
			intIndex = true
		}
		if field.Indexes != nil {
			for y, index := range field.Indexes {
				bTree, err := btree.New(index.Name, field.Length, intIndex)
				if err != nil {
					return err
				}
				c.fieldDef.Fields[x].Indexes[y].index = &bTree
			}
		}
	}

	return nil
}

func (c *CurrentTable) openPointerFile() error {
	filePath := c.filer.GetFullFilePath(c.tableName + recordPointerFileExt)
	file, err := c.openRw(filePath)
	if err != nil {
		return err
	}

	c.fileHandlers.rpt = file
	return nil
}

func (c *CurrentTable) openDatFile() error {
	filePath := c.filer.GetFullFilePath(c.tableName + dataFileExt)
	file, err := c.openRw(filePath)
	if err != nil {
		return err
	}

	c.fileHandlers.dat = file
	return nil
}

func (*CurrentTable) openRw(filePath string) (*os.File, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %s", err)
	}

	return file, nil
}

func (c *CurrentTable) calculateRecordSize() (int, error) {
	size := 0

	for _, field := range c.fieldDef.Fields {
		switch field.Type {
		case FtText:
			size += field.Length
		case FtBool:
			size++
		case FtInt:
			size += filemanager.Int64Length
		default:
			return 0, fmt.Errorf("field type not implemented in calculateRecordSize %d", field.Type)
		}
	}

	return size, nil
}

func (c *CurrentTable) recCount() (int64, error) {
	filePath := c.filer.GetFullFilePath(c.tableName + recordPointerFileExt)

	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return 0, err
	}

	return fileInfo.Size() / filemanager.PointerRecordLength, nil
}
