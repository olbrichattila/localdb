package localdb

import (
	"encoding/json"
	"godb/pkg/btree"
	filemanager "godb/pkg/file"
	"os"
)

const (
	defFileExt           = ".def"
	recordPointerFileExt = ".rpt"
	dataFileExt          = ".dat"
)

func newTableCreator() tableCreator {
	return &ct{filer: filemanager.New()}
}

type tableCreator interface {
	Create(tableName string, tableStruct *FieldDef) error
}

type ct struct {
	filer       filemanager.Filer
	tableName   string
	tableStruct *FieldDef
}

// FieldType defines the type of a field (acts like an enum)
type FieldType int

// TODO add new types, like date, blob, whatever
// Field types

// Create creates a database with it's structure
func (d *ct) Create(tableName string, tableStruct *FieldDef) error {
	d.tableName = tableName
	d.tableStruct = tableStruct
	err := d.filer.CreateDBFolderIfNotExists()
	if err != nil {
		return err
	}

	err = d.saveDefinition()
	if err != nil {
		return err
	}

	if err := d.createRecordPointerFile(); err != nil {
		return err
	}

	if err := d.createDataFile(); err != nil {
		return err
	}

	if err := d.createIndexes(); err != nil {
		return err
	}

	return nil
}

func (d *ct) createIndexes() error {
	for _, field := range d.tableStruct.Fields {

		intIndex := false
		if field.Type == FtInt {
			intIndex = true
		}

		if field.Indexes != nil {
			for _, index := range field.Indexes {
				_, err := btree.New(index.Name, field.Length, intIndex)
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (d *ct) saveDefinition() error {
	json, err := json.Marshal(d.tableStruct)
	if err != nil {
		return err
	}

	err = d.saveTableDefinitionFile(string(json))
	if err != nil {
		return err
	}

	return nil
}

func (d *ct) saveTableDefinitionFile(s string) error {
	fileName := d.tableName + defFileExt
	fullPath := d.filer.GetFullFilePath(fileName)
	file, err := os.Create(fullPath)
	if err != nil {

		return err
	}
	defer file.Close()

	_, err = file.WriteString(s)
	if err != nil {
		return err
	}

	return nil
}

func (d *ct) createRecordPointerFile() error {
	_, err := d.filer.CreateBlankFileIfNotExist(d.tableName + recordPointerFileExt)
	return err
}

func (d *ct) createDataFile() error {
	_, err := d.filer.CreateBlankFileIfNotExist(d.tableName + dataFileExt)
	return err
}
