package localdb

import (
	"encoding/binary"
	"fmt"
	filemanager "godb/pkg/file"
	"io"
)

type inserter interface {
	Insert(*currentTable, map[string]interface{}) (*currentTable, error)
}

type ins struct {
	filer        filemanager.Filer
	currentTable *currentTable
}

func newInserter() inserter {
	return &ins{
		filer: filemanager.New(),
	}
}

func (i *ins) Insert(c *currentTable, data map[string]interface{}) (*currentTable, error) {
	i.currentTable = c
	record, err := i.dataAsBytes(data)
	if err != nil {
		return nil, err
	}

	fPtr, err := i.addToDatFile(record)
	if err != nil {
		return nil, err
	}

	recordNo, err := i.addToRecordPointerFile(fPtr)
	if err != nil {
		return nil, err
	}

	i.addToIndexIfIndexed(data, recordNo)
	return i.currentTable, nil
}

func (i *ins) addToIndexIfIndexed(data map[string]interface{}, recordPtr int64) error {
	// var wg sync.WaitGroup
	for _, field := range i.currentTable.fieldDef.Fields {
		if field.Indexes != nil {
			for _, index := range field.Indexes {
				var value interface{}
				var err error
				if val, ok := data[field.Name]; ok {
					value = val
				}

				buf, err := i.convertToFileData(field, value)
				if err != nil {
					return err
				}
				index := *index.index

				err = index.Insert(buf, recordPtr)
				if err != nil {
					return err
				}

				// wg.Add(1)
				// go func(b []byte, r int64) {
				// 	defer wg.Done()
				// 	index.Insert(b, r)
				// }(buf, recordPtr)
			}

		}

	}

	// wg.Wait()
	return nil
}

func (i *ins) addToDatFile(data []byte) (int64, error) {
	offset, err := i.currentTable.fileHandlers.dat.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}

	_, err = i.currentTable.fileHandlers.dat.Write(data)
	if err != nil {
		return 0, err
	}

	return offset, nil
}

func (i *ins) addToRecordPointerFile(value int64) (int64, error) {
	// Make sure we are at the end of the file
	offset, err := i.currentTable.fileHandlers.rpt.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}

	buf := make([]byte, filemanager.Int64Length) // int64 is 8 bytes
	binary.LittleEndian.PutUint64(buf, uint64(value))
	buf = append(buf, 0) // add not deleted + 1 deleted flag

	_, err = i.currentTable.fileHandlers.rpt.Write(buf)
	if err != nil {
		return 0, err
	}

	return offset / filemanager.PointerRecordLength, nil
}

func (i *ins) dataAsBytes(data map[string]interface{}) ([]byte, error) {
	result := make([]byte, 0)

	for _, field := range i.currentTable.fieldDef.Fields {
		var value interface{}
		var err error
		if val, ok := data[field.Name]; ok {
			value = val
		}

		converted, err := i.convertToFileData(field, value)
		if err != nil {
			return nil, err
		}
		result = append(result, converted...)
	}

	return result, nil
}

func (i *ins) convertToFileData(field Field, value interface{}) ([]byte, error) {
	switch field.Type {
	case FtText:
		return i.convertFkText(field, value)
	case FtBool:
		return i.convertFkBool(field, value)
	case FtInt:
		return i.convertFkInt(field, value)

	}

	return nil, fmt.Errorf("non implemented field type")
}

func (i *ins) convertFkText(field Field, value interface{}) ([]byte, error) {
	if val, ok := value.(string); ok {
		res := make([]byte, field.Length)
		copy(res, val)
		return res, nil
	}

	return nil, fmt.Errorf("field %s requires string value in data map", field.Name)
}

func (i *ins) convertFkBool(field Field, value interface{}) ([]byte, error) {
	if val, ok := value.(bool); ok {
		if val {
			return []byte{1}, nil
		}

		return []byte{0}, nil
	}

	return nil, fmt.Errorf("field %s requires bool value in data map", field.Name)
}

func (i *ins) convertFkInt(field Field, value interface{}) ([]byte, error) {
	if val, ok := value.(int64); ok {
		buf := make([]byte, filemanager.Int64Length) // int64 is 8 bytes
		binary.LittleEndian.PutUint64(buf, uint64(val))

		return buf, nil

	}

	return nil, fmt.Errorf("field %s requires int64 value in data map", field.Name)
}
