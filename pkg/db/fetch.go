package localdb

import (
	"encoding/binary"
	"errors"
	"fmt"
	filemanager "godb/pkg/file"
	"strings"
)

const (
	errorMsgDeleted = "item %d is deleted"
)

var errNotFound = errors.New("not found")

func newFetcher() fetcher {
	return &fetch{filer: filemanager.New()}
}

type fetcher interface {
	First(c *currentTable) error
	Last(c *currentTable) error
	Fetch(c *currentTable, recNo int64) (map[string]interface{}, bool, error)
	Next(c *currentTable) (map[string]interface{}, bool, error)
	Prev(c *currentTable) (map[string]interface{}, bool, error)
	Locate(c *currentTable, fieldName string, value interface{}) (map[string]interface{}, error)
}

type fetch struct {
	filer        filemanager.Filer
	currentTable *currentTable
}

func (f *fetch) First(c *currentTable) error {
	if c.usedIndex != nil {
		index := *c.usedIndex
		ptr, _, err := index.First()
		if err != nil {
			return err
		}

		c.recordNo = ptr
	} else {
		c.recordNo = 0
	}

	return nil
}

func (f *fetch) Last(c *currentTable) error {
	if c.usedIndex != nil {
		index := *c.usedIndex

		ptr, _, err := index.Last()
		if err != nil {
			return err
		}

		c.recordNo = ptr
	} else {
		c.recordNo = c.CursorCount()
	}

	return nil
}

func (f *fetch) Fetch(c *currentTable, recNo int64) (map[string]interface{}, bool, error) {
	f.currentTable = c
	datFilePointer, isDeleted, eof, err := f.filer.GetDatFilePointer(c.fileHandlers.rpt, recNo)
	if err != nil {
		return nil, false, err
	}

	if eof {
		return nil, true, nil
	}

	if isDeleted {
		return nil, false, fmt.Errorf(errorMsgDeleted, recNo)
	}

	record, eof, err := f.filer.ReadBytes(c.fileHandlers.dat, datFilePointer, c.recordSize)
	if err != nil {
		return nil, false, err
	}

	if eof {
		return nil, true, nil
	}
	c.recordNo = recNo
	result, err := f.mapBufferToData(record)

	return result, false, err
}

func (f *fetch) Next(c *currentTable) (map[string]interface{}, bool, error) {
	return f.moveCursor(c, true)
}

func (f *fetch) Prev(c *currentTable) (map[string]interface{}, bool, error) {
	return f.moveCursor(c, false)
}

func (f *fetch) moveCursor(c *currentTable, moveDown bool) (map[string]interface{}, bool, error) {
	f.currentTable = c

	if c.usedIndex != nil {
		index := *c.usedIndex

		var ptr int64
		var eof bool
		var err error
		if moveDown {
			ptr, _, eof, err = index.Next()
		} else {
			ptr, _, eof, err = index.Prev()
		}
		if err != nil {
			return nil, false, err
		}

		if eof {
			return nil, true, nil
		}

		c.recordNo = ptr
	} else {
		if moveDown {
			c.recordNo++
		} else {
			c.recordNo--
		}
	}

	if c.recordNo == -1 {
		return nil, true, nil
	}

	datFilePointer, isDeleted, eof, err := f.filer.GetDatFilePointer(c.fileHandlers.rpt, c.recordNo)
	if err != nil {
		return nil, false, err
	}

	if isDeleted {
		return nil, false, fmt.Errorf(errorMsgDeleted, c.recordNo)
	}

	if eof {
		return nil, true, nil
	}

	record, eof, err := f.filer.ReadBytes(c.fileHandlers.dat, datFilePointer, c.recordSize)
	if err != nil {
		return nil, false, err
	}

	if eof {
		return nil, true, nil
	}

	result, err := f.mapBufferToData(record)
	return result, false, err
}

func (f *fetch) Locate(c *currentTable, fieldName string, value interface{}) (map[string]interface{}, error) {

	if c.usedIndex != nil {
		index := *c.usedIndex
		// TODO it works only with sting for now, extract this logic and implement for each type
		if v, ok := value.(string); ok {
			ptr, _, found, err := index.Search([]byte(v))
			if err != nil {
				return nil, err
			}
			if !found {
				return nil, errNotFound
			}

			val, eof, err := f.Fetch(c, ptr)
			if err != nil {
				return nil, err
			}
			if eof {
				return nil, errNotFound
			}

			return val, nil
		}
	}

	result, eof, err := f.Fetch(c, int64(0))
	if err != nil {
		return nil, err
	}

	if eof {
		return nil, errNotFound
	}

	for {
		if val, ok := result[fieldName]; ok {
			if val == value {
				return result, nil
			}
		}
		result, eof, err = f.Next(c)
		if err != nil {
			return nil, err
		}
		if eof {
			break
		}
	}

	return nil, errNotFound
}

func (f *fetch) mapBufferToData(data []byte) (map[string]interface{}, error) {
	mappedResult := make(map[string]interface{}, 0)
	index := 0
	str := ""
	var integer int64

	for _, field := range f.currentTable.fieldDef.Fields {
		switch field.Type {
		case FtText:
			index, str = f.copyBuffToStr(data, index, field.Length)
			mappedResult[field.Name] = str
		case FtBool:
			if data[index] == 0 {
				mappedResult[field.Name] = false
			} else {
				mappedResult[field.Name] = true
			}
			index++
		case FtInt:
			index, integer = f.copyBuffToInt64(data, index)
			mappedResult[field.Name] = integer
		default:
			return nil, fmt.Errorf("field type not implemented in mapBufferToData %d", field.Type)
		}
	}

	return mappedResult, nil
}

func (f *fetch) copyBuffToStr(buf []byte, index, count int) (int, string) {
	strB := &strings.Builder{}
	for {
		if buf[index] != 0 {
			strB.WriteByte(buf[index])
		}
		count--
		index++
		if count == 0 {
			break
		}
	}

	return index, strB.String()
}

func (f *fetch) copyBuffToInt64(buf []byte, index int) (int, int64) {
	int64Buf := make([]byte, filemanager.Int64Length)
	for i := 0; i < filemanager.Int64Length; i++ {
		int64Buf[i] = buf[index+i]
	}

	return index + filemanager.Int64Length, int64(binary.LittleEndian.Uint64(int64Buf))
}
