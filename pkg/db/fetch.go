package localdb

import (
	"encoding/binary"
	"errors"
	"fmt"
	filemanager "godb/pkg/file"
	"strings"
)

var errNotFound = errors.New("not found")

func newFetcher() fetcher {
	return &fetch{filer: filemanager.New()}
}

type fetcher interface {
	First(c *CurrentTable) error
	Last(c *CurrentTable) error
	Fetch(c *CurrentTable, recNo int64) (map[string]interface{}, bool, bool, error)
	FetchCurrent(c *CurrentTable) (map[string]interface{}, bool, bool, error)
	Next(c *CurrentTable) (bool, error)
	Prev(c *CurrentTable) (bool, error)
	Locate(c *CurrentTable, fieldName string, value interface{}) (map[string]interface{}, error)
	Seek(c *CurrentTable, value interface{}) error
}

type fetch struct {
	filer        filemanager.Filer
	CurrentTable *CurrentTable
}

func (f *fetch) First(c *CurrentTable) error {
	if c.userIndex != nil {
		index := *c.userIndex
		ptr, _, err := index.First()
		if err != nil {
			return err
		}

		c.recordNo = ptr
	} else {
		c.recordNo = 0
	}

	_, _, isDeleted, err := f.Fetch(c, c.recordNo)
	if err != nil {
		return err
	}
	if isDeleted {
		_, err := f.Next(c)
		if err != nil {
			return err
		}

	}
	return nil
}

func (f *fetch) Last(c *CurrentTable) error {
	if c.userIndex != nil {
		index := *c.userIndex

		ptr, _, err := index.Last()
		if err != nil {
			return err
		}

		c.recordNo = ptr
	} else {
		c.recordNo = c.CursorCount() - 1
	}

	_, _, isDeleted, err := f.Fetch(c, c.recordNo)
	if err != nil {
		return err
	}

	if isDeleted {
		_, err := f.Prev(c)
		if err != nil {
			return err
		}
		return nil
	}

	return nil
}

func (f *fetch) FetchCurrent(c *CurrentTable) (map[string]interface{}, bool, bool, error) {
	return f.Fetch(c, c.recordNo)
}

func (f *fetch) Fetch(c *CurrentTable, recNo int64) (map[string]interface{}, bool, bool, error) {
	f.CurrentTable = c
	datFilePointer, isDeleted, eof, err := f.filer.GetDatFilePointer(c.fileHandlers.rpt, recNo)
	if err != nil {
		return nil, false, false, err
	}

	c.recordNo = recNo

	if eof {
		return nil, true, false, nil
	}

	if isDeleted {
		return nil, false, true, nil
	}

	record, eof, err := f.filer.ReadBytes(c.fileHandlers.dat, datFilePointer, c.recordSize)
	if err != nil {
		return nil, false, false, err
	}

	if eof {
		return nil, true, false, nil
	}

	c.recordNo = recNo
	result, err := f.mapBufferToData(record)
	result["_recNo"] = recNo

	return result, false, false, err
}

func (f *fetch) Next(c *CurrentTable) (bool, error) {
	return f.moveCursor(c, true)
}

func (f *fetch) Prev(c *CurrentTable) (bool, error) {
	return f.moveCursor(c, false)
}

func (f *fetch) moveCursor(c *CurrentTable, moveDown bool) (bool, error) {
	f.CurrentTable = c

	if c.userIndex != nil {
		index := *c.userIndex

		var ptr int64
		var eof bool
		var err error
		if moveDown {
			ptr, _, eof, err = index.Next()
		} else {
			ptr, _, eof, err = index.Prev()
		}
		if err != nil {
			return false, err
		}

		if eof {
			return true, nil
		}

		c.recordNo = ptr
	} else {
		if moveDown {
			c.recordNo++
		} else {
			c.recordNo--
		}
	}

	if !moveDown && c.recordNo == -1 {
		c.recordNo = 0
		return true, nil
	}

	if moveDown && c.recordNo >= c.recordCount {
		c.recordNo = c.recordCount - 1
		return true, nil
	}

	return false, nil
}

func (f *fetch) Locate(c *CurrentTable, fieldName string, value interface{}) (map[string]interface{}, error) {
	if c.userIndex != nil {
		index := *c.userIndex
		// TODO it works only with sting for now, extract this logic and implement for each type
		if v, ok := value.(string); ok {
			ptr, _, found, err := index.Search([]byte(v))
			if err != nil {
				return nil, err
			}
			if !found {
				return nil, errNotFound
			}

			val, eof, isDeleted, err := f.Fetch(c, ptr)
			if err != nil {
				return nil, err
			}

			if eof {
				return nil, errNotFound
			}

			if isDeleted {
				return val, fmt.Errorf("record is deleted")
			}

			return val, nil
		}
	}

	result, eof, _, err := f.Fetch(c, int64(0))
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
		eof, err = f.Next(c)
		if err != nil {
			return nil, err
		}
		if eof {
			break
		}
	}

	return nil, errNotFound
}

// Seek tries to set the index cursor to the closest element in the tree
func (f *fetch) Seek(c *CurrentTable, value interface{}) error {
	if c.userIndex == nil {
		return fmt.Errorf("seek only works if index is is use")
	}

	index := *c.userIndex
	// TODO it works only with sting for now, extract this logic and implement for each type
	if v, ok := value.(string); ok {
		recNo, _, _, err := index.Search([]byte(v))
		if err != nil {
			return err
		}
		c.recordNo = recNo

		return nil
	}

	return fmt.Errorf("Seek not yet implemented for the requested field type")
}

func (f *fetch) mapBufferToData(data []byte) (map[string]interface{}, error) {
	mappedResult := make(map[string]interface{}, 0)
	index := 0
	str := ""
	var integer int64

	for _, field := range f.CurrentTable.fieldDef.Fields {
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
