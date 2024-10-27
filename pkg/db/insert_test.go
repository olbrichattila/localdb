package localdb

import (
	filemanager "godb/pkg/file"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/suite"
)

type insertTestSuite struct {
	suite.Suite
	db Manager
	ct *CurrentTable
}

func TestInsertRunner(t *testing.T) {
	suite.Run(t, new(insertTestSuite))
}

func (t *insertTestSuite) SetupTest() {
	err := os.RemoveAll(filemanager.DefaultFolder)
	if err != nil {
		panic("Cannot run test, the folder cannot be removed " + err.Error())
	}

	t.db = New()
	tableStruct := &FieldDef{
		Fields: []Field{
			{Name: "field_1", Type: FtText, Length: 15, Required: true, Indexes: []IndexDef{{Name: "field_1"}}},
			{Name: "field_2", Type: FtBool},
			{Name: "field_3", Type: FtInt},
		},
	}
	tableName := "insert_tests"
	err = t.db.Create(tableName, tableStruct)
	if err != nil {
		panic("Cannot run test, Could not create database " + err.Error())
	}

	ct, err := t.db.Open(tableName)
	if err != nil {
		panic("Cannot open table " + err.Error())
	}

	t.ct = ct

}

func (t *insertTestSuite) TearDownTest() {
	t.ct.Close()
	t.db = nil
}

func (t *insertTestSuite) TestInsertOpenAndFetch() {
	// Assert 0 record initial
	rc, err := t.db.RecCount(t.ct)
	t.Nil(err)
	t.Equal(int64(0), rc)

	data := map[string]interface{}{
		"field_1": "Test data",
		"field_2": true,
		"field_3": int64(150),
	}
	_, err = t.db.Insert(t.ct, data)
	t.Nil(err)
	data["field_1"] = "Test2 data"
	data["field_2"] = false
	_, err = t.db.Insert(t.ct, data)
	t.Nil(err)
	data["field_1"] = "Test3 data"
	_, err = t.db.Insert(t.ct, data)
	t.Nil(err)

	// Assert 3 records added
	rc, err = t.db.RecCount(t.ct)
	t.Nil(err)
	t.Equal(int64(3), rc)

	_, _, _, err = t.db.Fetch(t.ct, 0)
	t.Nil(err)

	result, _, _, err := t.db.Fetch(t.ct, int64(0))
	t.Nil(err)

	field1, ok := result["field_1"]
	t.True(ok)
	t.Equal("Test data", field1)

	field2, ok := result["field_2"]
	t.True(ok)
	t.Equal(true, field2)

	field3, ok := result["field_3"]
	t.True(ok)
	t.Equal(int64(150), field3)

	// test delete
	err = t.db.Delete(t.ct, 1)
	t.Nil(err)

	_, _, isDeleted, _ := t.db.Fetch(t.ct, int64(1))
	t.True(isDeleted)

	res, _, _, err := t.db.Fetch(t.ct, int64(0))
	t.Nil(err)
	fieldValue, ok := res["field_1"]
	t.True(ok)
	t.Equal("Test data", fieldValue)

	t.True(ok)
	t.Equal(int64(150), field3)
	readCount := 0
	for {
		_, eof, _ := t.db.Next(t.ct)
		if eof {
			break
		}
		readCount++
	}

	t.Equal(1, readCount)
}

func (t *insertTestSuite) TestInsert100Record() {
	// Assert 0 record initial
	rc, err := t.db.RecCount(t.ct)
	t.Nil(err)
	t.Equal(int64(0), rc)

	data := map[string]interface{}{
		"field_2": true,
	}

	for i := 0; i < 1000; i++ {
		data["field_1"] = "test data " + strconv.Itoa(i)
		data["field_3"] = int64(i)
		_, err = t.db.Insert(t.ct, data)
		t.Nil(err)
	}

	// Assert 1000 records added
	rc, err = t.db.RecCount(t.ct)
	t.Nil(err)
	t.Equal(int64(1000), rc)

	_, _, _, err = t.db.Fetch(t.ct, 0)
	t.Nil(err)

	result, _, _, err := t.db.Fetch(t.ct, int64(999))
	t.Nil(err)

	field1, ok := result["field_1"]
	t.True(ok)
	t.Equal("test data 999", field1)

	field2, ok := result["field_2"]
	t.True(ok)
	t.Equal(true, field2)

	field3, ok := result["field_3"]
	t.True(ok)
	t.Equal(int64(999), field3)

	res, err := t.db.Locate(t.ct, "field_1", "test data 339")
	t.Nil(err)
	field1, ok = res["field_1"]
	t.True(ok)
	t.Equal("test data 339", field1)
}
