package localdb

import (
	filemanager "godb/pkg/file"
	"os"
	"testing"

	"github.com/stretchr/testify/suite"
)

type createTestSuite struct {
	suite.Suite
	db Manager
}

func TestCreateRunner(t *testing.T) {
	suite.Run(t, new(createTestSuite))
}

func (t *createTestSuite) SetupTest() {

	err := os.RemoveAll(filemanager.DefaultFolder)
	if err != nil {
		panic("Cannot run test, the folder cannot be removed " + err.Error())
	}

	t.db = New()
}

func (t *createTestSuite) TearDownTest() {
	t.db = nil
}

func (t *createTestSuite) TestTableCreate() {
	tableStruct := &FieldDef{}
	tableName := "test_table"
	err := t.db.Create(tableName, tableStruct)
	t.Nil(err)
	t.FileExists(filemanager.DefaultFolder + "/" + tableName + defFileExt)
	t.FileExists(filemanager.DefaultFolder + "/" + tableName + recordPointerFileExt)
	t.FileExists(filemanager.DefaultFolder + "/" + tableName + dataFileExt)
}

func (t *createTestSuite) TestTableOpen() {
	tableStruct := &FieldDef{
		Fields: []Field{
			{Name: "field_1", Type: FtText, Length: 15, Required: true},
			{Name: "field_2", Type: FtBool},
		},
	}
	tableName := "test_table2"
	err := t.db.Create(tableName, tableStruct)
	t.Nil(err)

	opened, err := t.db.Open(tableName)
	t.Nil(err)

	t.Equal(tableName, opened.tableName)
	t.Equal(int64(0), opened.recordNo)

	t.Len(opened.fieldDef.Fields, 2)
	t.Equal(FtText, opened.fieldDef.Fields[0].Type)
	t.Equal(15, opened.fieldDef.Fields[0].Length)
	t.Equal(true, opened.fieldDef.Fields[0].Required)

	t.Equal(FtBool, opened.fieldDef.Fields[1].Type)
}
