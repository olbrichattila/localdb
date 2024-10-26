package btree

import (
	"fmt"
	filemanager "godb/pkg/file"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/suite"
)

type nodeTestSuite struct {
	suite.Suite
	node *Node
}

func TestNodeRunner(t *testing.T) {
	suite.Run(t, new(nodeTestSuite))
}

func (t *nodeTestSuite) SetupTest() {
	err := os.RemoveAll(filemanager.DefaultFolder)
	if err != nil {
		panic("Cannot run test, the folder cannot be removed " + err.Error())
	}

	testFileName := "nodetest.node"
	filer := filemanager.New()

	filer.CreateBlankFileIfNotExist(testFileName)
	file, err := filer.OpenReadWrite(testFileName)
	if err != nil {
		panic(err)
	}

	filer.WriteInt64(file, 0, 8)
	t.node = NewNode(file, filer, 6, 5, 0)
	t.node.save(8)
}

func (t *nodeTestSuite) TearDownTest() {
	t.node = nil
}

func (t *nodeTestSuite) TestSaveLoadNode() {
	testData := []DataItem{
		{data: []byte("hello"), children: 1123, mapPtr: 3210},
		{data: []byte("hell1"), children: 1124, mapPtr: 45454},
		{data: []byte("hell2"), children: 1125, mapPtr: 5778},
		{data: []byte("hell3"), children: 1126, mapPtr: 0},
		{data: []byte("hell4"), children: 1127, mapPtr: 1454},
		{data: []byte("hell5"), children: 1128, mapPtr: 555},
	}
	t.node.data = testData
	t.node.leftChild = 57846
	t.node.parentNodePtr = 123456

	t.node.save(8)

	t.node.data = nil
	t.node.leftChild = 0
	t.node.parentNodePtr = 0

	t.node.load(8)
	t.Equal(int64(57846), t.node.leftChild)
	t.Equal(int64(123456), t.node.parentNodePtr)

	for i, nd := range testData {
		t.Equal(string(t.node.data[i].data), string(nd.data))
		t.Equal(testData[i].children, nd.children)
		t.Equal(testData[i].mapPtr, nd.mapPtr)
	}
}

func (t *nodeTestSuite) TestFindPreviousByKey() {
	t.node.insert([]byte("00005"), 5)
	t.node.insert([]byte("00010"), 6)
	t.node.insert([]byte("00015"), 7)
	t.node.insert([]byte("00020"), 7)

	find := []byte("00012")
	idx := t.node.findPreviousNodeByKey(&find)
	t.Equal(1, idx)

	find = []byte("00003")
	idx = t.node.findPreviousNodeByKey(&find)
	t.Equal(-1, idx)

	find = []byte("00015")
	idx = t.node.findPreviousNodeByKey(&find)
	t.Equal(1, idx)

	find = []byte("00022")
	idx = t.node.findPreviousNodeByKey(&find)
	t.Equal(3, idx)

}
func (t *nodeTestSuite) TestLocate() {
	t.node.insert([]byte("00009"), 5)
	t.node.insert([]byte("00003"), 6)
	t.node.insert([]byte("00012"), 7)

	t.Equal("00003", string(t.node.data[0].data))
	t.Equal("00009", string(t.node.data[1].data))
	t.Equal("00012", string(t.node.data[2].data))

	t.True(t.node.data[0].isSet)
	t.True(t.node.data[1].isSet)
	t.True(t.node.data[2].isSet)
	t.False(t.node.data[3].isSet)

	item := []byte("00001")
	itemIndex, found := t.node.locateInData(&item)
	t.False(found)
	t.Equal(0, itemIndex)

	item = []byte("00002")
	itemIndex, found = t.node.locateInData(&item)
	t.False(found)
	t.Equal(0, itemIndex)

	item = []byte("00003")
	itemIndex, found = t.node.locateInData(&item)
	t.True(found)
	t.Equal(0, itemIndex)

	item = []byte("00004")
	itemIndex, found = t.node.locateInData(&item)
	t.False(found)
	t.Equal(1, itemIndex)

	item = []byte("00006")
	itemIndex, found = t.node.locateInData(&item)
	t.False(found)
	t.Equal(1, itemIndex)

	item = []byte("00006")
	itemIndex, found = t.node.locateInData(&item)
	t.False(found)
	t.Equal(1, itemIndex)

	item = []byte("00009")
	itemIndex, found = t.node.locateInData(&item)
	t.True(found)
	t.Equal(1, itemIndex)

	item = []byte("00010")
	itemIndex, found = t.node.locateInData(&item)
	t.False(found)
	t.Equal(2, itemIndex)

	item = []byte("00012")
	itemIndex, found = t.node.locateInData(&item)
	t.True(found)
	t.Equal(2, itemIndex)

	item = []byte("00013")
	itemIndex, found = t.node.locateInData(&item)
	t.False(found)
	t.Equal(3, itemIndex)

	item = []byte("00020")
	itemIndex, found = t.node.locateInData(&item)
	t.False(found)
	t.Equal(3, itemIndex)

}

func (t *nodeTestSuite) TestLocateWithFullNode() {
	t.node.insert([]byte("00008"), 5)
	t.node.insert([]byte("00002"), 5)
	t.node.insert([]byte("00004"), 6)
	t.node.insert([]byte("00010"), 6)
	t.node.insert([]byte("00012"), 6)
	t.node.insert([]byte("00006"), 7)

	t.Equal("00002", string(t.node.data[0].data))
	t.Equal("00004", string(t.node.data[1].data))
	t.Equal("00006", string(t.node.data[2].data))
	t.Equal("00008", string(t.node.data[3].data))
	t.Equal("00010", string(t.node.data[4].data))
	t.Equal("00012", string(t.node.data[5].data))

	item := []byte("00001")
	itemIndex, found := t.node.locateInData(&item)
	t.False(found)
	t.Equal(0, itemIndex)

	item = []byte("00012")
	itemIndex, found = t.node.locateInData(&item)
	t.True(found)
	t.Equal(5, itemIndex)

	item = []byte("00014")
	itemIndex, found = t.node.locateInData(&item)
	t.False(found)
	t.Equal(6, itemIndex)
}

func (t *nodeTestSuite) TestInsertUntilFullOrdersElementHandlesDuplicate() {
	t.Equal(0, t.node.itemCount())

	t.node.insert([]byte("Item2"), 0)
	t.Equal(1, t.node.itemCount())

	t.node.insert([]byte("Item1"), 1)
	t.Equal(2, t.node.itemCount())

	t.node.insert([]byte("Item4"), 2)
	t.Equal(3, t.node.itemCount())

	t.node.insert([]byte("Item3"), 3)
	t.Equal(4, t.node.itemCount())

	// Assert duplicated item
	t.node.insert([]byte("Item4"), 4)
	t.Equal(4, t.node.itemCount())

	t.node.insert([]byte("Item5"), 5)
	t.Equal(5, t.node.itemCount())

	t.node.insert([]byte("Item6"), 6)
	t.Equal(6, t.node.itemCount())

	t.Equal("Item1", string(t.node.data[0].data))
	mapItem, _, err := t.node.getNextMapItem(0)
	t.Nil(err)
	t.Equal(int64(1), mapItem)

	t.Equal("Item2", string(t.node.data[1].data))
	mapItem, _, err = t.node.getNextMapItem(1)
	t.Nil(err)
	t.Equal(int64(0), mapItem)

	t.Equal("Item3", string(t.node.data[2].data))
	mapItem, _, err = t.node.getNextMapItem(2)
	t.Nil(err)
	t.Equal(int64(3), mapItem)

	t.Equal("Item4", string(t.node.data[3].data))
	mapItem, eof, err := t.node.getNextMapItem(3)
	t.Nil(err)
	t.False(eof)
	t.Equal(int64(2), mapItem)

	mapItem, eof, err = t.node.getNextMapItem(3)
	t.Nil(err)
	t.False(eof)
	t.Equal(int64(4), mapItem)

	mapItem, eof, err = t.node.getNextMapItem(3)
	t.Nil(err)
	t.True(eof)
	t.Equal(int64(0), mapItem)

	t.Equal("Item5", string(t.node.data[4].data))
	mapItem, _, err = t.node.getNextMapItem(4)
	t.Nil(err)
	t.Equal(int64(5), mapItem)

	t.Equal("Item6", string(t.node.data[5].data))
	mapItem, _, err = t.node.getNextMapItem(5)
	t.Nil(err)
	t.Equal(int64(6), mapItem)

	t.node.save(8)
}

func (t *nodeTestSuite) TestRootNodeSplit() {
	t.Equal(0, t.node.itemCount())

	for i := 1; i < 8; i++ {
		err := t.node.insert([]byte(fmt.Sprintf("A%04d", i)), 0)
		t.Nil(err)
	}

	err := t.node.load(8)
	t.Nil(err)

	// test left node
	t.Equal(3, t.node.itemCount())
	t.Equal("A0001", string(t.node.data[0].data))
	t.Equal("A0002", string(t.node.data[1].data))
	t.Equal("A0003", string(t.node.data[2].data))
	t.Equal("\x00\x00\x00\x00\x00", string(t.node.data[3].data))
	t.Equal("\x00\x00\x00\x00\x00", string(t.node.data[4].data))
	t.Equal("\x00\x00\x00\x00\x00", string(t.node.data[5].data))

	// Test new root node
	err = t.node.load(t.node.parentNodePtr)
	t.Nil(err)
	t.Equal(1, t.node.itemCount())
	t.Equal("A0004", string(t.node.data[0].data))

	// Test new right node
	err = t.node.load(t.node.data[0].children)
	t.Nil(err)
	t.Equal(3, t.node.itemCount())

	t.Equal("A0005", string(t.node.data[0].data))
	t.Equal("A0006", string(t.node.data[1].data))
	t.Equal("A0007", string(t.node.data[2].data))
	t.Equal("\x00\x00\x00\x00\x00", string(t.node.data[3].data))
	t.Equal("\x00\x00\x00\x00\x00", string(t.node.data[4].data))
	t.Equal("\x00\x00\x00\x00\x00", string(t.node.data[5].data))
}

func (t *nodeTestSuite) TestRegularNodeSplit() {
	t.Equal(0, t.node.itemCount())

	for i := 0; i < 7; i++ {
		t.node.insert([]byte("Item"+strconv.Itoa(i)), 0)
	}

	t.node.load(8)
	t.Equal(3, t.node.itemCount())
	t.Equal("Item0", string(t.node.data[0].data))
	t.Equal("Item1", string(t.node.data[1].data))
	t.Equal("Item2", string(t.node.data[2].data))
	t.Equal("\x00\x00\x00\x00\x00", string(t.node.data[3].data))
}
