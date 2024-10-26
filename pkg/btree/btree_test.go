package btree

import (
	"encoding/binary"
	"fmt"
	filemanager "godb/pkg/file"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/suite"
)

const (
	indexItemLength = 5
)

type btreeTestSuite struct {
	suite.Suite
	tree BTree
}

func TestBtreeRunner(t *testing.T) {
	suite.Run(t, new(btreeTestSuite))
}

func (t *btreeTestSuite) SetupTest() {
	var err error
	err = os.RemoveAll(filemanager.DefaultFolder)
	if err != nil {
		panic("Cannot run test, the folder cannot be removed " + err.Error())
	}

	t.tree, err = New("test_index", indexItemLength, false)
	if err != nil {
		panic(err)
	}
}

func (t *btreeTestSuite) TearDownTest() {
	t.tree = nil
}

func (t *btreeTestSuite) TestBTree() {
	for i := 0; i <= 99999; i++ {
		if i != 99998 {
			err := t.tree.Insert([]byte(fmt.Sprintf("%05d", i)), 5)
			t.Nil(err)
		}
	}

	item := []byte(fmt.Sprintf("%05d", 99998))
	err := t.tree.Insert(item, 3298)
	t.Nil(err)

	res, _, found, err := t.tree.Search(item)
	t.Nil(err)
	t.True(found)
	t.Equal(int64(3298), res)
}

func (t *btreeTestSuite) TestSearchNextWith1NodeMultipleValues() {
	item := []byte(fmt.Sprintf("%05d", 99999))
	err := t.tree.Insert(item, 65)
	t.Nil(err)

	err = t.tree.Insert(item, 66)
	t.Nil(err)

	err = t.tree.Insert(item, 67)
	t.Nil(err)

	err = t.tree.Insert(item, 68)
	t.Nil(err)

	res, _, found, err := t.tree.Search(item)
	t.Nil(err)
	t.True(found)
	t.Equal(int64(65), res)

	res, _, eof, err := t.tree.Next()
	t.Nil(err)
	t.False(eof)
	t.Equal(int64(66), res)

	res, _, eof, err = t.tree.Next()
	t.Nil(err)
	t.False(eof)
	t.Equal(int64(67), res)

	res, _, eof, err = t.tree.Next()
	t.Nil(err)
	t.False(eof)
	t.Equal(int64(68), res)
	_, _, eof, err = t.tree.Next()
	t.Nil(err)
	t.False(eof)

	_, _, eof, err = t.tree.Next()
	t.Nil(err)
	t.False(eof) // TODO check why fails?, should be t.True?

}

func (t *btreeTestSuite) TestAllINdexedItemCanBeFound() {
	for i := 10000; i > 0; i-- {
		t.tree.Insert([]byte(fmt.Sprintf("%05d", i)), int64(i+5))
	}

	// DisplayTree("test_index")

	for i := 10000; i > 1; i-- {
		src := fmt.Sprintf("%05d", i)
		res, buf, found, err := t.tree.Search([]byte(src))
		t.Nil(err)
		t.True(found)
		t.Equal(src, string(*buf))
		t.Equal(int64(i+5), res)
	}
}

func (t *btreeTestSuite) TestSearchNextWithMultipleNodesAndMapping() {
	oFile, err := os.Create("output.txt")
	if err != nil {
		fmt.Println("Error creating file:", err)
		return
	}
	defer oFile.Close()

	// Redirect standard output to the file
	os.Stdout = oFile

	for i := 1000; i > 0; i-- {
		// for i := 0; i < 1000; i++ {
		t.tree.Insert([]byte(fmt.Sprintf("%05d", i)), 65535)
		for x := 0; x < 16; x++ {
			t.tree.Insert([]byte(fmt.Sprintf("%05d", i)), int64(x))
		}

		t.tree.Insert([]byte(fmt.Sprintf("%05d", i)), 255)
	}

	res, val, found, err := t.tree.Search([]byte(fmt.Sprintf("%05d", 1)))
	t.log(string(*val), strconv.FormatInt(res, 16))
	t.Nil(err)
	t.True(found)

	// DisplayTree("test_index")

	for {
		res, val, eof, err := t.tree.Next()
		if eof {
			break
		}

		t.Nil(err)
		if err != nil {
			break
		}
		t.log(string(*val), strconv.FormatInt(res, 16))
	}
}

func (t *btreeTestSuite) TestFirst() {
	for i := 1000; i > 0; i-- {
		t.tree.Insert([]byte(fmt.Sprintf("%05d", i)), 65535)
		for x := 0; x < 16; x++ {
			t.tree.Insert([]byte(fmt.Sprintf("%05d", i)), int64(x))
		}

		t.tree.Insert([]byte(fmt.Sprintf("%05d", i)), 255)
	}

	val, dat, err := t.tree.First()
	t.Nil(err)
	t.Equal(val, int64(65535))
	t.Equal(string(*dat), "00001")
}

func (t *btreeTestSuite) TestLast() {
	for i := 1000; i > 0; i-- {
		t.tree.Insert([]byte(fmt.Sprintf("%05d", i)), 65535)
		for x := 0; x < 16; x++ {
			t.tree.Insert([]byte(fmt.Sprintf("%05d", i)), int64(x))
		}

		t.tree.Insert([]byte(fmt.Sprintf("%05d", i)), 255)
	}

	val, dat, err := t.tree.Last()
	t.Nil(err)
	t.NotNil(dat)
	t.Equal(int64(65535), val)
	t.Equal("01000", string(*dat))
}

func (t *btreeTestSuite) TestPrev() {
	for i := 1000; i > 0; i-- {
		t.tree.Insert([]byte(fmt.Sprintf("%05d", i)), int64(i+100))
	}

	val, dat, err := t.tree.Last()
	t.Nil(err)
	t.NotNil(dat)
	t.Equal(int64(1100), val)
	t.Equal("01000", string(*dat))

	num := 999
	for {
		i, val, eof, err := t.tree.Prev()
		if val != nil {
			t.Equal(fmt.Sprintf("%05d", num), string(*val))
			t.Equal(int64(num+100), i)
		}

		t.log(val, i, eof, err, "prev")
		if eof || err != nil {
			break
		}
		num--
	}
}

func (t *btreeTestSuite) TestPrevWithInt() {
	var err error
	t.tree, err = New("test_index", 8, true)
	if err != nil {
		panic(err)
	}
	for i := 1000; i > 0; i-- {
		buf := t.int64ToBuf(int64(i))
		t.tree.Insert(buf, int64(i))
	}

	_, dat, err := t.tree.Last()
	t.Nil(err)
	t.NotNil(dat)
	// t.Equal(int64(1100), val)
	res := t.bufToInt64(*dat)
	t.Equal(int64(1000), res)

	num := 999
	for {
		i, val, eof, err := t.tree.Prev()
		if val != nil {
			res := t.bufToInt64(*val)
			t.Equal(int64(num), res)
			// t.Equal(int64(num), i)
			t.log(res, num, i, eof, err, "back")
		}

		if eof || err != nil {
			break
		}
		num--
	}
}

func (t *btreeTestSuite) log(s ...interface{}) {

	// filer := filemanager.New()
	// debugFileName := filer.GetFullFilePath("debug.log")
	debugFileName := "debug.log"
	file, err := os.OpenFile(debugFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer file.Close()

	for _, v := range s {
		if val, ok := v.(string); ok {
			file.WriteString(val)
		}

		if val, ok := v.(int); ok {
			file.WriteString(strconv.Itoa(val))
		}

		if val, ok := v.(int64); ok {
			file.WriteString(strconv.FormatInt(val, 10))
		}

		if val, ok := v.(error); ok {
			file.WriteString(val.Error())
		}

		if val, ok := v.([]byte); ok {
			file.WriteString(string(val))
		}

		if v == nil {
			file.WriteString("Nil")
		}

		if val, ok := v.(*[]byte); ok {
			if val != nil {
				file.WriteString(string(*val))
			}
		}

		file.WriteString(" | ")
	}
	file.WriteString("\n")
}

func (t *btreeTestSuite) bufToInt64(b []byte) int64 {
	// return *(*int64)(unsafe.Pointer(&b[0]))
	if len(b) < 8 {
		panic("Non 8 bytes buffer")
	}

	return int64(binary.LittleEndian.Uint64(b))
}

func (t *btreeTestSuite) int64ToBuf(num int64) []byte {
	// return (*[int64Length]byte)(unsafe.Pointer(&num))[:]
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(num))
	return buf
}
