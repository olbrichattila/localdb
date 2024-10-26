package index

import (
	filemanager "godb/pkg/file"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"golang.org/x/exp/rand"
)

type indexerTestSuite struct {
	suite.Suite
	indexer Indexer
}

func TestExampleRunner(t *testing.T) {
	suite.Run(t, new(indexerTestSuite))
}

func (t *indexerTestSuite) SetupTest() {
	err := os.RemoveAll(filemanager.DefaultFolder)
	if err != nil {
		panic("Cannot run test, the folder cannot be removed " + err.Error())
	}

	t.indexer, err = New("test_index", 5)
	if err != nil {
		panic(err.Error())
	}

}

func (t *indexerTestSuite) TearDownTest() {
	t.indexer = nil
}

func (t *indexerTestSuite) TestIndex() {
	// Assert error out if buf size incorrect
	err := t.indexer.Insert(int64(255), []byte("123"))
	t.Error(err)
	t.Equal("index and buffer size mismatch 3/5", err.Error())

	err = t.indexer.Insert(int64(255), []byte("12345"))
	t.Nil(err)

	err = t.indexer.Insert(int64(685), []byte("54321"))
	t.Nil(err)

	res, err := t.indexer.Seek([]byte("54321"))
	t.Nil(err)
	t.Len(res, 1)
	t.Equal(int64(685), res[0])
}

func (t *indexerTestSuite) TestIterateAndSeek() {

	for i := 0; i < 1000; i++ {
		str := generateRandomString(5)
		t.indexer.Insert(int64(121454), []byte(str))
	}

	err := t.indexer.Insert(int64(578488), []byte("BLABL"))
	t.Nil(err)

	for i := 0; i < 30; i++ {
		str := generateRandomString(5)
		t.indexer.Insert(int64(121454), []byte(str))
	}

	res, err := t.indexer.Seek([]byte("BLABL"))
	t.Nil(err)
	t.Len(res, 1)
	t.Equal(int64(578488), res[0])
}

func generateRandomString(length int) string {
	charset := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	seededRand := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}
