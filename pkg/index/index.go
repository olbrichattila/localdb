// Package index will create and maintain an index file
package index

import (
	"encoding/binary"
	"fmt"
	filemanager "godb/pkg/file"
	"io"
	"os"
)

const (
	indexFileExt = ".idx"
	equal        = 0
	smaller      = -1
	bigger       = 1
)

var nullPointers = make([]byte, filemanager.Int64Length*3)

// New indexer
func New(indexName string, bufSize int) (Indexer, error) {
	i := &ind{
		filer:     filemanager.New(),
		indexName: indexName,
		bufSize:   bufSize,
	}

	err := i.init()

	return i, err
}

// Indexer interface will represent the abstracted indexing logic
type Indexer interface {
	Insert(int64, []byte) error
	Seek([]byte) ([]int64, error)
}

type ind struct {
	filer             filemanager.Filer
	file              *os.File
	fileName          string
	indexName         string
	bufSize           int
	mappingPointer    int64
	smallestNodePtr   int64
	largestNodePtr    int64
	currentNodePtr    int64
	currentMappingPtr int64
	smallestNodeValue []byte
	largestNodeValue  []byte
}

func (i *ind) init() error {
	i.fileName = i.filer.GetFullFilePath(i.indexName + indexFileExt)
	err := i.createIndexFileIfNotExists()
	if err != nil {
		return err
	}

	i.smallestNodeValue = make([]byte, i.bufSize)
	i.largestNodeValue = make([]byte, i.bufSize)

	return i.openIndexFile()
}

func (i *ind) createIndexFileIfNotExists() error {
	_, err := i.filer.CreateBlankFileIfNotExist(i.indexName + indexFileExt)
	return err
}

func (i *ind) openIndexFile() error {
	file, err := os.OpenFile(i.fileName, os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	i.file = file

	return nil
}

func (i *ind) Insert(mappingPointer int64, data []byte) error {
	i.mappingPointer = mappingPointer
	bufSize := len(data)
	if bufSize != i.bufSize {
		return fmt.Errorf("index and buffer size mismatch %d/%d", bufSize, i.bufSize)
	}

	err := i.insertNode(0, &data)

	return err
}

func (i *ind) Seek(data []byte) ([]int64, error) {
	mapList, nodePointer, err := i.seekNode(0, &data)
	i.currentNodePtr = nodePointer
	i.currentMappingPtr = 0
	return mapList, err
}

func (i *ind) seekNode(nodePointer int64, data *[]byte) ([]int64, int64, error) {
	buf, leftNode, rightNode, mappingNode, err := i.readByNodePointer(nodePointer)
	if err != nil {
		return nil, nodePointer, err
	}

	if buf == nil {
		// empty index
		return nil, nodePointer, nil
	}

	compared := i.compareAsStr(buf, data)
	if compared == equal {
		mapList, err := i.getMapValues(mappingNode)
		return mapList, nodePointer, err
	}

	if compared == smaller {
		if leftNode != 0 {
			return i.seekNode(leftNode, data)
		}
		// Not found on left node
		return nil, nodePointer, nil
	}

	if compared == bigger {
		if rightNode != 0 {
			return i.seekNode(rightNode, data)
		}
		// Not found on right node
		return nil, nodePointer, nil
	}

	// Not found on any node
	return nil, nodePointer, nil
}

func (i *ind) insertNode(nodePointer int64, data *[]byte) error {
	// TODO if I insert a node, before I update the left or right node pointer need to check if there is already
	// an attached left or right node, and need to move this node id to the new node, otherwise will break the tree.
	buf, leftNode, rightNode, mappingNode, err := i.readByNodePointer(nodePointer)
	if err != nil {
		return err
	}

	leftNodeOffset := nodePointer + int64(i.bufSize)
	rightNodeOffset := nodePointer + int64(i.bufSize) + filemanager.Int64Length

	if buf == nil {
		// Add root node
		_, err := i.writeNewNode(data)
		if err != nil {
			return err
		}

		return nil
	}

	compared := i.compareAsStr(buf, data)
	if compared == equal {
		// TODO already found, add new mapping ony (unique logic will come here)
		_, err := i.mapValue(mappingNode)
		if err != nil {
			return err
		}

		return nil
	}

	if compared == smaller {
		if leftNode != 0 {
			return i.insertNode(leftNode, data)
		}
		// Add as left node
		offset, err := i.writeNewNode(data)
		if err != nil {
			return err
		}

		return i.writeInt64(offset, leftNodeOffset)
	}

	if compared == bigger {
		if rightNode != 0 {
			return i.insertNode(rightNode, data)
		}
		// Add as right node
		offset, err := i.writeNewNode(data)
		if err != nil {
			return err
		}

		return i.writeInt64(offset, rightNodeOffset)
	}

	return nil
}

func (i *ind) compareAsStr(c1, c2 *[]byte) int {
	s1 := string(*c1)
	s2 := string(*c2)

	if s1 == s2 {
		return equal
	}

	if s1 > s2 {
		return smaller
	}

	return bigger
}

func (i *ind) readByNodePointer(nodePointer int64) (*[]byte, int64, int64, int64, error) {
	// data size + leftNodePointer + rightNodePointer + mappingNodePointer
	readLen := i.bufSize + filemanager.Int64Length + filemanager.Int64Length + filemanager.Int64Length
	buf, eof, err := i.filer.ReadBytes(i.file, nodePointer, readLen)
	if err != nil {
		return nil, 0, 0, 0, err
	}

	if eof && nodePointer == 0 {
		// this is a non existent root node
		return nil, 0, 0, 0, nil
	}

	if eof && nodePointer > 0 {
		return nil, 0, 0, 0, fmt.Errorf("corrupt index file %s", i.fileName)
	}

	res := buf[:i.bufSize]
	leftNode := buf[i.bufSize : i.bufSize+filemanager.Int64Length]
	rightNode := buf[i.bufSize+filemanager.Int64Length : i.bufSize+filemanager.Int64Length*2]
	mappingNode := buf[i.bufSize+filemanager.Int64Length*2 : i.bufSize+filemanager.Int64Length*3]

	return &res,
		int64(binary.LittleEndian.Uint64(leftNode)),
		int64(binary.LittleEndian.Uint64(rightNode)),
		int64(binary.LittleEndian.Uint64(mappingNode)),
		nil
}

func (i *ind) writeNewNode(data *[]byte) (int64, error) {
	offset, err := i.file.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}
	buf := append(*data, nullPointers...)

	err = i.filer.WriteBytes(i.file, offset, buf)
	if err != nil {
		return offset, err
	}

	mapOffset, err := i.mapValue(0)
	if err != nil {
		return offset, err
	}

	err = i.writeInt64(mapOffset, offset+int64(i.bufSize)+filemanager.Int64Length*2)
	if err != nil {
		return offset, err
	}

	i.updateIndexStats(offset, data)

	return offset, err
}

func (i *ind) writeInt64(num, offset int64) error {
	buf := make([]byte, filemanager.Int64Length)
	binary.LittleEndian.PutUint64(buf, uint64(num))
	return i.filer.WriteBytes(i.file, offset, buf)
}

func (i *ind) mapValue(mappingNode int64) (int64, error) {
	var previousMappingNode int64
	for {
		if mappingNode == 0 {
			offset, err := i.file.Seek(0, io.SeekEnd)
			if err != nil {
				return 0, err
			}

			valueBuf := make([]byte, filemanager.Int64Length)
			binary.LittleEndian.PutUint64(valueBuf, uint64(i.mappingPointer))

			nextPointerBuf := make([]byte, filemanager.Int64Length)
			valueBuf = append(valueBuf, nextPointerBuf...)

			i.filer.WriteBytes(i.file, offset, valueBuf)

			if previousMappingNode != 0 {
				// Join new value to the mapping list
				i.writeInt64(offset, previousMappingNode+filemanager.Int64Length)
			}

			return offset, nil
		}
		previousMappingNode = mappingNode

		buf, eof, err := i.filer.ReadBytes(i.file, mappingNode, filemanager.Int64Length*2)
		if err != nil {
			return 0, err
		}

		mappingNode = int64(binary.LittleEndian.Uint64(buf[filemanager.Int64Length : filemanager.Int64Length*2]))

		if eof {
			return 0, fmt.Errorf("invalid index file %s, mapping buffer write error", i.fileName)
		}
	}
}

func (i *ind) getMapValues(mappingNode int64) ([]int64, error) {
	res := make([]int64, 0)
	for {
		if mappingNode == 0 {
			break
		}

		buf, eof, err := i.filer.ReadBytes(i.file, mappingNode, filemanager.Int64Length*2)
		if err != nil {
			return res, err
		}

		res = append(res, int64(binary.LittleEndian.Uint64(buf[:filemanager.Int64Length])))
		mappingNode = int64(binary.LittleEndian.Uint64(buf[filemanager.Int64Length : filemanager.Int64Length*2]))

		if eof {
			return res, fmt.Errorf("invalid index file %s, mapping buffer read error", i.fileName)
		}
	}

	return res, nil
}

func (i *ind) updateIndexStats(ptr int64, data *[]byte) {
	// Todo this should be preserved and readed back on index open
	comp := i.compareAsStr(data, &i.smallestNodeValue)

	if comp == smaller {
		i.smallestNodePtr = ptr
	}

	comp = i.compareAsStr(data, &i.largestNodeValue)

	if comp == bigger {
		i.largestNodePtr = ptr
	}
}
