// Package btree implement's an index
package btree

import (
	"fmt"
	filemanager "godb/pkg/file"
	"os"
	"strconv"
)

const (
	indexFileExt = ".idx"
	nodeSize     = 48
	// nodeSize = 6
)

// New creates a new balanced tree object
func New(indexName string, bufSize int, intIndex bool) (BTree, error) {
	if intIndex {
		bufSize = int64Length
	}
	t := &Tree{
		filer:     filemanager.New(),
		indexName: indexName,
		bufSize:   bufSize,
		intIndex:  intIndex,
	}

	err := t.init()
	if err != nil {
		return nil, err
	}
	return t, nil
}

// BTree is the interface of the methods
type BTree interface {
	Insert([]byte, int64) error
	First() (int64, *[]byte, error)
	Last() (int64, *[]byte, error)
	Search([]byte) (int64, *[]byte, bool, error)
	Next() (int64, *[]byte, bool, error)
	Prev() (int64, *[]byte, bool, error)
	Delete(int) bool
	Close() error
}

// Tree represents the B-tree as a whole.
type Tree struct {
	searchKey      *[]byte
	previousKey    *[]byte
	filer          filemanager.Filer
	indexName      string
	file           *os.File
	bufSize        int
	currentNode    *Node
	currentNodeIdx int
	parentNodePtr  int64
	intIndex       bool
}

// Insert inserts a key-value pair into the B-tree.
func (t *Tree) Insert(key []byte, value int64) error {
	sk := make([]byte, t.bufSize)
	copy(sk, key)
	t.searchKey = &sk

	rootNodePtr, eof, err := t.filer.ReadInt64(t.file, 0)
	if err != nil {
		return err
	}

	if eof {
		return fmt.Errorf("cannot get root node, file is corrupt")
	}

	node, _, _, err := t.recursiveSearch(rootNodePtr, &sk)
	if err != nil {
		return err
	}

	return node.insert(key, value)
}

// Close closes the Btree file
func (t *Tree) Close() error {
	return t.file.Close()
}

// Search searches an element node and reports if it was found, or not
func (t *Tree) Search(key []byte) (int64, *[]byte, bool, error) {
	sk := make([]byte, t.bufSize)
	copy(sk, key)

	ptr, eof, err := t.filer.ReadInt64(t.file, 0)
	if err != nil {
		return 0, nil, false, err
	}
	t.parentNodePtr = ptr

	if eof {
		return 0, nil, false, fmt.Errorf("search / cannot read root node pointer, corrupt index file")
	}

	node, idx, found, err := t.recursiveSearch(ptr, &sk)
	if err != nil {
		return 0, nil, false, err
	}

	t.currentNode = node
	t.currentNodeIdx = idx
	result, _, err := node.getNextMapItem(idx)

	return result, &node.data[idx].data, found, err
}

// First sets the index cursor to the first element
func (t *Tree) First() (int64, *[]byte, error) {
	ptr, eof, err := t.filer.ReadInt64(t.file, 0)
	if err != nil {
		return 0, nil, err
	}

	if eof {
		return 0, nil, fmt.Errorf("first / cannot read root node pointer, corrupt index file")
	}

	node, err := t.recursiveFirst(ptr)
	if err != nil {
		return 0, nil, err
	}

	t.currentNode = node
	t.currentNodeIdx = 0

	if node.data[0].isSet {
		result, _, err := node.getNextMapItem(0)
		return result, &node.data[0].data, err
	}

	return 0, nil, nil
}

func (t *Tree) recursiveFirst(ptr int64) (*Node, error) {
	sNode := t.getNode(0)
	err := sNode.load(ptr)
	if err != nil {
		return nil, err
	}

	if sNode.leftChild != 0 {
		return t.recursiveFirst(sNode.leftChild)
	}

	return sNode, nil
}

// Last places the index cursor to the last element
func (t *Tree) Last() (int64, *[]byte, error) {
	ptr, eof, err := t.filer.ReadInt64(t.file, 0)
	if err != nil {
		return 0, nil, err
	}

	if eof {
		return 0, nil, fmt.Errorf("last / cannot read root node pointer, corrupt index file")
	}

	node, err := t.recursiveLast(ptr)
	if err != nil {
		return 0, nil, err
	}

	t.currentNode = node
	t.currentNodeIdx = node.itemCount() - 1

	if t.currentNodeIdx >= 0 && node.data[t.currentNodeIdx].isSet {
		result, _, err := node.getNextMapItem(t.currentNodeIdx)
		return result, &node.data[t.currentNodeIdx].data, err
	}

	return 0, nil, nil
}

func (t *Tree) recursiveLast(ptr int64) (*Node, error) {
	sNode := t.getNode(0)
	err := sNode.load(ptr)
	if err != nil {
		return nil, err
	}

	// TODO: this may have to go to the caller, and pre fetching the root node, this may improve performace. Profile it!
	if sNode.isRoot() && sNode.itemCount() == 0 {
		return sNode, nil
	}

	lastItemIndex := sNode.itemCount() - 1
	if sNode.data[lastItemIndex].children != 0 {
		return t.recursiveLast(sNode.data[lastItemIndex].children)
	}

	return sNode, nil
}

// Next moves the index cursor to the next element, end returns the current value
func (t *Tree) Next() (int64, *[]byte, bool, error) {
	if t.currentNodeIdx == -1 {
		return 0, nil, true, nil // Why even getting into this condition?
	}
	t.previousKey = &t.currentNode.data[t.currentNodeIdx].data
	if t.currentNodeIdx >= 0 && t.currentNode.data[t.currentNodeIdx].isSet {
		item, eof, err := t.currentNode.getNextMapItem(t.currentNodeIdx)
		if err != nil {
			return 0, nil, false, err
		}

		if !eof {
			return item, &t.currentNode.data[t.currentNodeIdx].data, false, nil
		}
	}

	eof, err := t.recursiveNext()
	if err != nil {
		return 0, nil, false, err
	}

	if eof {
		return 0, nil, true, nil
	}

	return t.Next()
}

func (t *Tree) recursiveNext() (bool, error) {
	if t.currentNode.isLeaf() {
		t.currentNodeIdx++
		if t.currentNode.data[t.currentNodeIdx].isSet {
			return false, nil
		}

		if t.currentNode.isRoot() {
			// this is a root and leaf node at the same time, we reached at the end
			return true, nil
		}

		// at the end of the leaf node, to up
		t.currentNode.load(t.currentNode.parentNodePtr)
		idx, _ := t.currentNode.locateInData(t.previousKey)
		t.currentNodeIdx = idx

		if t.currentNode.data[idx].isSet {
			return false, nil
		}

		return t.recursiveNext()
	}

	// non leaf node
	if t.currentNodeIdx == -1 {
		if t.currentNode.leftChild != 0 {
			t.currentNode.load(t.currentNode.leftChild)
			t.currentNodeIdx = -1
			return t.recursiveNext()
		}
		t.currentNodeIdx = 0
		return false, nil
	}

	if !t.currentNode.data[t.currentNodeIdx].isSet {
		if t.currentNode.isRoot() {
			return true, nil
		}
		t.currentNode.load(t.currentNode.parentNodePtr)
		idx, _ := t.currentNode.locateInData(t.previousKey)
		t.currentNodeIdx = idx

		if t.currentNode.data[idx].isSet {
			return false, nil
		}

		return t.recursiveNext()
	}

	if t.currentNode.data[t.currentNodeIdx].children != 0 {
		t.currentNode.load(t.currentNode.data[t.currentNodeIdx].children)
		t.currentNodeIdx = -1
		return t.recursiveNext()
	}

	return true, nil
}

// Prev moves the index cursor to the previous element, returns current value
func (t *Tree) Prev() (int64, *[]byte, bool, error) {
	if t.currentNodeIdx == -1 {
		return 0, nil, true, nil
	}
	t.previousKey = &t.currentNode.data[t.currentNodeIdx].data
	if t.currentNodeIdx >= 0 && t.currentNode.data[t.currentNodeIdx].isSet {
		item, eof, err := t.currentNode.getNextMapItem(t.currentNodeIdx)

		if err != nil {
			return 0, nil, false, err
		}

		if !eof {
			return item, &t.currentNode.data[t.currentNodeIdx].data, false, nil
		}
	}

	eof, err := t.prevRecursive(false)
	if err != nil {
		return 0, nil, false, err
	}

	if eof {
		return 0, nil, true, nil
	}

	return t.Prev()
}

func (t *Tree) prevRecursive(isUp bool) (bool, error) {
	if t.currentNodeIdx == -1 && t.currentNode.isRoot() {
		return true, nil
	}

	t.currentNodeIdx = t.currentNode.findPreviousNodeByKey(t.previousKey)
	if t.currentNode.isLeaf() {
		if t.currentNodeIdx == -1 {
			t.currentNode.load(t.currentNode.parentNodePtr)
			idx := t.currentNode.findPreviousNodeByKey(t.previousKey)
			if idx < 0 {
				return t.prevRecursive(true)
			}
			t.currentNodeIdx = idx
		}

		return false, nil
	}

	// non leaf node
	if t.currentNodeIdx < 0 {
		if t.currentNode.leftChild != 0 && !isUp {
			t.currentNode.load(t.currentNode.leftChild)

			return t.prevRecursive(false)
		}

		// no left child probably go up?
		t.currentNode.load(t.currentNode.parentNodePtr)
		idx := t.currentNode.findPreviousNodeByKey(t.previousKey)
		if idx < 0 {
			return t.prevRecursive(true)
		}
		t.currentNodeIdx = idx
		return false, nil
	}

	if t.currentNode.data[t.currentNodeIdx].children != 0 {
		t.currentNode.load(t.currentNode.data[t.currentNodeIdx].children)
		idx := t.currentNode.findPreviousNodeByKey(t.previousKey)
		t.currentNodeIdx = idx

		return t.prevRecursive(false)
	}

	return true, nil
}

func (t *Tree) recursiveSearch(ptr int64, s *[]byte) (*Node, int, bool, error) {
	sNode := t.getNode(0)
	err := sNode.load(ptr)
	if err != nil {
		return nil, 0, false, err
	}
	i, f := sNode.locateInData(s)
	if f {
		return sNode, i, true, nil
	}

	if i == 0 {
		if sNode.leftChild != 0 {
			return t.recursiveSearch(sNode.leftChild, s)
		}

		// not found, reached the beginning
		return sNode, i, false, nil
	}

	if sNode.data[i-1].children != 0 {
		return t.recursiveSearch(sNode.data[i-1].children, s)
	}

	return sNode, i, false, nil
}

// Delete removes an element from the tree, not yet implemented
func (t *Tree) Delete(key int) bool {
	panic("Delete not yet implemented, cannot remove " + strconv.Itoa(key))
}

func (t *Tree) init() error {
	indexFileName := t.indexName + indexFileExt
	newFile, err := t.filer.CreateBlankFileIfNotExist(indexFileName)
	if err != nil {
		return err
	}

	file, err := t.filer.OpenReadWrite(indexFileName)
	if err != nil {
		return err
	}
	t.file = file

	if newFile {
		parentNode := t.getNode(0)
		err := parentNode.filer.WriteInt64(file, 0, int64(int64Length))
		if err != nil {
			return err
		}
		return parentNode.save(int64(int64Length))
	}

	return nil
}

func (t *Tree) getNode(parentNodePtr int64) *Node {
	if t.intIndex {
		return NewInt64Node(t.file, t.filer, nodeSize, parentNodePtr)
	}
	return NewNode(t.file, t.filer, nodeSize, t.bufSize, parentNodePtr)
}
