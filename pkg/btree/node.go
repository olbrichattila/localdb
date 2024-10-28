package btree

import (
	"encoding/binary"
	"fmt"
	filemanager "godb/pkg/file"
	"io"
	"os"
	"strings"
)

const (
	int64Length = 8
	boolLength  = 1

	isLess    = -1
	isEqual   = 0
	isGreater = 1
)

// NewNode creates a new BTree node instance
func NewNode(file *os.File, filer filemanager.Filer, elementSize, bufSize int, parentNodePtr int64) *Node {
	return newTypedNode(file, filer, elementSize, bufSize, parentNodePtr, false)
}

// NewInt64Node creates a new 64 bit integer index node instance
func NewInt64Node(file *os.File, filer filemanager.Filer, elementSize int, parentNodePtr int64) *Node {
	return newTypedNode(file, filer, elementSize, int64Length, parentNodePtr, true)
}

func newTypedNode(file *os.File, filer filemanager.Filer, elementSize, bufSize int, parentNodePtr int64, isIntNode bool) *Node {
	// Adding extra element, if node is full still can add element order in then we will split it before saving

	data := make([]DataItem, elementSize+1)
	dataElementSize := bufSize + int64Length*2 + boolLength
	bfLen := int64Length*2 + dataElementSize*(elementSize+1)
	return &Node{
		maxElementCount: elementSize,
		minElementCount: elementSize / 2,
		data:            data,
		file:            file,
		filer:           filer,
		bufSize:         bufSize,
		parentNodePtr:   parentNodePtr,
		bfLen:           bfLen,
		isIntNode:       isIntNode,
	}
}

// Node represents a node in the B-tree.
type Node struct {
	maxElementCount int
	minElementCount int
	parentNodePtr   int64
	data            []DataItem
	currentPtr      int64
	leftChild       int64
	filer           filemanager.Filer
	file            *os.File
	bufSize         int
	bfLen           int
	itemIndex       int
	isIntNode       bool
}

// DataItem is a data with it's right node pointer
type DataItem struct {
	data         []byte
	children     int64
	mapPtr       int64
	fetchMmapPtr int64
	isSet        bool
}

func (n *Node) add(parentNodePtr int64) *Node {
	data := make([]DataItem, n.maxElementCount+1)
	return &Node{
		maxElementCount: n.maxElementCount,
		minElementCount: n.minElementCount,
		data:            data,
		file:            n.file,
		filer:           n.filer,
		bufSize:         n.bufSize,
		parentNodePtr:   parentNodePtr,
		bfLen:           n.bfLen,
		isIntNode:       n.isIntNode,
	}
}

func (n *Node) saveAsNew() (int64, error) {
	offset, err := n.file.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}

	err = n.save(offset)
	if err != nil {
		return 0, err
	}

	return offset, nil
}

func (n *Node) save(ptr int64) error {
	n.currentPtr = ptr
	buf := make([]byte, n.bfLen)

	index := n.copyOffsetInt64(&buf, n.parentNodePtr, 0)
	index = n.copyOffsetInt64(&buf, n.leftChild, index)

	dataLen := len(n.data)
	for i1 := 0; i1 <= n.maxElementCount; i1++ {
		if i1 >= dataLen {
			continue
		}
		index = n.copyOffset(&buf, &n.data[i1].data, index)
		index = n.copyOffsetInt64(&buf, n.data[i1].children, index)
		index = n.copyOffsetInt64(&buf, n.data[i1].mapPtr, index)
		index = n.copyOffsetBool(&buf, n.data[i1].isSet, index)
	}

	n.filer.WriteBytes(n.file, ptr, buf)

	// n.saveNodeDebug()

	return nil
}

func (n *Node) update() error {
	return n.save(n.currentPtr)
}

func (n *Node) reload() error {
	return n.load(n.currentPtr)
}

func (n *Node) load(ptr int64) error {
	index := 0
	n.currentPtr = ptr
	buf, eof, err := n.filer.ReadBytes(n.file, ptr, n.bfLen)
	if err != nil {
		return err
	}

	if eof {
		return fmt.Errorf("trying to read after end of the file, node load")
	}

	n.parentNodePtr, index = n.readOffsetAsInt64(&buf, 0)
	n.leftChild, index = n.readOffsetAsInt64(&buf, index)

	n.data = make([]DataItem, n.maxElementCount+1)
	for i := 0; i <= n.maxElementCount; i++ {
		n.data[i].data, index = n.readOffset(&buf, index, n.bufSize)
		n.data[i].children, index = n.readOffsetAsInt64(&buf, index)

		n.data[i].mapPtr, index = n.readOffsetAsInt64(&buf, index)
		n.data[i].fetchMmapPtr = n.data[i].mapPtr

		n.data[i].isSet, index = n.readOffsetAsBool(&buf, index)
	}

	// todo comment out
	// n.saveNodeDebug()

	return nil
}

func (n *Node) insert(item []byte, mapValue int64) error {
	insertItem := make([]byte, n.bufSize)
	copy(insertItem, item)

	return n.insertWithPointer(insertItem, 0, mapValue, nil)
}

func (n *Node) insertWithPointer(item []byte, childPtr, mapValue int64, movedFromNode *Node) error {
	n.reload()
	var mapPtr int64
	var err error
	pos, found := n.locateInData(&item)
	if found {
		// Todo review this logic, we also inserting in Btree? It may never goes here?

		if n.data[pos].mapPtr == 0 {
			return fmt.Errorf("missing mapping node, corrupt index")
		}

		if movedFromNode == nil {
			return n.insertToMap(n.data[pos].mapPtr, mapValue)
		}

		return nil
	}

	dataLen := len(n.data)
	for i := dataLen - 1; i > pos; i-- {
		n.data[i] = n.data[i-1]
	}

	if movedFromNode != nil {
		// Update parent rather then whole node parent update
		err := n.filer.WriteInt64(n.file, childPtr, n.currentPtr)
		if err != nil {
			return err
		}
		mapPtr = mapValue
	} else {
		mapPtr, err = n.addNewMap(mapValue)
		if err != nil {
			return err
		}
	}

	n.data[pos].data = item
	n.data[pos].children = childPtr
	n.data[pos].mapPtr = mapPtr
	n.data[pos].fetchMmapPtr = mapPtr
	n.data[pos].isSet = true

	err = n.update()
	if err != nil {
		return err
	}

	if n.needToSplit() {
		err := n.split()
		if err != nil {
			return err
		}
	}

	return nil
}

// func (n *Node) remove(ptr int64) {
// 	// Remove element from node if it does not go under minimum
// 	panic("Remove element not implemented yet")
// }

func (n *Node) itemCount() int {
	cnt := 0
	for _, d := range n.data {
		if d.isSet {
			cnt++
		}
	}

	return cnt
}

func (n *Node) getNextMapItem(itemInd int) (int64, bool, error) {
	if n.data[itemInd].fetchMmapPtr == 0 {
		n.data[itemInd].fetchMmapPtr = n.data[itemInd].mapPtr

		return 0, true, nil
	}

	mapValue, nextPtr, err := n.getMapItem(n.data[itemInd].fetchMmapPtr)
	n.data[itemInd].fetchMmapPtr = nextPtr

	return mapValue, false, err
}

func (n *Node) needToSplit() bool {
	return n.itemCount() == n.maxElementCount+1
}

// func (n *Node) needToMerge() bool {
// 	return n.itemCount() < n.minElementCount && !n.isRoot()
// }

func (n *Node) isLeaf() bool {
	return n.data[0].isSet && n.data[0].children == 0
}

func (n *Node) isRoot() bool {
	return n.parentNodePtr == 0
}

func (n *Node) split() error {
	if n.isRoot() {
		return n.splitRootNode()
	}

	return n.splitRegularNode()
}

func (n *Node) splitRootNode() error {
	newRootNode := n.add(0)
	newRootNodePtr, err := newRootNode.saveAsNew()
	if err != nil {
		return err
	}
	err = newRootNode.setRoot()
	if err != nil {
		return err
	}

	newRightNode := n.add(newRootNodePtr)
	n.parentNodePtr = newRootNodePtr

	newRootNode.leftChild = n.currentPtr
	newRootNode.data[0] = n.data[n.minElementCount]

	for i := n.minElementCount + 1; i <= n.maxElementCount; i++ {
		newRightNode.data[i-n.minElementCount-1] = n.data[i]
		n.data[i].data = make([]byte, n.bufSize)
		n.data[i].isSet = false
		n.data[i].children = 0
		n.data[i].mapPtr = 0
		n.data[i].fetchMmapPtr = 0
	}

	newRightNode.leftChild = n.data[n.minElementCount].children
	newRightNodePtr, err := newRightNode.saveAsNew()
	if err != nil {
		return err
	}

	newRootNode.data[0].children = newRightNodePtr
	newRightNode.parentNodePtr = newRootNode.currentPtr

	n.data[n.minElementCount].data = make([]byte, n.bufSize)
	n.data[n.minElementCount].isSet = false
	n.data[n.minElementCount].children = 0
	n.data[n.minElementCount].mapPtr = 0
	n.data[n.minElementCount].fetchMmapPtr = 0

	newRootNode.update()
	newRightNode.update()
	n.update()

	newRootNode.updateAllChildParentPointer()
	newRightNode.updateAllChildParentPointer()
	// n.updateAllChildParentPointer() // This probably stays intact, this is the new left node which was not moved

	return nil
}

func (n *Node) splitRegularNode() error {
	n.reload()
	middleElement := n.data[n.minElementCount]

	// Create a new right node
	rightNode := n.add(n.parentNodePtr)

	// Move elements to the right node
	rightNode.leftChild = middleElement.children

	for i := n.minElementCount + 1; i <= n.maxElementCount; i++ {
		rightNode.data[i-n.minElementCount-1] = n.data[i]

		n.data[i].children = 0
		n.data[i].isSet = false
		n.data[i].mapPtr = 0
		n.data[i].fetchMmapPtr = 0
		n.data[i].data = make([]byte, n.bufSize)
	}

	// Save the new right node and get its offset
	rightNodeOffset, err := rightNode.saveAsNew()
	if err != nil {
		return err
	}

	// Clear the middle element in the current node
	n.data[n.minElementCount].children = 0
	n.data[n.minElementCount].isSet = false
	n.data[n.minElementCount].mapPtr = 0
	n.data[n.minElementCount].fetchMmapPtr = 0
	n.data[n.minElementCount].data = make([]byte, n.bufSize)

	// Insert middle key into parent and update parent’s child pointers
	parentNode := n.add(0)
	parentNode.load(n.parentNodePtr) // The parent pointer loads back from the node to the file

	rightNode.updateAllChildParentPointer()
	// n.updateAllChildParentPointer() // This probably stays intact
	// parentNode.updateAllChildParentPointer() // This is rather handled in the insert with pointer, so save some file operations

	err = parentNode.insertWithPointer(middleElement.data, rightNodeOffset, middleElement.mapPtr, n)
	if err != nil {
		return err
	}

	return n.update()
}

func (n *Node) updateAllChildParentPointer() error {
	if n.leftChild != 0 {
		err := n.filer.WriteInt64(n.file, n.leftChild, n.currentPtr)
		if err != nil {
			return err
		}
	}

	for _, dat := range n.data {
		if dat.isSet && dat.children != 0 {
			err := n.filer.WriteInt64(n.file, dat.children, n.currentPtr)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (n *Node) locateInData(buf *[]byte) (int, bool) {
	for i, dat := range n.data {
		if !dat.isSet {
			return i, false
		}

		result := n.bytesCompare(*buf, dat.data)
		if result == isEqual {
			return i, true
		}

		if result == isLess {
			return i, false
		}
	}

	return n.maxElementCount, false
}

func (n *Node) findPreviousNodeByKey(buf *[]byte) int {
	for i := len(n.data) - 1; i >= 0; i-- {
		if !n.data[i].isSet {
			continue
		}

		result := n.bytesCompare(n.data[i].data, *buf)
		if result == isLess {
			return i
		}
	}

	return -1
}

func (n *Node) bufToInt64(b []byte) int64 {
	// return *(*int64)(unsafe.Pointer(&b[0]))
	if len(b) < 8 {
		panic("Non 8 bytes buffer")
	}

	return int64(binary.LittleEndian.Uint64(b))
}

func (n *Node) int64ToBuf(num int64) []byte {
	// return (*[int64Length]byte)(unsafe.Pointer(&num))[:]
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(num))
	return buf
}

func (n *Node) copyOffset(to, from *[]byte, toIndex int) int {
	toSlice := *to
	i := toIndex
	for _, fromByte := range *from {
		toSlice[i] = fromByte
		i++
	}

	return i
}

func (n *Node) copyOffsetInt64(to *[]byte, val int64, toIndex int) int {
	buf := n.int64ToBuf(val)
	return n.copyOffset(to, &buf, toIndex)
}

func (n *Node) copyOffsetBool(to *[]byte, val bool, toIndex int) int {
	isSetAsByte := []byte{0}
	if val {
		isSetAsByte[0] = 1
	}

	return n.copyOffset(to, &isSetAsByte, toIndex)
}

func (n *Node) readOffset(from *[]byte, fromIndex, length int) ([]byte, int) {
	result := make([]byte, length)
	f := *from
	for i := 0; i < length; i++ {
		result[i] = f[fromIndex]
		fromIndex++
	}

	return result, fromIndex
}

func (n *Node) readOffsetAsInt64(from *[]byte, fromIndex int) (int64, int) {
	buf, index := n.readOffset(from, fromIndex, 8)

	return n.bufToInt64(buf), index
}

func (n *Node) readOffsetAsBool(from *[]byte, fromIndex int) (bool, int) {
	isSetAsByte, index := n.readOffset(from, fromIndex, 1)
	return isSetAsByte[0] == 1, index
}

func (n *Node) bytesCompare(buf1, buf2 []byte) int {
	if !n.isIntNode {
		return n.stringCompare(&buf1, &buf2)
		// lets try null terminated string compare
		// return bytes.Compare(buf1, buf2)
	}

	if len(buf1) == 0 {
		return isLess
	}

	n1 := n.bufToInt64(buf1)
	n2 := n.bufToInt64(buf2)
	if n1 == n2 {
		return isEqual
	}

	if n1 < n2 {
		return isLess
	}

	return isGreater
}
func (n *Node) stringCompare(buf1, buf2 *[]byte) int {
	s1 := n.bufToStr(buf1)
	s2 := n.bufToStr(buf2)

	if s1 == s2 {
		return isEqual
	}

	if s1 < s2 {
		return isLess
	}

	return isGreater
}

func (n *Node) bufToStr(buf *[]byte) string {
	b := &strings.Builder{}
	for _, v := range *buf {
		if v == 0 {
			break
		}
		b.WriteByte(v)
	}

	return b.String()

}

func (n *Node) setRoot() error {
	return n.filer.WriteInt64(n.file, 0, n.currentPtr)
}
