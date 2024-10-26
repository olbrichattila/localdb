package btree

import "fmt"

func (n *Node) addNewMap(val int64) (int64, error) {
	buf := n.int64ToBuf(val)
	nullPointer := make([]byte, int64Length)
	buf = append(buf, nullPointer...)

	return n.filer.AppendBytes(n.file, buf)
}

func (n *Node) insertToMap(ptr, val int64) error {
	for {
		buf, eof, err := n.filer.ReadBytes(n.file, ptr, int64Length*2)
		if err != nil {
			return err
		}

		if eof {
			return fmt.Errorf("insert into map file operation returned eof, corrupt index file")
		}

		currentVal := n.bufToInt64(buf[:int64Length])
		if currentVal == val {
			// already exists, nothing to do
			return nil
		}

		nextElementPtr := n.bufToInt64(buf[int64Length : int64Length+int64Length])
		if nextElementPtr == 0 {
			// no new element add it
			newPtr, err := n.addNewMap(val)
			if err != nil {
				return err
			}

			return n.filer.WriteInt64(n.file, ptr+int64Length, newPtr)
		}

		ptr = nextElementPtr
	}
}

func (n *Node) getMapItem(ptr int64) (int64, int64, error) {
	buf, eof, err := n.filer.ReadBytes(n.file, ptr, int64Length*2)
	if err != nil {
		return 0, 0, err
	}

	if eof {
		return 0, 0, fmt.Errorf("get next map item operation returned eof, read over attempt")
	}

	return n.bufToInt64(buf[:int64Length]), n.bufToInt64(buf[int64Length : int64Length+int64Length]), nil
}

func (n *Node) getAllMapItems(ptr int64) ([]int64, error) {
	result := make([]int64, 0)
	currPtr := ptr
	for {
		val, nextPtr, err := n.getMapItem(currPtr)
		if err != nil {
			return result, err
		}

		result = append(result, val)
		if nextPtr == 0 {
			break
		}

		currPtr = nextPtr
	}

	return result, nil
}
