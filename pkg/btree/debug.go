package btree

import (
	filemanager "godb/pkg/file"
	"os"
	"strconv"
	"strings"
)

type debugParams struct {
	filer filemanager.Filer
	file  *os.File
}

// DisplayTree is only for debugging, when all done this may have to be removed, It saves tree nodes as text
func DisplayTree(indexName string) error {
	indexFileName := indexName + indexFileExt
	filer := filemanager.New()

	file, err := filer.OpenReadWrite(indexFileName)
	if err != nil {
		return err
	}
	defer file.Close()

	debugParams := &debugParams{
		filer: filer,
		file:  file,
	}

	ptr, _, _ := filer.ReadInt64(file, 0)
	displayTreeRecursive(debugParams, 0, ptr, ptr)

	return nil

}

func displayTreeRecursive(debugParams *debugParams, level int, ptr int64, parentNodePtr int64) error {
	node := NewNode(debugParams.file, debugParams.filer, nodeSize, 5, parentNodePtr)
	err := node.load(ptr)
	if err != nil {
		return err
	}

	saveNodeInfo(debugParams.filer, node, level)
	if node.leftChild != 0 {
		displayTreeRecursive(debugParams, level+1, node.leftChild, parentNodePtr)
	}

	for _, dat := range node.data {
		if dat.isSet && dat.children != 0 {
			displayTreeRecursive(debugParams, level+1, dat.children, parentNodePtr)
		}
	}

	return nil
}

func getDebugInfo(n *Node, level int) []byte {
	str := &strings.Builder{}

	str.WriteString("Level: ")
	str.WriteString(strconv.Itoa(level))
	str.Write([]byte{13, 10})
	str.WriteString("Buffer size: ")
	str.WriteString(strconv.Itoa(n.bufSize))
	str.Write([]byte{13, 10})

	str.WriteString("Max element length: ")
	str.WriteString(strconv.Itoa(n.maxElementCount))
	str.Write([]byte{13, 10})

	str.WriteString("Parent: ")
	str.WriteString(strconv.FormatInt(n.parentNodePtr, 10))
	str.Write([]byte{13, 10})

	str.WriteString("Left child: ")
	str.WriteString(strconv.FormatInt(n.leftChild, 10))
	str.Write([]byte{13, 10})

	str.WriteString("Current item index: ")
	str.WriteString(strconv.Itoa(n.itemIndex))
	str.Write([]byte{13, 10})

	str.WriteString("Items:")
	str.Write([]byte{13, 10})

	for i, data := range n.data {

		str.WriteString("  node index: ")
		str.WriteString(strconv.Itoa(i))
		str.Write([]byte{13, 10})

		str.WriteString("    data: ")
		str.Write(data.data)
		str.Write([]byte{13, 10})

		str.WriteString("    childPointer: ")
		str.WriteString(strconv.FormatInt(data.children, 10))
		str.Write([]byte{13, 10})

		if data.isSet {
			str.WriteString("    is set: true")
		} else {
			str.WriteString("    is set: false")
		}
		str.Write([]byte{13, 10})

		str.WriteString("    map ptr: ")
		str.WriteString(strconv.FormatInt(data.mapPtr, 10))
		str.Write([]byte{13, 10})

		str.WriteString("    next fetch map ptr: ")
		str.WriteString(strconv.FormatInt(data.fetchMmapPtr, 10))
		str.Write([]byte{13, 10})

		if data.mapPtr != 0 {
			firstMapItemValue, nextMapItemPointer, err := n.getMapItem(data.mapPtr)
			if err != nil {
				str.WriteString("      error could not fetch map item: ")
				str.WriteString(err.Error())
				str.Write([]byte{13, 10})
			} else {
				str.WriteString("      first map value: ")
				str.WriteString(strconv.FormatInt(firstMapItemValue, 10))
				str.Write([]byte{13, 10})

				str.WriteString("      next map pointer: ")
				str.WriteString(strconv.FormatInt(nextMapItemPointer, 10))
				str.Write([]byte{13, 10})
			}

			mapItems, err := n.getAllMapItems(data.mapPtr)
			if err != nil {
				str.WriteString("      error could not fetch map items: ")
				str.WriteString(err.Error())
				str.Write([]byte{13, 10})
			} else {
				str.WriteString("      map items: ")
				for i, mi := range mapItems {
					if i > 0 {
						str.WriteString(", ")
					}
					str.WriteString(strconv.FormatInt(mi, 10))
				}
				str.Write([]byte{13, 10})
			}
		}
	}

	str.Write([]byte{13, 10})
	return []byte(str.String())
}

func saveNodeInfo(filer filemanager.Filer, node *Node, level int) error {
	reportFileName := strconv.FormatInt(node.currentPtr, 10) + "_" + strconv.Itoa(level) + "_" + strconv.FormatInt(node.parentNodePtr, 10)
	fileName := filer.GetFullFilePath(reportFileName)

	file, err := os.Create(fileName)
	if err != nil {
		return err
	}

	_, err = file.Write(getDebugInfo(node, level))
	if err != nil {
		return err
	}

	return file.Close()
}
