package btree

import (
	filemanager "godb/pkg/file"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"
)

type nodeSplit0TestSuite struct {
	suite.Suite
	node *Node
}

func TestSplit0NodeRunner(t *testing.T) {
	suite.Run(t, new(nodeSplit0TestSuite))
}

func (t *nodeSplit0TestSuite) SetupTest() {
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
	// t.node = NewNode(file, filer, 6, 5, 0)
	t.node = NewInt64Node(file, filer, 6, 0)
	t.node.save(8)
}

func (t *nodeSplit0TestSuite) TearDownTest() {
	t.node = nil
}

// Main test is here
func (t *nodeSplit0TestSuite) TestSaveLoadNode() {

	loaderNode := t.node.add(0)

	// for i := 1000; i > 0; i-- {
	for i := 10000; i >= 1; i-- {
		// for i := 1; i <= 10000; i++ {
		// for i := 1; i <= 65; i++ {
		// it := []byte(fmt.Sprintf("%0.5d", i))
		it := loaderNode.int64ToBuf(int64(i))
		ptr, f := t.search(&it)
		if !f {
			loaderNode.load(ptr)
			loaderNode.insert(it, 0)
		}
	}

	ptr, _, _ := t.node.filer.ReadInt64(t.node.file, 0)
	t.renderAll(ptr)
}

func (t *nodeSplit0TestSuite) search(s *[]byte) (int64, bool) {
	ptr, _, _ := t.node.filer.ReadInt64(t.node.file, 0)

	return t.rSearch(ptr, s)
}

func (t *nodeSplit0TestSuite) rSearch(ptr int64, s *[]byte) (int64, bool) {
	sNode := t.node.add(0)
	sNode.load(ptr)
	i, f := sNode.locateInData(s)
	if f {
		return ptr, true
	}

	if i == 0 {
		if sNode.leftChild != 0 {
			return t.rSearch(sNode.leftChild, s)
		}

		// not found, reached the beginning
		return sNode.currentPtr, false
	}

	if sNode.data[i-1].children != 0 {
		return t.rSearch(sNode.data[i-1].children, s)
	}

	return sNode.currentPtr, false
}

func (t *nodeSplit0TestSuite) renderAll(ptrs ...int64) {
	loaderNode := t.node.add(0)
	subLoaderNode := t.node.add(0)

	for _, ptr := range ptrs {
		loaderNode.load(ptr)
		t.saveNodeDebug(loaderNode)
		if loaderNode.leftChild != 0 {
			subLoaderNode.load(loaderNode.leftChild)
			t.saveNodeDebug(subLoaderNode)
			t.renderAll(loaderNode.leftChild)

		}

		for _, d := range loaderNode.data {
			if d.isSet && d.children != 0 {
				subLoaderNode.load(d.children)
				t.saveNodeDebug(subLoaderNode)
				t.renderAll(d.children)
			}
		}
	}
}

func (t *nodeSplit0TestSuite) saveNodeDebug(node *Node) {
	reportFileName := strconv.FormatInt(node.currentPtr, 10) + ".html"
	// fileName := t.node.filer.GetFullFilePath(reportFileName)
	fileName := "./treedata/" + reportFileName

	file, _ := os.Create(fileName)
	defer file.Close()
	_, _ = file.Write(t.renderDebugNode(node))
}

func (t *nodeSplit0TestSuite) renderDebugNode(n *Node) []byte {
	b := &strings.Builder{}
	b.WriteString(`<style>
	table {
		border-collapse: collapse;
	}
	table th, table td {
		border: 1px solid;
		width: 50px;
		text-align: center;
	}
	</style>`)
	b.WriteString("<table><thead>")

	b.WriteString("<tr><th colspan=\"" + strconv.Itoa(n.maxElementCount+2) + "\">")
	b.WriteString("Current Ptr ")
	b.WriteString(strconv.FormatInt(n.currentPtr, 10))
	b.WriteString("</th></tr>")

	b.WriteString("<tr><th colspan=\"" + strconv.Itoa(n.maxElementCount+2) + "\">")
	b.WriteString("Parent Ptr <a href=\"" + strconv.FormatInt(n.parentNodePtr, 10) + ".html\">")
	b.WriteString(strconv.FormatInt(n.parentNodePtr, 10))
	b.WriteString("</a></th></tr>")

	b.WriteString("</thead>")

	b.WriteString("<tbody>")
	b.WriteString("<tr>")

	b.WriteString("<td>Left Child</td>")

	for _, dat := range n.data {
		b.WriteString("<td>")
		// b.Write(dat.data)
		num := n.bufToInt64(dat.data)
		b.WriteString(strconv.FormatInt(num, 10))

		b.WriteString("</td>")
	}
	b.WriteString("</tr>")

	b.WriteString("<tr>")
	if n.leftChild == 0 {
		b.WriteString("<td>0</td>")
	} else {
		b.WriteString("<td><a href=\"" + strconv.FormatInt(n.leftChild, 10) + ".html\">")
		b.WriteString(strconv.FormatInt(n.leftChild, 10))
		b.WriteString("</td>")
	}

	for _, dat := range n.data {
		b.WriteString("<td>")
		if dat.children == 0 {
			b.WriteString(strconv.FormatInt(dat.children, 10))
		} else {
			b.WriteString("<a href=\"" + strconv.FormatInt(dat.children, 10) + ".html\">")
			b.WriteString(strconv.FormatInt(dat.children, 10))
			b.WriteString("</a>")
		}
		b.WriteString("</td>")
	}
	b.WriteString("</tr>")
	b.WriteString("</tbody>")
	b.WriteString("</table>")

	return []byte(b.String())
}
