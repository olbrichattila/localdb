package btree

// func (n *Node) saveNodeDebug() {
// 	reportFileName := strconv.FormatInt(n.currentPtr, 10) + ".html"
// 	fileName := "./treedata/" + reportFileName

// 	file, _ := os.Create(fileName)
// 	defer file.Close()
// 	_, _ = file.Write(n.renderDebugNode())
// }

// func (n *Node) renderDebugNode() []byte {
// 	b := &strings.Builder{}
// 	b.WriteString(`<style>
// 	table {
// 		border-collapse: collapse;
// 	}
// 	table th, table td {
// 		border: 1px solid;
// 		width: 50px;
// 		text-align: center;
// 	}
// 	</style>`)
// 	b.WriteString("<table><thead>")

// 	b.WriteString("<tr><th colspan=\"" + strconv.Itoa(n.maxElementCount+2) + "\">")
// 	b.WriteString("Current Ptr ")
// 	b.WriteString(strconv.FormatInt(n.currentPtr, 10))
// 	b.WriteString("</th></tr>")

// 	b.WriteString("<tr><th colspan=\"" + strconv.Itoa(n.maxElementCount+2) + "\">")
// 	b.WriteString("Parent Ptr <a href=\"" + strconv.FormatInt(n.parentNodePtr, 10) + ".html\">")
// 	b.WriteString(strconv.FormatInt(n.parentNodePtr, 10))
// 	b.WriteString("</a></th></tr>")

// 	b.WriteString("</thead>")

// 	b.WriteString("<tbody>")
// 	b.WriteString("<tr>")

// 	b.WriteString("<td>Left Child</td>")

// 	for _, dat := range n.data {
// 		b.WriteString("<td>")
// 		// b.Write(dat.data)
// 		num := n.bufToInt64(dat.data)
// 		b.WriteString(strconv.FormatInt(num, 10))

// 		b.WriteString("</td>")
// 	}
// 	b.WriteString("</tr>")

// 	b.WriteString("<tr>")
// 	if n.leftChild == 0 {
// 		b.WriteString("<td>0</td>")
// 	} else {
// 		b.WriteString("<td><a href=\"" + strconv.FormatInt(n.leftChild, 10) + ".html\">")
// 		b.WriteString(strconv.FormatInt(n.leftChild, 10))
// 		b.WriteString("</td>")
// 	}

// 	for _, dat := range n.data {
// 		b.WriteString("<td>")
// 		if dat.children == 0 {
// 			b.WriteString(strconv.FormatInt(dat.children, 10))
// 		} else {
// 			b.WriteString("<a href=\"" + strconv.FormatInt(dat.children, 10) + ".html\">")
// 			b.WriteString(strconv.FormatInt(dat.children, 10))
// 			b.WriteString("</a>")
// 		}
// 		b.WriteString("</td>")
// 	}
// 	b.WriteString("</tr>")
// 	b.WriteString("</tbody>")
// 	b.WriteString("</table>")

// 	return []byte(b.String())
// }
