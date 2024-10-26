package localdb

import filemanager "godb/pkg/file"

func newDeleter() deleter {
	return &del{filer: filemanager.New()}
}

type deleter interface {
	Delete(c *currentTable, recNo int64) error
}

type del struct {
	filer filemanager.Filer
}

func (d *del) Delete(c *currentTable, recNo int64) error {
	ptrFilePointer := recNo*filemanager.PointerRecordLength + filemanager.Int64Length

	return d.filer.WriteBytes(c.fileHandlers.rpt, ptrFilePointer, []byte{1})
}
