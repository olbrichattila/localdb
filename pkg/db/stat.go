package localdb

import (
	filemanager "godb/pkg/file"
	"os"
)

func newStat() stater {
	return &stat{
		filer: filemanager.New(),
	}
}

type stater interface {
	RecCount(*CurrentTable) (int64, error)
}

type stat struct {
	filer filemanager.Filer
}

func (s *stat) RecCount(c *CurrentTable) (int64, error) {
	filePath := s.filer.GetFullFilePath(c.tableName + recordPointerFileExt)

	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return 0, err
	}

	return fileInfo.Size() / filemanager.PointerRecordLength, nil
}
