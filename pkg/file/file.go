// Package filemanager responsible to low level file read and writes
package filemanager

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"unsafe"
)

const (
	// Int64Length is the generic length assigned to int field type
	Int64Length = 8
	// DefaultFolder is the default database file folder
	DefaultFolder = "./dbfolder"
	// PointerRecordLength is int64 + 1 deleted flag
	PointerRecordLength = 9
)

var (
	errSeekFile  = errors.New("error seeking file")
	errReadFile  = errors.New("error reading file")
	errWriteFile = errors.New("failed to write to file")
)

// New creates a new file manager
func New() Filer {
	return &fil{}
}

// Filer contains methods for low level file operations
type Filer interface {
	GetDbFolder() string
	GetFullFilePath(s string) string
	GetDatFilePointer(file *os.File, recNo int64) (int64, bool, bool, error)
	OpenReadWrite(fileName string) (*os.File, error)
	ReadBytes(file *os.File, filePointer int64, bytesToRead int) ([]byte, bool, error)
	ReadInt64(file *os.File, filePointer int64) (int64, bool, error)
	WriteBytes(file *os.File, filePointer int64, bytes []byte) error
	WriteInt64(file *os.File, filePointer int64, num int64) error
	AppendBytes(file *os.File, buf []byte) (int64, error)
	CreateDBFolderIfNotExists() error
	CreateBlankFileOverwriteIfExist(fileName string) error
	CreateBlankFileIfNotExist(fileName string) (bool, error)
}

type fil struct {
}

// GetDbFolder will retrieve the folder of the database files
func (*fil) GetDbFolder() string {
	// Todo later add folder from conf
	return DefaultFolder
}

// GetFullFilePath will return with the full path in the db folder of the specified file
func (d *fil) GetFullFilePath(s string) string {
	return d.GetDbFolder() + "/" + s
}

// GetDatFilePointer returns the pointer of the data file by it's record no
func (d *fil) GetDatFilePointer(file *os.File, recNo int64) (int64, bool, bool, error) {
	recordFilePointer := recNo * PointerRecordLength

	datPointerInfo, eof, err := d.ReadBytes(file, recordFilePointer, PointerRecordLength)
	if err != nil {
		return 0, false, false, err
	}

	if eof {
		return 0, false, true, err
	}

	// Deleted flag
	if datPointerInfo[Int64Length] == 0 {
		return int64(binary.LittleEndian.Uint64(datPointerInfo[:Int64Length])), false, false, nil
	}

	return int64(binary.LittleEndian.Uint64(datPointerInfo[:Int64Length])), true, false, nil
}

// OpenReadWrite Opens a file from the database directory read/write
func (d *fil) OpenReadWrite(fileName string) (*os.File, error) {
	fn := d.GetFullFilePath(fileName)
	file, err := os.OpenFile(fn, os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	return file, nil
}

// ReadBytes read specified amount of bytes from the file from the specified file pointer
func (d *fil) ReadBytes(file *os.File, filePointer int64, bytesToRead int) ([]byte, bool, error) {
	_, err := file.Seek(filePointer, io.SeekStart)
	if err != nil {
		return nil, false, errSeekFile
	}

	buffer := make([]byte, bytesToRead)
	_, err = file.Read(buffer)
	if err != nil {
		if err == io.EOF {
			return nil, true, nil
		}
		return nil, false, errReadFile
	}

	return buffer, false, nil
}

// WriteBytes writes a byte buffer to a specified pointer
func (d *fil) WriteBytes(file *os.File, filePointer int64, buf []byte) error {
	_, err := file.Seek(filePointer, io.SeekStart)
	if err != nil {
		return errSeekFile
	}

	_, err = file.Write(buf)
	if err != nil {
		return errWriteFile
	}

	return nil
}

// AppendBytes append a buffer to the end of the file and return the offset where it was written
func (d *fil) AppendBytes(file *os.File, buf []byte) (int64, error) {
	offset, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return offset, errSeekFile
	}

	_, err = file.Write(buf)
	if err != nil {
		return offset, errWriteFile
	}

	return offset, nil
}

// WriteInt64 write int to a specific file pointer
func (d *fil) WriteInt64(file *os.File, filePointer int64, num int64) error {
	_, err := file.Seek(filePointer, io.SeekStart)
	if err != nil {
		return errSeekFile
	}

	// Might need to compare performance the above is more platform safe but may be less effective
	// buf := make([]byte, Int64Length) // Allocate 8 bytes since int64 is 8 bytes
	// binary.BigEndian.PutUint64(buf, uint64(num)) // Convert int64 to uint64 for byte conversion

	// buf := (*[Int64Length]byte)(unsafe.Pointer(&num))[:]
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(num))

	_, err = file.Write(buf)
	if err != nil {
		return errWriteFile
	}

	return nil
}

// ReadInt64 reads an int64 from a specific file pointer
func (d *fil) ReadInt64(file *os.File, filePointer int64) (int64, bool, error) {
	_, err := file.Seek(filePointer, io.SeekStart)
	if err != nil {
		return 0, false, errSeekFile
	}

	buf := make([]byte, Int64Length)
	_, err = file.Read(buf)
	if err != nil {
		if err == io.EOF {
			return 0, true, nil
		}
		return 0, false, errReadFile
	}

	// num := int64(binary.BigEndian.Uint64(buf)) // This is the safe but may be less performant way, read desc above, swap if other swapped
	num := *(*int64)(unsafe.Pointer(&buf[0])) // Convert []byte back to int64

	return num, false, nil
}

// CreateDBFolderIfNotExists Creates the db folder if not exists already
func (d *fil) CreateDBFolderIfNotExists() error {
	path := d.GetDbFolder()
	if _, err := os.Stat(path); os.IsNotExist(err) {
		err := os.MkdirAll(path, 0755)
		if err != nil {
			return fmt.Errorf("failed to create directory: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("failed to check directory: %w", err)
	}

	return nil
}

// CreateBlankFileIfNotExist will create a blank file, including DB folder if not exists
func (d *fil) CreateBlankFileIfNotExist(fileName string) (bool, error) {
	fn := d.GetFullFilePath(fileName)
	_, err := os.Stat(fn)
	if os.IsNotExist(err) {
		d.CreateDBFolderIfNotExists()
		file, err := os.Create(fn)
		if err != nil {
			return false, fmt.Errorf("error creating index file: %s", err.Error())
		}
		defer file.Close()
		return true, nil
	}

	return false, nil
}

// CreateBlankFileOverwriteIfExist creates a file reset is's size to 0 if exists
func (d *fil) CreateBlankFileOverwriteIfExist(fileName string) error {
	fn := d.GetFullFilePath(fileName)
	file, err := os.OpenFile(fn, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	return nil
}
