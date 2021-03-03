// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package fileops

import (
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
)

// IsDirEmpty returns true if the dir at dirPath is empty
func IsDirEmpty(dirPath string) (bool, error) {
	f, err := os.Open(dirPath)
	if err != nil {
		return false, errors.WithMessagef(err, "error opening dir [%s]", dirPath)
	}
	defer f.Close()

	_, err = f.Readdir(1)
	if err == io.EOF {
		return true, nil
	}

	if err != nil {
		return false, errors.WithMessagef(err, "error checking if dir [%s] is empty", dirPath)
	}
	return false, nil
}

// ListSubdirs returns the subdirectories
func ListSubdirs(dirPath string) ([]string, error) {
	var subdirs []string

	files, err := ioutil.ReadDir(dirPath)
	if err != nil {
		return nil, errors.Wrapf(err, "error reading dir [%s]", dirPath)
	}

	for _, f := range files {
		if f.IsDir() {
			subdirs = append(subdirs, f.Name())
		}
	}

	return subdirs, nil
}

// Exists checks whether the given file exists.
func Exists(filePath string) (bool, error) {
	_, err := os.Stat(filePath)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, errors.Wrapf(err, "error checking if file [%s] exists", filePath)
	}
	return true, nil
}

// CreateDir creates a dir for dirPath. If the dirPath already
// exists, it returns nil, i.e., no-op
func CreateDir(dirPath string) error {
	if !strings.HasSuffix(dirPath, "/") {
		dirPath = dirPath + "/"
	}

	if err := os.MkdirAll(path.Dir(dirPath), 0755); err != nil {
		return err
	}

	return SyncDir(filepath.Dir(dirPath))
}

// OpenFile opens an existing file. If the file does not exist, it creates
// a new one.
func OpenFile(filePath string, perm os.FileMode) (*os.File, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, perm)
	if err != nil {
		return nil, errors.Wrapf(err, "error while opening file:%s", filePath)
	}

	if err := SyncDir(filepath.Dir(filePath)); err != nil {
		if closeErr := file.Close(); closeErr != nil {
			log.Printf("error while closing the file [%s]", file.Name())
		}
		return nil, errors.Wrap(err, "error while synching the opened file")
	}

	return file, nil
}

// CreateFile creates a file. It retuns an error if the file already exists
func CreateFile(filePath string) error {
	exist, err := Exists(filePath)
	if err != nil {
		return err
	}
	if exist {
		return errors.New("file " + filePath + " exist")
	}

	file, err := os.Create(filePath)
	if err != nil {
		return errors.Wrapf(err, "error while creating file [%s]", filePath)
	}

	if err := file.Close(); err != nil {
		return errors.Wrap(err, "error while closing the file")
	}

	if err := SyncDir(filepath.Dir(filePath)); err != nil {
		return errors.Wrap(err, "error while synching the created file")
	}

	return nil
}

// Remove removes the file
func Remove(path string) error {
	if err := os.Remove(path); err != nil {
		return err
	}

	return SyncDir(filepath.Dir(path))
}

// RemoveAll removes all files/directories present in the
// path including the path itself
func RemoveAll(path string) error {
	if err := os.RemoveAll(path); err != nil {
		return err
	}

	return SyncDir(filepath.Dir(path))
}

// Write writes the given content to the file
func Write(f *os.File, content []byte) (int, error) {
	n, err := f.Write(content)
	if err != nil {
		return n, errors.Wrapf(err, "error while writing to file [%s]", f.Name())
	}

	if err := f.Sync(); err != nil {
		return n, errors.Wrapf(err, "error while synching the file [%s]", f.Name())
	}

	return n, SyncDir(filepath.Dir(f.Name()))
}

// Truncate truncates the file to a given size and also reset the IO offset
func Truncate(f *os.File, toSize int64) error {
	if err := f.Truncate(toSize); err != nil {
		return err
	}

	if _, err := f.Seek(toSize, 0); err != nil {
		return errors.Wrapf(err, "error while setting IO offset for file [%s] to %d offset", f.Name(), toSize)
	}

	if err := f.Sync(); err != nil {
		return errors.Wrapf(err, "error while truncating file [%s] to size %d", f.Name(), toSize)
	}

	return SyncDir(filepath.Dir(f.Name()))
}

// SyncDir fsyncs the given dir
func SyncDir(dirPath string) error {
	dir, err := os.Open(dirPath)
	if err != nil {
		return errors.Wrapf(err, "error while opening dir: %s", dirPath)
	}

	if err := dir.Sync(); err != nil {
		if closeErr := dir.Close(); closeErr != nil {
			log.Printf("error while closing the directory [%s]", dir.Name())
		}
		return errors.Wrapf(err, "error while synching dir: %s", dirPath)
	}

	if err := dir.Close(); err != nil {
		return errors.Wrapf(err, "error while closing dir: %s", dirPath)
	}

	return nil
}
