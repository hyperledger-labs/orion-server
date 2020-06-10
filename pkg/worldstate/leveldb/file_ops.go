package leveldb

import (
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"

	"github.com/pkg/errors"
)

// isDirEmpty returns true if the dir at dirPath is empty
func isDirEmpty(dirPath string) (bool, error) {
	f, err := os.Open(dirPath)
	if err != nil {
		return false, errors.WithMessagef(err, "error opening dir [%s]", dirPath)
	}
	defer f.Close()

	_, err = f.Readdir(1)
	if err == io.EOF {
		return true, nil
	}
	err = errors.WithMessagef(err, "error checking if dir [%s] is empty", dirPath)
	return false, err
}

// listSubdirs returns the subdirectories
func listSubdirs(dirPath string) ([]string, error) {
	subdirs := []string{}
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

// fileExists checks whether the given file exists.
func fileExists(filePath string) (bool, error) {
	_, err := os.Stat(filePath)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, errors.Wrapf(err, "error checking if file [%s] exists", filePath)
	}
	return true, nil
}

// createDir creates a dir for dirPath if not already exists
func createDir(dirPath string) error {
	if !strings.HasSuffix(dirPath, "/") {
		dirPath = dirPath + "/"
	}
	return os.MkdirAll(path.Dir(dirPath), 0755)
}
