// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package fileops

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsDirEmpty(t *testing.T) {
	testDir := prepareTestDir(t)

	t.Run("non-empty directory", func(t *testing.T) {
		isEmpty, err := IsDirEmpty(path.Join(testDir, "dir"))
		require.NoError(t, err)
		require.False(t, isEmpty)
	})

	t.Run("empty directory", func(t *testing.T) {
		require.NoError(t, CreateDir(path.Join(testDir, "z")))
		isEmpty, err := IsDirEmpty(path.Join(testDir, "z"))
		require.NoError(t, err)
		require.True(t, isEmpty)
	})

	t.Run("error case", func(t *testing.T) {
		dirPath := path.Join(testDir, "xx")
		_, err := IsDirEmpty(dirPath)
		require.EqualError(t, err,
			fmt.Sprintf("error opening dir [%s]: open %s: no such file or directory", dirPath, dirPath))
	})
}

func TestListSubdirs(t *testing.T) {
	testDir := prepareTestDir(t)

	t.Run("subdirs exist", func(t *testing.T) {
		dirs, err := ListSubdirs(path.Join(testDir, "dir"))
		require.NoError(t, err)
		expectedDirs := []string{"a", "b", "c", "d"}
		require.Equal(t, expectedDirs, dirs)
	})

	t.Run("subdirs do not exist", func(t *testing.T) {
		dirs, err := ListSubdirs(path.Join(testDir, "dir", "a"))
		require.NoError(t, err)
		require.Empty(t, dirs)
	})

	t.Run("error case", func(t *testing.T) {
		dirPath := path.Join(testDir, "xx")
		_, err := ListSubdirs(dirPath)
		require.EqualError(t, err,
			fmt.Sprintf("error reading dir [%s]: open %s: no such file or directory", dirPath, dirPath))
	})
}

func TestFileExists(t *testing.T) {
	testDir := prepareTestDir(t)

	exists, err := Exists(path.Join(testDir, "dir"))
	require.NoError(t, err)
	require.True(t, exists)

	exists, err = Exists(path.Join(testDir, "dir", "e"))
	require.NoError(t, err)
	require.True(t, exists)

	exists, err = Exists(path.Join(testDir, "dir", "xx"))
	require.NoError(t, err)
	require.False(t, exists)
}

func TestCreateDir(t *testing.T) {
	testDir := prepareTestDir(t)

	require.DirExists(t, path.Join(testDir, "dir"))
	require.NoError(t, CreateDir(path.Join(testDir, "dir")))

	require.NoError(t, CreateDir(path.Join(testDir, "tmp")))
	require.DirExists(t, path.Join(testDir, "tmp"))
	require.NoError(t, os.RemoveAll(path.Join(testDir, "tmp")))

	require.NoError(t, CreateDir(path.Join(testDir, "tmp")))
	require.DirExists(t, path.Join(testDir, "tmp"))
	require.NoError(t, os.RemoveAll(path.Join(testDir, "tmp")))
}

func TestOpenFile(t *testing.T) {
	testDir := prepareTestDir(t)

	testCases := []struct {
		description  string
		filePath     string
		permission   os.FileMode
		existingFile bool
	}{
		{
			description:  "opening an existing file",
			filePath:     path.Join(testDir, "dir", "e"),
			permission:   0644,
			existingFile: true,
		},
		{
			description:  "opening a new file",
			filePath:     path.Join(testDir, "dir", "new-file"),
			permission:   0755,
			existingFile: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			switch tc.existingFile {
			case true:
				require.FileExists(t, tc.filePath)
			default:
				defer os.Remove(tc.filePath)
			}

			f, err := OpenFile(tc.filePath, tc.permission)
			require.NoError(t, err)
			require.Equal(t, tc.filePath, f.Name())
			require.FileExists(t, tc.filePath)

			stats, err := f.Stat()
			require.NoError(t, err)
			require.Equal(t, tc.permission, stats.Mode())
		})
	}
}

func TestCreateFile(t *testing.T) {
	testDir := prepareTestDir(t)

	tests := []struct {
		name         string
		filePath     string
		existingFile bool
		expectedErr  string
	}{
		{
			name:         "creating a new file",
			filePath:     path.Join(testDir, "new-File"),
			existingFile: false,
			expectedErr:  "",
		},
		{
			name:         "file already exist",
			filePath:     path.Join(testDir, "dir", "e"),
			existingFile: true,
			expectedErr:  "/dir/e exist",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			if tt.existingFile {
				require.FileExists(t, tt.filePath)
			}

			err := CreateFile(tt.filePath)
			if tt.expectedErr != "" {
				require.Contains(t, err.Error(), tt.expectedErr)
			}
			require.FileExists(t, tt.filePath)
		})
	}
}

func TestRemove(t *testing.T) {
	testDir := prepareTestDir(t)

	var tests = []struct {
		name        string
		filePath    string
		errExpected bool
	}{
		{
			name:        "removing a file",
			filePath:    path.Join(testDir, "dir", "e"),
			errExpected: false,
		},
		{
			name:        "removing an empty dir",
			filePath:    path.Join(testDir, "dir", "a"),
			errExpected: false,
		},
		{
			name:        "removing a non-existing file",
			filePath:    path.Join(testDir, "dir", "doesnotexist"),
			errExpected: true,
		},
		{
			name:        "removing an non-empty dir",
			filePath:    path.Join(testDir, "dir"),
			errExpected: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			switch tt.errExpected {
			case true:
				require.NotNil(t, Remove(tt.filePath))
			default:
				require.Nil(t, Remove(tt.filePath))
			}
		})
	}
}

func TestRemoveAll(t *testing.T) {
	testDir := prepareTestDir(t)

	var tests = []struct {
		name     string
		filePath string
	}{
		{
			name:     "removing an non-empty directory",
			filePath: path.Join(testDir, "dir"),
		},
		{
			name:     "removing a non-existing file",
			filePath: path.Join(testDir, "doesnotexist"),
			// os.RemoveAll does not return error if file does not exist
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			require.NoError(t, RemoveAll(tt.filePath))
		})
	}
}

func TestWrite(t *testing.T) {
	testDir := prepareTestDir(t)

	setup := func() (*os.File, *os.File) {
		contentFilePath := path.Join(testDir, "contentfile")
		contentFile, err := OpenFile(contentFilePath, 0644)
		require.NoError(t, err)
		l, err := contentFile.Write([]byte("hello"))
		require.NoError(t, err)
		require.Equal(t, len([]byte("hello")), l)

		emptyFilePath := path.Join(testDir, "emptyfile")
		emptyFile, err := OpenFile(emptyFilePath, 0644)
		require.NoError(t, err)

		return contentFile, emptyFile
	}

	contentFile, emptyFile := setup()

	var tests = []struct {
		name            string
		file            *os.File
		writeContent    []byte
		expectedContent []byte
	}{
		{
			name:            "write to non-empty file",
			file:            contentFile,
			writeContent:    []byte("world"),
			expectedContent: []byte("helloworld"),
		},
		{
			name:            "write to an empty file",
			file:            emptyFile,
			writeContent:    []byte("world"),
			expectedContent: []byte("world"),
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			l, err := Write(tt.file, tt.writeContent)
			require.NoError(t, err)
			require.Equal(t, len(tt.writeContent), l)

			content, err := ioutil.ReadFile(tt.file.Name())
			require.NoError(t, err)
			require.Equal(t, tt.expectedContent, content)
		})
	}
}

func TestTruncate(t *testing.T) {
	testDir := prepareTestDir(t)

	setup := func() *os.File {
		contentFilePath := path.Join(testDir, "contentfile")
		contentFile, err := OpenFile(contentFilePath, 0644)
		require.NoError(t, err)
		l, err := contentFile.Write([]byte("helloworld"))
		require.NoError(t, err)
		require.Equal(t, len([]byte("helloworld")), l)

		return contentFile
	}

	contentFile := setup()

	var tests = []struct {
		name            string
		file            *os.File
		truncateTo      int64
		expectedContent []byte
		offsetAt        int64
	}{
		{
			name:            "truncate some content",
			file:            contentFile,
			truncateTo:      5,
			expectedContent: []byte("hello"),
			offsetAt:        5,
		},
		{
			name:            "truncate to empty",
			file:            contentFile,
			truncateTo:      0,
			expectedContent: []byte{},
			offsetAt:        0,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			require.NoError(t, Truncate(tt.file, tt.truncateTo))

			content, err := ioutil.ReadFile(tt.file.Name())
			require.NoError(t, err)
			require.Equal(t, tt.expectedContent, content)

			stats, err := contentFile.Stat()
			require.NoError(t, err)
			require.Equal(t, tt.offsetAt, stats.Size())
		})
	}
}

func TestSyncDir(t *testing.T) {
	testDir := prepareTestDir(t)

	t.Run("green-path", func(t *testing.T) {
		testPath := path.Join(testDir, "dir")
		require.NoError(t, SyncDir(testPath))
	})

	t.Run("non-existent-dir", func(t *testing.T) {
		dirPath := path.Join(testDir, "non-existent-dir")
		err := SyncDir(dirPath)
		require.EqualError(t, err,
			fmt.Sprintf("error while opening dir: %s: open %s: no such file or directory", dirPath, dirPath))
	})
}

func prepareTestDir(t *testing.T) string {
	tempDir := t.TempDir()

	require.NoError(t, os.Mkdir(path.Join(tempDir, "dir"), 0755))
	require.NoError(t, os.Mkdir(path.Join(tempDir, "dir", "a"), 0755))
	require.NoError(t, os.Mkdir(path.Join(tempDir, "dir", "b"), 0755))
	require.NoError(t, os.Mkdir(path.Join(tempDir, "dir", "c"), 0755))
	require.NoError(t, os.Mkdir(path.Join(tempDir, "dir", "d"), 0755))

	file, err := os.OpenFile(path.Join(tempDir, "dir", "e"), os.O_CREATE, 0644)
	require.NoError(t, err)
	defer file.Close()

	return tempDir
}
