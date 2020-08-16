package fileops

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsDirEmpty(t *testing.T) {
	t.Run("non-empty directory", func(t *testing.T) {
		t.Parallel()
		isEmpty, err := IsDirEmpty("./testdata/dir")
		require.NoError(t, err)
		require.False(t, isEmpty)
	})

	t.Run("empty directory", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, CreateDir("./testdata/z"))
		isEmpty, err := IsDirEmpty("./testdata/z")
		require.NoError(t, err)
		require.True(t, isEmpty)
		require.NoError(t, os.RemoveAll("./testdata/z"))
	})

	t.Run("error case", func(t *testing.T) {
		t.Parallel()
		_, err := IsDirEmpty("xx")
		require.Contains(t, err.Error(), "error opening dir [xx]")
	})
}

func TestListSubdirs(t *testing.T) {
	t.Run("subdirs exist", func(t *testing.T) {
		t.Parallel()
		dirs, err := ListSubdirs("./testdata/dir")
		require.NoError(t, err)
		expectedDirs := []string{"a", "b", "c", "d"}
		require.Equal(t, expectedDirs, dirs)
	})

	t.Run("subdirs do not exist", func(t *testing.T) {
		t.Parallel()
		dirs, err := ListSubdirs("./testdata/dir/a")
		require.NoError(t, err)
		require.Empty(t, dirs)
	})

	t.Run("error case", func(t *testing.T) {
		t.Parallel()
		_, err := ListSubdirs("xx")
		require.Contains(t, err.Error(), "error reading dir [xx]")
	})
}

func TestFileExists(t *testing.T) {
	exists, err := Exists("./testdata/dir")
	require.NoError(t, err)
	require.True(t, exists)

	exists, err = Exists("./testdata/dir/e")
	require.NoError(t, err)
	require.True(t, exists)

	exists, err = Exists("xx")
	require.NoError(t, err)
	require.False(t, exists)
}

func TestCreateDir(t *testing.T) {
	require.DirExists(t, "./testdata/dir")
	require.NoError(t, CreateDir("./testdata/dir"))

	require.NoError(t, CreateDir("./testdata/tmp"))
	require.DirExists(t, "./testdata/tmp")
	require.NoError(t, os.RemoveAll("./testdata/tmp"))

	require.NoError(t, CreateDir("./testdata/tmp/"))
	require.DirExists(t, "./testdata/tmp")
	require.NoError(t, os.RemoveAll("./testdata/tmp"))
}

func TestOpenFile(t *testing.T) {
	testCases := []struct {
		description  string
		filePath     string
		permission   os.FileMode
		existingFile bool
	}{
		{
			description:  "opening an existing file",
			filePath:     "./testdata/dir/e",
			permission:   0644,
			existingFile: true,
		},
		{
			description:  "opening a new file",
			filePath:     "./testdata/new-file",
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
	tests := []struct {
		name         string
		filePath     string
		existingFile bool
		expectedErr  error
	}{
		{
			name:         "creating a new file",
			filePath:     "./testdata/new-file",
			existingFile: false,
			expectedErr:  nil,
		},
		{
			name:         "file already exist",
			filePath:     "./testdata/dir/e",
			existingFile: true,
			expectedErr:  fmt.Errorf("file [./testdata/dir/e] exist"),
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			switch tt.existingFile {
			case false:
				defer os.RemoveAll(tt.filePath)
			default:
				require.FileExists(t, tt.filePath)
			}

			require.Equal(t, CreateFile(tt.filePath), tt.expectedErr)
			require.FileExists(t, tt.filePath)
		})
	}
}

func TestRemove(t *testing.T) {
	var cleanup func()

	setup := func() {
		filePath := "./testdata/toberemovedfile"
		dirPath := "./testdata/toberemoveddir"
		require.NoError(t, CreateFile(filePath))
		require.NoError(t, CreateDir(dirPath))

		cleanup = func() {
			os.RemoveAll(filePath)
			os.RemoveAll(dirPath)
		}
	}

	var tests = []struct {
		name        string
		filePath    string
		errExpected bool
	}{
		{
			name:        "removing a file",
			filePath:    "./testdata/toberemovedfile",
			errExpected: false,
		},
		{
			name:        "removing an empty dir",
			filePath:    "./testdata/toberemoveddir",
			errExpected: false,
		},
		{
			name:        "removing a non-existing file",
			filePath:    "./testdata/doesnotexist",
			errExpected: true,
		},
		{
			name:        "removing an non-empty dir",
			filePath:    "./testdata/",
			errExpected: true,
		},
	}

	setup()
	defer cleanup()

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
	var cleanup func()

	setup := func() {
		require.NoError(t, CreateDir("./testdata/toberemoveddir"))
		require.NoError(t, CreateDir("./testdata/toberemoveddir/a"))
		require.NoError(t, CreateFile("./testdata/toberemoveddir/b"))

		cleanup = func() {
			os.RemoveAll("./testdata/toberemoveddir")
		}
	}

	var tests = []struct {
		name     string
		filePath string
	}{
		{
			name:     "removing an non-empty directory",
			filePath: "./testdata/toberemoveddir",
		},
		{
			name:     "removing a non-existing file",
			filePath: "./testdata/doesnotexist",
			// os.RemoveAll does not return error if file does not exist
		},
	}

	setup()
	defer cleanup()

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			require.NoError(t, RemoveAll(tt.filePath))
		})
	}
}

func TestWrite(t *testing.T) {
	var cleanup func()

	setup := func() (*os.File, *os.File) {
		contentFilePath := "./testdata/contentfile"
		contentFile, err := OpenFile(contentFilePath, 0644)
		require.NoError(t, err)
		contentFile.Write([]byte("hello"))

		emptyFilePath := "./testdata/emptyfile"
		emptyFile, err := OpenFile(emptyFilePath, 0644)
		require.NoError(t, err)

		cleanup = func() {
			contentFile.Close()
			emptyFile.Close()

			defer os.RemoveAll(contentFilePath)
			defer os.RemoveAll(emptyFilePath)
		}

		return contentFile, emptyFile
	}

	contentFile, emptyFile := setup()
	defer cleanup()

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
	var cleanup func()

	setup := func() *os.File {
		contentFilePath := "./testdata/contentfile"
		contentFile, err := OpenFile(contentFilePath, 0644)
		require.NoError(t, err)
		contentFile.Write([]byte("helloworld"))

		cleanup = func() {
			contentFile.Close()
			defer os.RemoveAll(contentFilePath)
		}

		return contentFile
	}

	contentFile := setup()
	defer cleanup()

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
	t.Run("green-path", func(t *testing.T) {
		testPath := "./testdata/dir"
		require.NoError(t, SyncDir(testPath))
	})

	t.Run("non-existent-dir", func(t *testing.T) {
		require.EqualError(
			t,
			SyncDir("non-existent-dir"),
			"error while opening dir:non-existent-dir: open non-existent-dir: no such file or directory",
		)
	})
}
