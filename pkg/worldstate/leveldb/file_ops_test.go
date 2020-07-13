package leveldb

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsDirEmpty(t *testing.T) {
	t.Run("non-empty directory", func(t *testing.T) {
		t.Parallel()
		isEmpty, err := isDirEmpty("./testdata/dir")
		require.NoError(t, err)
		require.False(t, isEmpty)
	})

	t.Run("empty directory", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, createDir("./testdata/z"))
		isEmpty, err := isDirEmpty("./testdata/z")
		require.NoError(t, err)
		require.True(t, isEmpty)
		require.NoError(t, os.RemoveAll("./testdata/z"))
	})

	t.Run("error case", func(t *testing.T) {
		t.Parallel()
		_, err := isDirEmpty("xx")
		require.Contains(t, err.Error(), "error opening dir [xx]")
	})
}

func TestListSubdirs(t *testing.T) {
	t.Run("subdirs exist", func(t *testing.T) {
		t.Parallel()
		dirs, err := listSubdirs("./testdata/dir")
		require.NoError(t, err)
		expectedDirs := []string{"a", "b", "c", "d"}
		require.Equal(t, expectedDirs, dirs)
	})

	t.Run("subdirs do not exist", func(t *testing.T) {
		t.Parallel()
		dirs, err := listSubdirs("./testdata/dir/a")
		require.NoError(t, err)
		require.Empty(t, dirs)
	})

	t.Run("error case", func(t *testing.T) {
		t.Parallel()
		_, err := listSubdirs("xx")
		require.Contains(t, err.Error(), "error reading dir [xx]")
	})
}

func TestFileExists(t *testing.T) {
	exists, err := fileExists("./testdata/dir")
	require.NoError(t, err)
	require.True(t, exists)

	exists, err = fileExists("./testdata/dir/e")
	require.NoError(t, err)
	require.True(t, exists)

	exists, err = fileExists("xx")
	require.NoError(t, err)
	require.False(t, exists)
}

func TestCreateDir(t *testing.T) {
	require.DirExists(t, "./testdata/dir")
	require.NoError(t, createDir("./testdata/dir"))

	require.NoError(t, os.RemoveAll("./testdata/tmp"))
	require.NoError(t, createDir("./testdata/tmp"))
	require.DirExists(t, "./testdata/tmp")
	require.NoError(t, os.RemoveAll("./testdata/tmp"))

	require.NoError(t, createDir("./testdata/tmp/"))
	require.DirExists(t, "./testdata/tmp")
	require.NoError(t, os.RemoveAll("./testdata/tmp"))
}
