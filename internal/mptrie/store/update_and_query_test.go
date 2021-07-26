package store

import (
	"encoding/binary"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/IBM-Blockchain/bcdb-server/internal/mptrie"
	"github.com/IBM-Blockchain/bcdb-server/pkg/crypto"
	"github.com/IBM-Blockchain/bcdb-server/pkg/logger"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
)

func TestPutAndPersist(t *testing.T) {
	lc := &logger.Config{
		Level:         "debug",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
	}
	logger, err := logger.New(lc)
	require.NoError(t, err)

	t.Run("only put", func(t *testing.T) {
		t.Parallel()

		testDir, err := ioutil.TempDir(".", "update_and_query_test")
		require.NoError(t, err)
		defer os.RemoveAll(testDir)

		storeDir := filepath.Join(testDir, "test-store1")
		c := &Config{
			StoreDir: storeDir,
			Logger:   logger,
		}
		s, err := Open(c)
		require.NoError(t, err)

		assertStore(t, storeDir, s)

		pointers := fillStore(t, s, false, 0, uint64(1))
		checkStoreContent(t, s, pointers, true, true, 0)
		checkStoreContent(t, s, pointers, true, false, 1000)
		invalidPointers := [][]byte{{0, 0, 0}, {1, 1, 1}, {2, 2, 2}, {3, 3, 3}}
		checkStoreContent(t, s, invalidPointers, false, false, 0)
	})

	t.Run("put and persist", func(t *testing.T) {
		t.Parallel()

		testDir, err := ioutil.TempDir(".", "update_and_query_test")
		require.NoError(t, err)
		defer os.RemoveAll(testDir)

		storeDir := filepath.Join(testDir, "test-store2")
		c := &Config{
			StoreDir: storeDir,
			Logger:   logger,
		}
		s, err := Open(c)
		require.NoError(t, err)

		assertStore(t, storeDir, s)

		pointers := fillStore(t, s, true, 0, uint64(1))
		checkStoreContent(t, s, pointers, true, true, 0)
		checkStoreContent(t, s, pointers, true, false, 1000)
		invalidPointers := [][]byte{{0, 0, 0}, {1, 1, 1}, {2, 2, 2}, {3, 3, 3}}
		checkStoreContent(t, s, invalidPointers, false, false, 0)
	})

	t.Run("put partial persist and clean memory", func(t *testing.T) {
		t.Parallel()

		testDir, err := ioutil.TempDir(".", "update_and_query_test")
		require.NoError(t, err)
		defer os.RemoveAll(testDir)

		storeDir := filepath.Join(testDir, "test-store3")
		c := &Config{
			StoreDir: storeDir,
			Logger:   logger,
		}
		s, err := Open(c)
		require.NoError(t, err)

		assertStore(t, storeDir, s)

		pointers := fillStore(t, s, false, 0, uint64(1))
		checkStoreContent(t, s, pointers, true, true, 0)

		for i, p := range pointers[:500] {
			switch i % 4 {
			case 3:
				isChanged, err := s.PersistValue(p)
				require.NoError(t, err)
				require.True(t, isChanged)
			default:
				isChanged, err := s.PersistNode(p)
				require.NoError(t, err)
				require.True(t, isChanged)
			}
		}

		require.NoError(t, s.CommitChanges(uint64(2)))
		for i, p := range pointers[:500] {
			switch i % 4 {
			case 3:
				isChanged, err := s.PersistValue(p)
				require.NoError(t, err)
				require.False(t, isChanged)
			default:
				isChanged, err := s.PersistNode(p)
				require.NoError(t, err)
				require.False(t, isChanged)
			}
		}

		checkStoreContent(t, s, pointers[:500], true, true, 0)
		checkStoreContent(t, s, pointers[500:], false, false, 0)
	})

	t.Run("put and persist - reopen store", func(t *testing.T) {
		t.Parallel()

		testDir, err := ioutil.TempDir(".", "update_and_query_test")
		require.NoError(t, err)
		defer os.RemoveAll(testDir)

		storeDir := filepath.Join(testDir, "test-store4")
		c := &Config{
			StoreDir: storeDir,
			Logger:   logger,
		}
		s, err := Open(c)
		require.NoError(t, err)

		assertStore(t, storeDir, s)

		pointers := fillStore(t, s, true, 0, uint64(2))
		checkStoreContent(t, s, pointers, true, true, 0)
		checkStoreContent(t, s, pointers, true, false, 1000)
		invalidPointers := [][]byte{{0, 0, 0}, {1, 1, 1}, {2, 2, 2}, {3, 3, 3}}
		checkStoreContent(t, s, invalidPointers, false, false, 0)
		s.Close()

		s, err = Open(c)
		require.NoError(t, err)

		assertStore(t, storeDir, s)
		checkStoreContent(t, s, pointers, true, true, 0)
		checkStoreContent(t, s, pointers, true, false, 1000)
		checkStoreContent(t, s, invalidPointers, false, false, 0)
	})
}

func fillStore(t *testing.T, s *Store, persist bool, nonce int, blockNum uint64) [][]byte {
	pointers := make([][]byte, 1000)
	for i := 0; i < 1000; i++ {
		numBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(numBytes, uint64(nonce+i))
		ptr, err := crypto.ComputeSHA256Hash(append(numBytes, numBytes...))
		require.NoError(t, err)
		require.NotNil(t, ptr)
		switch i % 4 {
		case 0:
			node := &mptrie.BranchNode{
				Children: make([][]byte, 16),
				ValuePtr: ptr,
				Deleted:  false,
			}
			require.NoError(t, s.PutNode(ptr, node))
			if persist {
				_, err := s.PersistNode(ptr)
				require.NoError(t, err)
			}
		case 1:
			node := &mptrie.ExtensionNode{
				Key:   numBytes,
				Child: nil,
			}
			require.NoError(t, s.PutNode(ptr, node))
			if persist {
				_, err := s.PersistNode(ptr)
				require.NoError(t, err)
			}
		case 2:
			node := &mptrie.ValueNode{
				Key:      numBytes,
				ValuePtr: ptr,
			}
			require.NoError(t, s.PutNode(ptr, node))
			if persist {
				_, err := s.PersistNode(ptr)
				require.NoError(t, err)
			}
		case 3:
			require.NoError(t, s.PutValue(ptr, numBytes))
			if persist {
				_, err := s.PersistValue(ptr)
				require.NoError(t, err)
			}
		}
		pointers[i] = ptr
	}
	if persist {
		s.CommitChanges(blockNum)
	}
	return pointers
}

func checkStoreContent(t *testing.T, s *Store, pointers [][]byte, isFound, isEqual bool, nonce int) {
	for i, p := range pointers {
		numBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(numBytes, uint64(i+nonce))
		ptr, err := crypto.ComputeSHA256Hash(append(numBytes, numBytes...))
		require.NoError(t, err)
		require.NotNil(t, ptr)
		switch i % 4 {
		case 0:
			node, err := s.GetNode(p)
			if isFound {
				require.NoError(t, err)
				expectedNode := &mptrie.BranchNode{
					Children: make([][]byte, 16),
					ValuePtr: ptr,
					Deleted:  false,
				}
				if isEqual {
					require.True(t, proto.Equal(node.(*mptrie.BranchNode), expectedNode))
				} else {
					require.False(t, proto.Equal(node.(*mptrie.BranchNode), expectedNode))
				}
			} else {
				require.Error(t, err)
			}
		case 1:
			node, err := s.GetNode(p)
			if isFound {
				require.NoError(t, err)
				expectedNode := &mptrie.ExtensionNode{
					Key:   numBytes,
					Child: nil,
				}
				if isEqual {
					require.True(t, proto.Equal(node.(*mptrie.ExtensionNode), expectedNode))
				} else {
					require.False(t, proto.Equal(node.(*mptrie.ExtensionNode), expectedNode))
				}
			} else {
				require.Error(t, err)
			}
		case 2:
			node, err := s.GetNode(p)
			if isFound {
				require.NoError(t, err)
				expectedNode := &mptrie.ValueNode{
					Key:      numBytes,
					ValuePtr: ptr,
				}
				if isEqual {
					require.True(t, proto.Equal(node.(*mptrie.ValueNode), expectedNode))
				} else {
					require.False(t, proto.Equal(node.(*mptrie.ValueNode), expectedNode))
				}
			} else {
				require.Error(t, err)
			}
		case 3:
			val, err := s.GetValue(p)
			if isFound {
				require.NoError(t, err)
				if isEqual {
					require.EqualValues(t, numBytes, val)
				} else {
					require.NotEqualValues(t, numBytes, val)
				}
			} else {
				require.Error(t, err)
			}
		}
	}
}
