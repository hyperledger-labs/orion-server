// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package mptrie

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

var keyNibbles = []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "A", "B", "C", "D", "E", "F"}

const (
	Branch    = 0x1
	Extension = 0x2
	Value     = 0x3
)

func TestNewTrie(t *testing.T) {
	trie, err := NewTrie(nil, newMockStore())
	require.NoError(t, err)
	require.NotNil(t, trie)
	_, ok := trie.root.(*BranchNode)
	require.True(t, ok)
	require.Len(t, trie.store.(*trieStoreMock).inMemoryNodes, 1)
	require.Len(t, trie.store.(*trieStoreMock).inMemoryValues, 0)
	err = trie.Commit(1)
	require.NoError(t, err)
	require.Len(t, trie.store.(*trieStoreMock).inMemoryNodes, 0)
	require.Len(t, trie.store.(*trieStoreMock).persistNodes, 1)
	require.Equal(t, uint64(1), trie.store.(*trieStoreMock).lastBlock)

	trie = nil
	hash, err := trie.Hash()
	assert.Nil(t, hash)
	assert.NoError(t, err)
}

func TestUpdateTrieStructure(t *testing.T) {
	keys := [][]byte{
		convertHexToKey(t, []byte("a123")),
		convertHexToKey(t, []byte("da1f")),
		convertHexToKey(t, []byte("50ff1a")),
		convertHexToKey(t, []byte("da1fe111")),
		convertHexToKey(t, []byte("da")),
		convertHexToKey(t, []byte("d1")),
		convertHexToKey(t, []byte("a111")),
		convertHexToKey(t, []byte("a112")),
		convertHexToKey(t, []byte("a212")),
		convertHexToKey(t, []byte("a1")),
		convertHexToKey(t, []byte("da1f")),
		convertHexToKey(t, []byte("da1fe1")),
		convertHexToKey(t, []byte("da1de1")),
		convertHexToKey(t, []byte("ada1df")),
		convertHexToKey(t, []byte("ada1de")),
		convertHexToKey(t, []byte("ada1")),
	}

	values := [][]byte{
		[]byte("B"),
		[]byte("D"),
		[]byte("G"),
		[]byte("F"),
		[]byte("H"),
		[]byte("H2"),
		[]byte("I"),
		[]byte("A"),
		[]byte("B"),
	}

	tests := []struct {
		name string
		data *testData
	}{
		{
			name: "one insert",
			data: &testData{
				op:     []string{"U"},
				keys:   [][]byte{keys[0]},
				values: [][]byte{values[0]},
				trieStat: &trieStatistic{
					branchNodesNum:    1,
					extensionNodesNum: 0,
					valueNodesNum:     1,
					valuesNum:         1,
				},
			},
		},
		{
			name: "three inserts, only value nodes",
			data: &testData{
				op:     []string{"U", "U", "U"},
				keys:   [][]byte{keys[0], keys[1], keys[2]},
				values: [][]byte{values[0], values[1], values[2]},
				trieStat: &trieStatistic{
					branchNodesNum:    1,
					extensionNodesNum: 0,
					valueNodesNum:     3,
					valuesNum:         3,
				},
			},
		},
		{
			name: "four inserts, extension with branch added",
			data: &testData{
				op:     []string{"U", "U", "U", "U"},
				keys:   [][]byte{keys[0], keys[1], keys[2], keys[3]},
				values: [][]byte{values[0], values[1], values[2], values[3]},
				trieStat: &trieStatistic{
					branchNodesNum:    2,
					extensionNodesNum: 1,
					valueNodesNum:     3,
					valuesNum:         4,
				},
			},
		},
		{
			name: "four inserts, one update, extension with branch added",
			data: &testData{
				op:     []string{"U", "U", "U", "U", "U"},
				keys:   [][]byte{keys[0], keys[1], keys[2], keys[3], keys[3]},
				values: [][]byte{values[0], values[1], values[2], values[3], values[4]},
				trieStat: &trieStatistic{
					branchNodesNum:    2,
					extensionNodesNum: 1,
					valueNodesNum:     3,
					valuesNum:         4,
				},
			},
		},
		{
			name: "five inserts, one more extensions with branch added",
			data: &testData{
				op:     []string{"U", "U", "U", "U", "U"},
				keys:   [][]byte{keys[0], keys[1], keys[2], keys[3], keys[4]},
				values: [][]byte{values[0], values[1], values[2], values[3], values[4]},
				trieStat: &trieStatistic{
					branchNodesNum:    3,
					extensionNodesNum: 2,
					valueNodesNum:     3,
					valuesNum:         5,
				},
			},
		},
		{
			name: "six inserts, one extension eliminated and one branch with value added",
			data: &testData{
				op:     []string{"U", "U", "U", "U", "U", "U"},
				keys:   [][]byte{keys[0], keys[1], keys[2], keys[3], keys[4], keys[5]},
				values: [][]byte{values[0], values[1], values[2], values[3], values[4], values[6]},
				trieStat: &trieStatistic{
					branchNodesNum:    4,
					extensionNodesNum: 1,
					valueNodesNum:     4,
					valuesNum:         6,
				},
			},
		},
		{
			name: "seven inserts, key ends at extension node",
			data: &testData{
				op:     []string{"U", "U", "U", "U", "U", "U", "U"},
				keys:   [][]byte{keys[0], keys[1], keys[2], keys[3], keys[4], keys[5], keys[10]},
				values: [][]byte{values[0], values[1], values[2], values[3], values[4], values[6], values[7]},
				trieStat: &trieStatistic{
					branchNodesNum:    4,
					extensionNodesNum: 1,
					valueNodesNum:     4,
					valuesNum:         6,
				},
			},
		},
		{
			name: "six inserts, one update, same statistic",
			data: &testData{
				op:     []string{"U", "U", "U", "U", "U", "U", "U"},
				keys:   [][]byte{keys[0], keys[1], keys[2], keys[3], keys[4], keys[5], keys[4]},
				values: [][]byte{values[0], values[1], values[2], values[3], values[4], values[6], values[5]},
				trieStat: &trieStatistic{
					branchNodesNum:    4,
					extensionNodesNum: 1,
					valueNodesNum:     4,
					valuesNum:         6,
				},
			},
		},
		{
			name: "two inserts, path to value node splitted by extension and branch node, new value node added",
			data: &testData{
				op:     []string{"U", "U"},
				keys:   [][]byte{keys[6], keys[7]},
				values: [][]byte{values[7], values[8]},
				trieStat: &trieStatistic{
					branchNodesNum:    2,
					extensionNodesNum: 1,
					valueNodesNum:     2,
					valuesNum:         2,
				},
			},
		},
		{
			name: "two inserts, path to value node splitted by branch node only, new value node added",
			data: &testData{
				op:     []string{"U", "U"},
				keys:   [][]byte{keys[6], keys[8]},
				values: [][]byte{values[7], values[8]},
				trieStat: &trieStatistic{
					branchNodesNum:    2,
					extensionNodesNum: 0,
					valueNodesNum:     2,
					valuesNum:         2,
				},
			},
		},
		{
			name: "two inserts, path to value node split by extension and branch node",
			data: &testData{
				op:     []string{"U", "U"},
				keys:   [][]byte{keys[6], keys[9]},
				values: [][]byte{values[7], values[8]},
				trieStat: &trieStatistic{
					branchNodesNum:    2,
					extensionNodesNum: 1,
					valueNodesNum:     1,
					valuesNum:         2,
				},
			},
		},
		{
			name: "three inserts, path to extension node splitted",
			data: &testData{
				op:     []string{"U", "U", "U"},
				keys:   [][]byte{keys[3], keys[11], keys[12]},
				values: [][]byte{values[0], values[1], values[2]},
				trieStat: &trieStatistic{
					branchNodesNum:    3,
					extensionNodesNum: 2,
					valueNodesNum:     2,
					valuesNum:         3,
				},
			},
		},
		{
			name: "three inserts, only value nodes, 3 deletes, two for exist keys",
			data: &testData{
				op:     []string{"U", "U", "U", "D", "D", "D"},
				keys:   [][]byte{keys[0], keys[1], keys[2], keys[1], keys[0], keys[4]},
				values: [][]byte{values[0], values[1], values[2], values[1], values[0], nil},
				trieStat: &trieStatistic{
					branchNodesNum:    1,
					extensionNodesNum: 0,
					valueNodesNum:     3,
					valuesNum:         3,
				},
			},
		},
		{
			name: "six inserts, one delete in branch node",
			data: &testData{
				op:     []string{"U", "U", "U", "U", "U", "U", "D"},
				keys:   [][]byte{keys[0], keys[1], keys[2], keys[3], keys[4], keys[5], keys[4]},
				values: [][]byte{values[0], values[1], values[2], values[3], values[4], values[6], values[4]},
				trieStat: &trieStatistic{
					branchNodesNum:    4,
					extensionNodesNum: 1,
					valueNodesNum:     4,
					valuesNum:         6,
				},
			},
		},
		{
			name: "three insters, branch after branch",
			data: &testData{
				op:     []string{"U", "U", "U"},
				keys:   [][]byte{keys[13], keys[14], keys[15]},
				values: [][]byte{values[0], values[1], values[2]},
				trieStat: &trieStatistic{
					branchNodesNum:    3,
					extensionNodesNum: 1,
					valueNodesNum:     2,
					valuesNum:         3,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newMockStore()
			trie, err := NewTrie(nil, store)
			require.NoError(t, err)
			require.NotNil(t, trie)

			resKeyValue := make(map[string][]byte)

			for i, key := range tt.data.keys {
				keyStr := base64.StdEncoding.EncodeToString(key)
				val := tt.data.values[i]
				if tt.data.op[i] == "U" {
					err = trie.Update(key, val)
					require.NoError(t, err)
					resKeyValue[keyStr] = val
					fmt.Print("AFTER INSERT ")
				} else if tt.data.op[i] == "D" {
					rVal, err := trie.Delete(key)
					require.NoError(t, err)
					require.Equal(t, val, rVal)
					resKeyValue[keyStr] = []byte("DELETED")
					fmt.Print("AFTER DELETE ")
				}
				for _, n := range convertByteToHex(key) {
					fmt.Print(keyNibbles[n])
				}
				fmt.Println("")
				printTrie(t, trie, 10)
			}
			validateTrieStatistic(t,
				trie,
				tt.data.trieStat.branchNodesNum,
				tt.data.trieStat.extensionNodesNum,
				tt.data.trieStat.valueNodesNum,
				tt.data.trieStat.valuesNum)
			validateTrie(t, trie.store, trie.root, true)
			keys := make([][]byte, 0)
			values := make([][]byte, 0)
			for keyStr, v := range resKeyValue {
				k, err := base64.StdEncoding.DecodeString(keyStr)
				require.NoError(t, err)
				keys = append(keys, k)
				values = append(values, v)
			}
			validateValues(t, trie, keys, values)
		})
	}
}

func TestGet(t *testing.T) {
	keys := [][]byte{
		convertHexToKey(t, []byte("11ada1df")),
		convertHexToKey(t, []byte("11ada1de")),
		convertHexToKey(t, []byte("11ada1")),
		convertHexToKey(t, []byte("11ada3")),
		convertHexToKey(t, []byte("11ad")),
	}
	values := [][]byte{
		[]byte("B"),
		[]byte("D"),
		[]byte("G"),
	}

	tests := []struct {
		name string
		data *testGetData
	}{
		{
			name: "get for extension node corner cases",
			data: &testGetData{
				insertKeys:   [][]byte{keys[0], keys[1], keys[2]},
				insertValues: [][]byte{values[0], values[1], values[2]},
				getKeys:      [][]byte{keys[0], keys[1], keys[2], keys[3], keys[4]},
				getValues:    [][]byte{values[0], values[1], values[2], nil, nil},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newMockStore()
			trie, err := NewTrie(nil, store)
			require.NoError(t, err)
			require.NotNil(t, trie)

			for i, key := range tt.data.insertKeys {
				err := trie.Update(key, tt.data.insertValues[i])
				require.NoError(t, err)
			}
			for i, key := range tt.data.getKeys {
				val, err := trie.Get(key)
				require.NoError(t, err)
				require.Equal(t, tt.data.getValues[i], val)
			}
		})
	}
}

type testGetData struct {
	insertKeys   [][]byte
	insertValues [][]byte
	getKeys      [][]byte
	getValues    [][]byte
}

type testData struct {
	op       []string
	keys     [][]byte
	values   [][]byte
	trieStat *trieStatistic
}

type trieStatistic struct {
	branchNodesNum    int
	extensionNodesNum int
	valueNodesNum     int
	valuesNum         int
}

type storeStatistic struct {
	inMemoryNodes  int
	persistNodes   int
	inMemoryValues int
	persistValues  int
}

func TestHistoryAccess(t *testing.T) {
	store := newMockStore()
	trie, err := NewTrie(nil, store)
	require.NoError(t, err)
	require.NotNil(t, trie)

	keys := [][]byte{
		convertHexToKey(t, []byte("a123")),
		convertHexToKey(t, []byte("da1f")),
		convertHexToKey(t, []byte("50ff1a")),
		convertHexToKey(t, []byte("da1fe111")),
		convertHexToKey(t, []byte("da")),
		convertHexToKey(t, []byte("d1")),
	}

	values := [][]byte{
		[]byte("B"),
		[]byte("D"),
		[]byte("G"),
		[]byte("F"),
		[]byte("H"),
		[]byte("H2"),
		[]byte("I"),
		[]byte("A"),
		[]byte("B"),
	}

	tests := []struct {
		name             string
		beforeCheckpoint *testData
		afterCheckpoint  *testData
	}{
		{
			name: "simple value update",
			beforeCheckpoint: &testData{
				keys:   [][]byte{keys[0]},
				values: [][]byte{values[0]},
				trieStat: &trieStatistic{
					branchNodesNum:    1,
					extensionNodesNum: 0,
					valueNodesNum:     1,
					valuesNum:         1,
				},
			},
			afterCheckpoint: &testData{
				keys:   [][]byte{keys[0]},
				values: [][]byte{values[1]},
				trieStat: &trieStatistic{
					branchNodesNum:    1,
					extensionNodesNum: 0,
					valueNodesNum:     1,
					valuesNum:         1,
				},
			},
		},
		{
			name: "one value before checkpoint, update and two inserts after, total 3 values",
			beforeCheckpoint: &testData{
				keys:   [][]byte{keys[0]},
				values: [][]byte{values[0]},
				trieStat: &trieStatistic{
					branchNodesNum:    1,
					extensionNodesNum: 0,
					valueNodesNum:     1,
					valuesNum:         1,
				},
			},
			afterCheckpoint: &testData{
				keys:   [][]byte{keys[1], keys[2], keys[0]},
				values: [][]byte{values[1], values[2], values[3]},
				trieStat: &trieStatistic{
					branchNodesNum:    1,
					extensionNodesNum: 0,
					valueNodesNum:     3,
					valuesNum:         3,
				},
			},
		},
		{
			name: "only inserts, three before checkpoint, three after, total six values",
			beforeCheckpoint: &testData{
				keys:   [][]byte{keys[1], keys[2], keys[0]},
				values: [][]byte{values[1], values[2], values[3]},
				trieStat: &trieStatistic{
					branchNodesNum:    1,
					extensionNodesNum: 0,
					valueNodesNum:     3,
					valuesNum:         3,
				},
			},
			afterCheckpoint: &testData{
				keys:   [][]byte{keys[3], keys[4], keys[5]},
				values: [][]byte{values[3], values[4], values[6]},
				trieStat: &trieStatistic{
					branchNodesNum:    4,
					extensionNodesNum: 1,
					valueNodesNum:     4,
					valuesNum:         6,
				},
			},
		},
		{
			name: "three inserts and one update before checkpoint, three insets and two updates after, total six values",
			beforeCheckpoint: &testData{
				keys:   [][]byte{keys[1], keys[2], keys[0], keys[2]},
				values: [][]byte{values[1], values[2], values[3], values[8]},
				trieStat: &trieStatistic{
					branchNodesNum:    1,
					extensionNodesNum: 0,
					valueNodesNum:     3,
					valuesNum:         3,
				},
			},
			afterCheckpoint: &testData{
				keys:   [][]byte{keys[0], keys[3], keys[4], keys[5], keys[4]},
				values: [][]byte{values[5], values[3], values[4], values[6], values[7]},
				trieStat: &trieStatistic{
					branchNodesNum:    4,
					extensionNodesNum: 1,
					valueNodesNum:     4,
					valuesNum:         6,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newMockStore()
			trie, err := NewTrie(nil, store)
			require.NoError(t, err)
			require.NotNil(t, trie)

			resKeyValueBeforeCheckpoint := make(map[string][]byte)

			for i, key := range tt.beforeCheckpoint.keys {
				keyStr := base64.StdEncoding.EncodeToString(key)
				val := tt.beforeCheckpoint.values[i]
				resKeyValueBeforeCheckpoint[keyStr] = val
				err = trie.Update(key, val)
				require.NoError(t, err)
			}
			checkpointHash, err := trie.Hash()
			require.NoError(t, err)
			require.NotNil(t, checkpointHash)

			resKeyValueAfterCheckpoint := make(map[string][]byte)
			for i, key := range tt.afterCheckpoint.keys {
				keyStr := base64.StdEncoding.EncodeToString(key)
				val := tt.afterCheckpoint.values[i]
				resKeyValueAfterCheckpoint[keyStr] = val
				err = trie.Update(key, val)
				require.NoError(t, err)
			}

			checkTrie(t,
				trie,
				tt.afterCheckpoint.trieStat.branchNodesNum,
				tt.afterCheckpoint.trieStat.extensionNodesNum,
				tt.afterCheckpoint.trieStat.valueNodesNum,
				tt.afterCheckpoint.trieStat.valuesNum,
				resKeyValueAfterCheckpoint)

			trieBeforeCheckpoint, err := NewTrie(checkpointHash, store)
			require.NoError(t, err)
			require.NotNil(t, trieBeforeCheckpoint)

			checkTrie(t,
				trieBeforeCheckpoint,
				tt.beforeCheckpoint.trieStat.branchNodesNum,
				tt.beforeCheckpoint.trieStat.extensionNodesNum,
				tt.beforeCheckpoint.trieStat.valueNodesNum,
				tt.beforeCheckpoint.trieStat.valuesNum,
				resKeyValueBeforeCheckpoint)

			// Check for non-exist before checkpoint keys
			for _, k := range tt.afterCheckpoint.keys {
				keyStr := base64.StdEncoding.EncodeToString(k)
				if _, ok := resKeyValueBeforeCheckpoint[keyStr]; !ok {
					v, err := trieBeforeCheckpoint.Get(k)
					require.NoError(t, err)
					require.Nil(t, v)
				}
			}
		})
	}
}

func TestTrieCommit(t *testing.T) {
	store := newMockStore()
	trie, err := NewTrie(nil, store)
	require.NoError(t, err)
	require.NotNil(t, trie)

	keys := [][]byte{
		convertHexToKey(t, []byte("a123")),
		convertHexToKey(t, []byte("da1f")),
		convertHexToKey(t, []byte("50ff1a")),
		convertHexToKey(t, []byte("da1fe111")),
		convertHexToKey(t, []byte("da")),
		convertHexToKey(t, []byte("d1")),
		convertHexToKey(t, []byte("da1fe1")),
		convertHexToKey(t, []byte("da1de1")),
	}

	values := [][]byte{
		[]byte("B"),
		[]byte("D"),
		[]byte("G"),
		[]byte("F"),
		[]byte("H"),
		[]byte("H2"),
		[]byte("I"),
		[]byte("A"),
		[]byte("B"),
	}

	tests := []struct {
		name                       string
		firstUpdateSeq             *testData
		afterFirstCommitStoreStat  *storeStatistic
		secondUpdateSeq            *testData
		afterUpdateStoreStat       *storeStatistic
		afterSecondCommitStoreStat *storeStatistic
	}{
		{
			name: "first update insert one value, second update and two inserts after, total 3 values",
			firstUpdateSeq: &testData{
				keys:   [][]byte{keys[0]},
				values: [][]byte{values[0]},
				trieStat: &trieStatistic{
					branchNodesNum:    1,
					extensionNodesNum: 0,
					valueNodesNum:     1,
					valuesNum:         1,
				},
			},
			afterFirstCommitStoreStat: &storeStatistic{
				inMemoryNodes:  0,
				persistNodes:   2,
				inMemoryValues: 0,
				persistValues:  1,
			},
			secondUpdateSeq: &testData{
				keys:   [][]byte{keys[1], keys[2], keys[0]},
				values: [][]byte{values[1], values[2], values[3]},
				trieStat: &trieStatistic{
					branchNodesNum:    1,
					extensionNodesNum: 0,
					valueNodesNum:     3,
					valuesNum:         3,
				},
			},
			afterUpdateStoreStat: &storeStatistic{
				inMemoryNodes:  6,
				persistNodes:   2,
				inMemoryValues: 3,
				persistValues:  1,
			},
			afterSecondCommitStoreStat: &storeStatistic{
				inMemoryNodes:  0,
				persistNodes:   6,
				inMemoryValues: 0,
				persistValues:  4,
			},
		},
		{
			name: "one insert first, two inserts after, all node types created",
			firstUpdateSeq: &testData{
				keys:   [][]byte{keys[3]},
				values: [][]byte{values[0]},
				trieStat: &trieStatistic{
					branchNodesNum:    1,
					extensionNodesNum: 0,
					valueNodesNum:     1,
					valuesNum:         1,
				},
			},
			afterFirstCommitStoreStat: &storeStatistic{
				inMemoryNodes:  0,
				persistNodes:   2,
				inMemoryValues: 0,
				persistValues:  1,
			},
			secondUpdateSeq: &testData{
				keys:   [][]byte{keys[6], keys[7]},
				values: [][]byte{values[1], values[2]},
				trieStat: &trieStatistic{
					branchNodesNum:    3,
					extensionNodesNum: 2,
					valueNodesNum:     2,
					valuesNum:         3,
				},
			},
			afterUpdateStoreStat: &storeStatistic{
				inMemoryNodes:  9,
				persistNodes:   2,
				inMemoryValues: 2,
				persistValues:  1,
			},
			afterSecondCommitStoreStat: &storeStatistic{
				inMemoryNodes:  0,
				persistNodes:   9,
				inMemoryValues: 0,
				persistValues:  3,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newMockStore()
			trie, err := NewTrie(nil, store)
			require.NoError(t, err)
			require.NotNil(t, trie)

			resKeyValueBeforeCheckpoint := make(map[string][]byte)

			for i, key := range tt.firstUpdateSeq.keys {
				keyStr := base64.StdEncoding.EncodeToString(key)
				val := tt.firstUpdateSeq.values[i]
				resKeyValueBeforeCheckpoint[keyStr] = val
				err = trie.Update(key, val)
				require.NoError(t, err)
			}
			trie.Commit(1)
			stat := &storeStatistic{}
			stat.inMemoryNodes, stat.persistNodes, stat.inMemoryValues, stat.persistValues = trie.store.(*trieStoreMock).storeStatistic()
			require.Equal(t, tt.afterFirstCommitStoreStat, stat)
			require.Equal(t, uint64(1), trie.store.(*trieStoreMock).lastBlock)
			fmt.Println("BEFORE CHECKPOINT")
			printTrie(t, trie, 10)
			checkTrie(t,
				trie,
				tt.firstUpdateSeq.trieStat.branchNodesNum,
				tt.firstUpdateSeq.trieStat.extensionNodesNum,
				tt.firstUpdateSeq.trieStat.valueNodesNum,
				tt.firstUpdateSeq.trieStat.valuesNum,
				resKeyValueBeforeCheckpoint)

			resKeyValueAfterCheckpoint := make(map[string][]byte)
			for i, key := range tt.secondUpdateSeq.keys {
				keyStr := base64.StdEncoding.EncodeToString(key)
				val := tt.secondUpdateSeq.values[i]
				resKeyValueAfterCheckpoint[keyStr] = val
				err = trie.Update(key, val)
				require.NoError(t, err)
			}

			stat.inMemoryNodes, stat.persistNodes, stat.inMemoryValues, stat.persistValues = trie.store.(*trieStoreMock).storeStatistic()
			require.Equal(t, tt.afterUpdateStoreStat, stat)
			trie.Commit(2)
			stat.inMemoryNodes, stat.persistNodes, stat.inMemoryValues, stat.persistValues = trie.store.(*trieStoreMock).storeStatistic()
			require.Equal(t, tt.afterSecondCommitStoreStat, stat)
			require.Equal(t, uint64(2), trie.store.(*trieStoreMock).lastBlock)

			fmt.Println("AFTER CHECKPOINT")
			printTrie(t, trie, 10)
			checkTrie(t,
				trie,
				tt.secondUpdateSeq.trieStat.branchNodesNum,
				tt.secondUpdateSeq.trieStat.extensionNodesNum,
				tt.secondUpdateSeq.trieStat.valueNodesNum,
				tt.secondUpdateSeq.trieStat.valuesNum,
				resKeyValueAfterCheckpoint)
		})
	}
}

func validateValues(t *testing.T, trie *MPTrie, keys [][]byte, values [][]byte) {
	for i := range keys {
		k := keys[i]
		v := values[i]
		val, err := trie.Get(k)
		require.NoError(t, err)
		if string(v) != "DELETED" {
			require.EqualValues(t, v, val)
		} else {
			require.Nil(t, val)
		}
	}
}

func validateTrie(t *testing.T, store Store, root TrieNode, isRoot bool) {
	switch root.(type) {
	case *BranchNode:
		var childrenCount int
		for _, childPtr := range root.(*BranchNode).Children {
			if childPtr != nil {
				childrenCount++
				childNode, err := store.GetNode(childPtr)
				require.NoError(t, err)
				require.NotNil(t, childNode)
				validateTrie(t, store, childNode, false)
			}
		}
		if root.(*BranchNode).ValuePtr != nil {
			val, err := store.GetValue(root.(*BranchNode).ValuePtr)
			require.NoError(t, err)
			require.NotNil(t, val)
			require.GreaterOrEqual(t, childrenCount, 1)
		} else if !isRoot {
			require.GreaterOrEqual(t, childrenCount, 2)
		}
	case *ExtensionNode:
		require.NotNil(t, root.(*ExtensionNode).Child)
		require.NotNil(t, root.(*ExtensionNode).Key)
		childPtr := root.(*ExtensionNode).Child
		childNode, err := store.GetNode(childPtr)
		require.NoError(t, err)
		require.NotNil(t, childNode)
		validateTrie(t, store, childNode, false)
	case *ValueNode:
		require.NotNil(t, root.(*ValueNode).ValuePtr)
		val, err := store.GetValue(root.(*ValueNode).ValuePtr)
		require.NoError(t, err)
		require.NotNil(t, val)
	default:
		require.Fail(t, "Invalid node type")
	}
}

func validateTrieStatistic(t *testing.T, trie *MPTrie, branchNodesNum, extensionNodesNum, valueNodesNum, valuesNum int) {
	b1, e1, v1, vv1, err := trie.getStatistic(trie.root)
	require.NoError(t, err)
	require.Equal(t, branchNodesNum, b1)
	require.Equal(t, extensionNodesNum, e1)
	require.Equal(t, valueNodesNum, v1)
	require.Equal(t, valuesNum, vv1)

}

type trieStoreMock struct {
	inMemoryNodes  map[string][]byte
	inMemoryValues map[string][]byte
	persistNodes   map[string][]byte
	persistValues  map[string][]byte
	lastBlock      uint64
}

type nodeBytesWithType struct {
	nodeBytes []byte
	name      string
}

func newMockStore() Store {
	return &trieStoreMock{
		inMemoryNodes:  make(map[string][]byte),
		inMemoryValues: make(map[string][]byte),
		persistNodes:   make(map[string][]byte),
		persistValues:  make(map[string][]byte),
	}
}

func (s *trieStoreMock) IsDisabled() bool {
	return false
}

func (s *trieStoreMock) SetDisabled(bool) {
}

func (s *trieStoreMock) GetNode(nodePtr []byte) (TrieNode, error) {
	key := base64.StdEncoding.EncodeToString(nodePtr)
	nodeBytes, ok := s.persistNodes[key]
	if !ok {
		nodeBytes, ok = s.inMemoryNodes[key]
		if !ok {
			return nil, errors.Errorf("Node with key %s not found", key)
		}
	}
	var err error
	nodeTypePrefix := nodeBytes[0]
	switch nodeTypePrefix {
	case Branch:
		branchNode := &BranchNode{
			Children: make([][]byte, 16),
		}
		err = proto.Unmarshal(nodeBytes[1:], branchNode)
		if err != nil {
			return nil, err
		}
		return cleanBranchNode(branchNode), nil
	case Extension:
		extensionNode := &ExtensionNode{}
		err = proto.Unmarshal(nodeBytes[1:], extensionNode)
		if err != nil {
			return nil, err
		}
		if len(extensionNode.Child) == 0 {
			extensionNode.Child = nil
		}
		if len(extensionNode.Key) == 0 {
			extensionNode.Key = nil
		}
		return extensionNode, nil
	case Value:
		valueNode := &ValueNode{}
		err = proto.Unmarshal(nodeBytes[1:], valueNode)
		if err != nil {
			return nil, err
		}
		return valueNode, nil
	}
	return nil, errors.New("unknown type")
}

func (s *trieStoreMock) GetValue(valuePtr []byte) ([]byte, error) {
	key := base64.StdEncoding.EncodeToString(valuePtr)
	valueBytes, ok := s.persistValues[key]
	if !ok {
		valueBytes = s.inMemoryValues[key]
	}
	return valueBytes, nil
}

func (s *trieStoreMock) PutNode(nodePtr []byte, node TrieNode) error {
	key := base64.StdEncoding.EncodeToString(nodePtr)
	var nb []byte
	var err error

	switch node.(type) {
	case *BranchNode:
		nb, err = proto.Marshal(node.(*BranchNode))
		if err != nil {
			return err
		}
		nb = append([]byte{Branch}, nb...)
	case *ExtensionNode:
		nb, err = proto.Marshal(node.(*ExtensionNode))
		if err != nil {
			return err
		}
		nb = append([]byte{Extension}, nb...)
	case *ValueNode:
		nb, err = proto.Marshal(node.(*ValueNode))
		if err != nil {
			return err
		}
		nb = append([]byte{Value}, nb...)
	}
	s.inMemoryNodes[key] = nb
	return nil
}

func (s *trieStoreMock) PutValue(valuePtr, value []byte) error {
	key := base64.StdEncoding.EncodeToString(valuePtr)
	s.inMemoryValues[key] = value
	return nil
}

func (s *trieStoreMock) PersistNode(nodePtr []byte) (bool, error) {
	key := base64.StdEncoding.EncodeToString(nodePtr)
	nb, ok := s.inMemoryNodes[key]
	if ok {
		s.persistNodes[key] = nb
		return true, nil
	}
	return false, nil
}

func (s *trieStoreMock) PersistValue(valuePtr []byte) (bool, error) {
	key := base64.StdEncoding.EncodeToString(valuePtr)
	vb, ok := s.inMemoryValues[key]
	if ok {
		s.persistValues[key] = vb
		return true, nil
	}
	return false, nil
}

func (s *trieStoreMock) CommitChanges(blockNum uint64) error {
	s.lastBlock = blockNum
	s.inMemoryNodes = make(map[string][]byte)
	s.inMemoryValues = make(map[string][]byte)
	return nil
}

func (s *trieStoreMock) RollbackChanges() error {
	return nil
}

func (s *trieStoreMock) Height() (uint64, error) {
	return s.lastBlock, nil
}

func (s *trieStoreMock) storeStatistic() (inMemoryNodes, persistNodes, inMemoryValues, persistValues int) {
	return len(s.inMemoryNodes), len(s.persistNodes), len(s.inMemoryValues), len(s.persistValues)
}

func convertHexToKey(t *testing.T, hexKey []byte) []byte {
	res := make([]byte, hex.DecodedLen(len(hexKey)))
	n, err := hex.Decode(res, hexKey)
	require.NoError(t, err)

	return res[:n]
}

func (t *MPTrie) getStatistic(n TrieNode) (branchNodeNum int, extensionNodeNum int, valueNodeNum int, valueNum int, err error) {
	switch n.(type) {
	case *BranchNode:
		var currBranchNodeNum, currExtensionNodeNum, currValueNodeNum, currValueNum int
		branchNodeNum = 1
		if n.(*BranchNode).ValuePtr != nil {
			valueNum = 1
		}
		for _, childPtr := range n.(*BranchNode).Children {
			if childPtr != nil {
				child, err := t.store.GetNode(childPtr)
				if err != nil {
					return 0, 0, 0, 0, err
				}
				currBranchNodeNum, currExtensionNodeNum, currValueNodeNum, currValueNum, err = t.getStatistic(child)
				branchNodeNum += currBranchNodeNum
				extensionNodeNum += currExtensionNodeNum
				valueNodeNum += currValueNodeNum
				valueNum += currValueNum
			}
		}
		return branchNodeNum, extensionNodeNum, valueNodeNum, valueNum, nil
	case *ExtensionNode:
		child, err := t.store.GetNode(n.(*ExtensionNode).Child)
		if err != nil {
			return 0, 0, 0, 0, err
		}
		branchNodeNum, extensionNodeNum, valueNodeNum, valueNum, err = t.getStatistic(child)
		if err != nil {
			return 0, 0, 0, 0, err
		}
		extensionNodeNum++
		return branchNodeNum, extensionNodeNum, valueNodeNum, valueNum, nil
	case *ValueNode:
		return 0, 0, 1, 1, nil
	default:
		return 0, 0, 0, 0, errors.New("invalid node type")
	}
}

var triePrintStrings []string

func (t *MPTrie) printTrie(n TrieNode, depth, ident int) error {
	for i := len(triePrintStrings[3*depth]); i < ident; i++ {
		for j := 0; j < 3; j++ {
			triePrintStrings[3*depth+j] += " "
		}
	}
	currIdent := len(triePrintStrings[3*depth])
	switch n.(type) {
	case *BranchNode:
		bn := n.(*BranchNode)

		var value []byte
		var err error
		if bn.ValuePtr != nil {
			value, err = t.store.GetValue(bn.ValuePtr)
			if err != nil {
				return err
			}
		}
		triePrintStrings[3*depth] += " |      Branch            |"
		triePrintStrings[3*depth+1] += " |0123456789ABCDEF Value D|"

		resStr := " |"
		for _, childPtr := range n.(*BranchNode).Children {
			if childPtr != nil {
				resStr += "*"
			} else {
				resStr += " "
			}
		}
		resStr += fmt.Sprintf(" %5.5s", string(value))
		if bn.isDeleted() {
			resStr += " *|"
		} else {
			resStr += "  |"
		}
		triePrintStrings[3*depth+2] += resStr

		for _, childPtr := range n.(*BranchNode).Children {
			if childPtr != nil {
				child, err := t.store.GetNode(childPtr)
				if err != nil {
					return err
				}
				err = t.printTrie(child, depth+1, currIdent)
				if err != nil {
					return err
				}
			}
		}
	case *ExtensionNode:
		child, err := t.store.GetNode(n.(*ExtensionNode).Child)
		if err != nil {
			return err
		}
		nodeLen := len(n.(*ExtensionNode).Key)
		if nodeLen < 9 {
			nodeLen = 9
		}
		resStr := " |Extension"
		for i := 9; i < nodeLen; i++ {
			resStr += " "
		}
		resStr += "|"
		triePrintStrings[3*depth] += resStr
		resStr = " |Key"
		for i := 3; i < nodeLen; i++ {
			resStr += " "
		}
		resStr += "|"
		triePrintStrings[3*depth+1] += resStr
		resStr = " |"
		for _, nibble := range n.(*ExtensionNode).Key {
			resStr += keyNibbles[nibble]
		}
		for i := len(n.(*ExtensionNode).Key); i < nodeLen; i++ {
			resStr += " "
		}
		resStr += "|"
		triePrintStrings[3*depth+2] += resStr

		err = t.printTrie(child, depth+1, currIdent)
		if err != nil {
			return err
		}
	case *ValueNode:
		nodeLenKey := len(n.(*ValueNode).Key)
		if nodeLenKey < 3 {
			nodeLenKey = 3
		}

		nodeLen := nodeLenKey + 6
		resStr := " |Value"
		for i := 5; i < nodeLen; i++ {
			resStr += " "
		}
		resStr += "|"
		triePrintStrings[3*depth] += resStr
		resStr = " |Key"
		for i := 3; i < nodeLenKey; i++ {
			resStr += " "
		}

		resStr += " Val D|"
		triePrintStrings[3*depth+1] += resStr
		resStr = " |"
		for _, nibble := range n.(*ValueNode).Key {
			resStr += keyNibbles[nibble]
		}

		for i := len(n.(*ValueNode).Key); i < nodeLenKey; i++ {
			resStr += " "
		}

		var value []byte
		var err error
		value, err = t.store.GetValue(n.(*ValueNode).ValuePtr)
		if err != nil {
			return err
		}

		resStr += fmt.Sprintf(" %3.3s", string(value))
		if n.(*ValueNode).isDeleted() {
			resStr += " *|"
		} else {
			resStr += "  |"
		}
		triePrintStrings[3*depth+2] += resStr
	default:
		return errors.New("invalid node type")
	}
	return nil
}

func checkTrie(t *testing.T, trie *MPTrie, branchNodesNum, extensionNodesNum, valueNodesNum, valuesNum int, keysAndValues map[string][]byte) {
	validateTrieStatistic(t, trie, branchNodesNum, extensionNodesNum, valueNodesNum, valuesNum)
	validateTrie(t, trie.store, trie.root, true)
	keys := make([][]byte, 0)
	values := make([][]byte, 0)
	for keyStr, v := range keysAndValues {
		k, err := base64.StdEncoding.DecodeString(keyStr)
		require.NoError(t, err)
		keys = append(keys, k)
		values = append(values, v)
	}
	validateValues(t, trie, keys, values)
}

func printTrie(t *testing.T, trie *MPTrie, maxDepth int) {
	triePrintStrings = make([]string, maxDepth*3)
	for i := 0; i < maxDepth*3; i++ {
		triePrintStrings[i] = ""
	}
	require.NoError(t, trie.printTrie(trie.root, 0, 0))

	for i, line := range triePrintStrings {
		if len(line) == 0 {
			continue
		}
		if i%3 == 0 {
			fmt.Println(" ")
		}
		fmt.Println(line)
	}
}

func cleanBranchNode(node *BranchNode) *BranchNode {
	for i, c := range node.Children {
		if c != nil && len(c) == 0 {
			node.Children[i] = nil
		}
	}
	return node
}
