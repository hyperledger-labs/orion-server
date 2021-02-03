package provenance

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/server/pkg/logger"
	"github.ibm.com/blockchaindb/server/pkg/types"
)

type testEnv struct {
	storeDir string
	s        *Store
	cleanup  func()
}

func newTestEnv(t *testing.T) *testEnv {
	storeDir, err := ioutil.TempDir("", "provenance")
	require.NoError(t, err)

	lc := &logger.Config{
		Level:         "debug",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
	}
	lggr, err := logger.New(lc)
	require.NoError(t, err)

	c := &Config{
		StoreDir: storeDir,
		Logger:   lggr,
	}

	store, err := Open(c)
	if err != nil {
		if rmErr := os.RemoveAll(storeDir); rmErr != nil {
			t.Errorf("error while removing directory %s, %v", storeDir, rmErr)
		}

		t.Fatalf("error while opening store on path %s, %v", storeDir, err)
	}

	return &testEnv{
		storeDir: storeDir,
		s:        store,
		cleanup: func() {
			if err := store.Close(); err != nil {
				t.Errorf("error while closing the store %s, %v", storeDir, err)
			}

			if err := os.RemoveAll(storeDir); err != nil {
				t.Fatalf("error while removing directory %s, %v", storeDir, err)
			}
		},
	}
}

func setup(t *testing.T, s *Store) {
	block1TxsData := []*TxDataForProvenance{
		{
			IsValid: true,
			DBName:  "db1",
			UserID:  "user1",
			TxID:    "tx1",
			Writes: []*types.KVWithMetadata{
				{
					Key:   "key1",
					Value: []byte("value1"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 1,
							TxNum:    0,
						},
					},
				},
			},
		},
		{
			IsValid: true,
			DBName:  "db1",
			UserID:  "user1",
			TxID:    "tx2",
			Writes: []*types.KVWithMetadata{
				{
					Key:   "key2",
					Value: []byte("value1"),
					Metadata: &types.Metadata{
						AccessControl: &types.AccessControl{
							ReadWriteUsers: map[string]bool{
								"user1": true,
								"user2": true,
							},
						},
						Version: &types.Version{
							BlockNum: 1,
							TxNum:    1,
						},
					},
				},
			},
		},
		{
			IsValid: false,
			TxID:    "tx10",
		},
	}

	block2TxsData := []*TxDataForProvenance{
		{
			IsValid: true,
			DBName:  "db1",
			UserID:  "user1",
			TxID:    "tx3",
			Reads: []*KeyWithVersion{
				{
					Key: "key1",
					Version: &types.Version{
						BlockNum: 1,
						TxNum:    0,
					},
				},
			},
			Writes: []*types.KVWithMetadata{
				{
					Key:   "key1",
					Value: []byte("value2"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 2,
							TxNum:    0,
						},
					},
				},
			},
			OldVersionOfWrites: map[string]*types.Version{
				"key1": {
					BlockNum: 1,
					TxNum:    0,
				},
			},
		},
		{
			IsValid: true,
			DBName:  "db1",
			UserID:  "user2",
			TxID:    "tx4",
			Reads: []*KeyWithVersion{
				{
					Key: "key2",
					Version: &types.Version{
						BlockNum: 1,
						TxNum:    1,
					},
				},
			},
			Writes: []*types.KVWithMetadata{
				{
					Key:   "key1",
					Value: []byte("value3"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 2,
							TxNum:    1,
						},
					},
				},
			},
			OldVersionOfWrites: map[string]*types.Version{
				"key1": {
					BlockNum: 2,
					TxNum:    0,
				},
			},
		},
	}

	block3TxsData := []*TxDataForProvenance{
		{
			IsValid: true,
			DBName:  "db1",
			UserID:  "user2",
			TxID:    "tx5",
			Reads: []*KeyWithVersion{
				{
					Key: "key1",
					Version: &types.Version{
						BlockNum: 2,
						TxNum:    1,
					},
				},
				{
					Key: "key2",
					Version: &types.Version{
						BlockNum: 1,
						TxNum:    1,
					},
				},
			},
			Writes: []*types.KVWithMetadata{
				{
					Key:   "key1",
					Value: []byte("value4"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 3,
							TxNum:    0,
						},
					},
				},
				{
					Key:   "key2",
					Value: []byte("value2"),
					Metadata: &types.Metadata{
						AccessControl: &types.AccessControl{
							ReadWriteUsers: map[string]bool{
								"user1": true,
								"user2": true,
							},
						},
						Version: &types.Version{
							BlockNum: 3,
							TxNum:    0,
						},
					},
				},
			},
			OldVersionOfWrites: map[string]*types.Version{
				"key1": {
					BlockNum: 2,
					TxNum:    1,
				},
				"key2": {
					BlockNum: 1,
					TxNum:    1,
				},
			},
		},
	}

	block4TxsData := []*TxDataForProvenance{
		{
			IsValid: true,
			DBName:  "db1",
			UserID:  "user2",
			TxID:    "tx50",
			Deletes: map[string]*types.Version{
				"key1": {
					BlockNum: 3,
					TxNum:    0,
				},
				"key3": {
					BlockNum: 1,
					TxNum:    1,
				},
			},
		},
	}

	block5TxsData := []*TxDataForProvenance{
		{
			IsValid: true,
			DBName:  "db1",
			UserID:  "user2",
			TxID:    "tx6",
			Writes: []*types.KVWithMetadata{
				{
					Key:   "key1",
					Value: []byte("value5"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 4,
							TxNum:    0,
						},
					},
				},
			},
			OldVersionOfWrites: map[string]*types.Version{
				"key1": {
					BlockNum: 3,
					TxNum:    0,
				},
			},
		},
	}

	block6TxsData := []*TxDataForProvenance{
		{
			IsValid: true,
			DBName:  "db1",
			UserID:  "user2",
			TxID:    "tx50",
			Deletes: map[string]*types.Version{
				"key1": {
					BlockNum: 4,
					TxNum:    0,
				},
			},
		},
	}

	require.NoError(t, s.Commit(1, block1TxsData))
	require.NoError(t, s.Commit(2, block2TxsData))
	require.NoError(t, s.Commit(3, block3TxsData))
	require.NoError(t, s.Commit(4, block4TxsData))
	require.NoError(t, s.Commit(5, block5TxsData))
	require.NoError(t, s.Commit(6, block6TxsData))
}

func TestGetValueAt(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	defer env.cleanup()

	setup(t, env.s)

	tests := []struct {
		name          string
		dbName        string
		key           string
		version       *types.Version
		expectedValue *types.ValueWithMetadata
	}{
		{
			name:   "fetch second value of key1",
			dbName: "db1",
			key:    "key1",
			version: &types.Version{
				BlockNum: 2,
				TxNum:    0,
			},
			expectedValue: &types.ValueWithMetadata{
				Value: []byte("value2"),
				Metadata: &types.Metadata{
					Version: &types.Version{
						BlockNum: 2,
						TxNum:    0,
					},
				},
			},
		},
		{
			name:   "fetch first value of key2",
			dbName: "db1",
			key:    "key2",
			version: &types.Version{
				BlockNum: 1,
				TxNum:    1,
			},
			expectedValue: &types.ValueWithMetadata{
				Value: []byte("value1"),
				Metadata: &types.Metadata{
					AccessControl: &types.AccessControl{
						ReadWriteUsers: map[string]bool{
							"user1": true,
							"user2": true,
						},
					},
					Version: &types.Version{
						BlockNum: 1,
						TxNum:    1,
					},
				},
			},
		},
		{
			name:   "fetch non-existing key",
			dbName: "db1",
			key:    "key3",
			version: &types.Version{
				BlockNum: 1,
				TxNum:    1,
			},
			expectedValue: nil,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			value, err := env.s.GetValueAt(tt.dbName, tt.key, tt.version)
			require.NoError(t, err)
			require.Equal(t, tt.expectedValue, value)
		})
	}
}

func TestGetValues(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	defer env.cleanup()

	setup(t, env.s)

	tests := []struct {
		name           string
		dbName         string
		key            string
		expectedValues []*types.ValueWithMetadata
	}{
		{
			name:   "fetch all values of key1",
			dbName: "db1",
			key:    "key1",
			expectedValues: []*types.ValueWithMetadata{
				{
					Value: []byte("value1"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 1,
							TxNum:    0,
						},
					},
				},
				{
					Value: []byte("value2"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 2,
							TxNum:    0,
						},
					},
				},
				{
					Value: []byte("value3"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 2,
							TxNum:    1,
						},
					},
				},
				{
					Value: []byte("value4"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 3,
							TxNum:    0,
						},
					},
				},
				{
					Value: []byte("value5"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 4,
							TxNum:    0,
						},
					},
				},
			},
		},
		{
			name:   "fetch all values of key2",
			dbName: "db1",
			key:    "key2",
			expectedValues: []*types.ValueWithMetadata{
				{
					Value: []byte("value1"),
					Metadata: &types.Metadata{
						AccessControl: &types.AccessControl{
							ReadWriteUsers: map[string]bool{
								"user1": true,
								"user2": true,
							},
						},
						Version: &types.Version{
							BlockNum: 1,
							TxNum:    1,
						},
					},
				},
				{
					Value: []byte("value2"),
					Metadata: &types.Metadata{
						AccessControl: &types.AccessControl{
							ReadWriteUsers: map[string]bool{
								"user1": true,
								"user2": true,
							},
						},
						Version: &types.Version{
							BlockNum: 3,
							TxNum:    0,
						},
					},
				},
			},
		},
		{
			name:           "fetch non-existing value",
			dbName:         "db1",
			key:            "key3",
			expectedValues: nil,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			value, err := env.s.GetValues(tt.dbName, tt.key)
			require.NoError(t, err)
			require.ElementsMatch(t, tt.expectedValues, value)
		})
	}
}

func TestGetTxSubmittedByUser(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	defer env.cleanup()

	setup(t, env.s)

	tests := []struct {
		name          string
		userID        string
		expectedTxIDs []string
	}{
		{
			name:          "fetch ids of tx submitted by user1",
			userID:        "user1",
			expectedTxIDs: []string{"tx1", "tx2", "tx3"},
		},
		{
			name:          "fetch ids of tx submitted by user2",
			userID:        "user2",
			expectedTxIDs: []string{"tx4", "tx5", "tx50", "tx6"},
		},
		{
			name:          "fetch non-existing transaction",
			userID:        "user3",
			expectedTxIDs: nil,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {

			txIDs, err := env.s.GetTxIDsSubmittedByUser(tt.userID)
			require.NoError(t, err)
			require.ElementsMatch(t, tt.expectedTxIDs, txIDs)
		})
	}
}

func TestGetReaders(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	defer env.cleanup()

	setup(t, env.s)

	tests := []struct {
		name            string
		dbName          string
		key             string
		expectedReaders map[string]uint32
	}{
		{
			name:   "fetch users who have read key1",
			dbName: "db1",
			key:    "key1",
			expectedReaders: map[string]uint32{
				"user1": 1,
				"user2": 1,
			},
		},
		{
			name:   "fetch users who have read key2",
			dbName: "db1",
			key:    "key2",
			expectedReaders: map[string]uint32{
				"user2": 2,
			},
		},
		{
			name:            "fetch users who have read non-existing key",
			dbName:          "db1",
			key:             "key3",
			expectedReaders: make(map[string]uint32),
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			users, err := env.s.GetReaders(tt.dbName, tt.key)
			require.NoError(t, err)
			require.Equal(t, tt.expectedReaders, users)
		})
	}
}

func TestGetWriters(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	defer env.cleanup()

	setup(t, env.s)

	tests := []struct {
		name            string
		dbName          string
		key             string
		expectedWriters map[string]uint32
	}{
		{
			name:   "fetch users who have written to key1",
			dbName: "db1",
			key:    "key1",
			expectedWriters: map[string]uint32{
				"user1": 2,
				"user2": 3,
			},
		},
		{
			name:   "fetch users who have written to key2",
			dbName: "db1",
			key:    "key2",
			expectedWriters: map[string]uint32{
				"user1": 1,
				"user2": 1,
			},
		},
		{
			name:            "fetch users who have written non-existing key",
			dbName:          "db1",
			key:             "key3",
			expectedWriters: make(map[string]uint32),
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			users, err := env.s.GetWriters(tt.dbName, tt.key)
			require.NoError(t, err)
			require.Equal(t, tt.expectedWriters, users)
		})
	}
}

func TestGetValuesReadByUser(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	defer env.cleanup()

	setup(t, env.s)

	tests := []struct {
		name          string
		userID        string
		expectedReads []*types.KVWithMetadata
	}{
		{
			name:   "fetch all values read by user1",
			userID: "user1",
			expectedReads: []*types.KVWithMetadata{
				{
					Key:   "key1",
					Value: []byte("value1"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 1,
							TxNum:    0,
						},
					},
				},
			},
		},
		{
			name:   "fetch all values read by user2",
			userID: "user2",
			expectedReads: []*types.KVWithMetadata{
				{
					Key:   "key2",
					Value: []byte("value1"),
					Metadata: &types.Metadata{
						AccessControl: &types.AccessControl{
							ReadWriteUsers: map[string]bool{
								"user1": true,
								"user2": true,
							},
						},
						Version: &types.Version{
							BlockNum: 1,
							TxNum:    1,
						},
					},
				},
				{
					Key:   "key1",
					Value: []byte("value3"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 2,
							TxNum:    1,
						},
					},
				},
				{
					Key:   "key2",
					Value: []byte("value1"),
					Metadata: &types.Metadata{
						AccessControl: &types.AccessControl{
							ReadWriteUsers: map[string]bool{
								"user1": true,
								"user2": true,
							},
						},
						Version: &types.Version{
							BlockNum: 1,
							TxNum:    1,
						},
					},
				},
			},
		},
		{
			name:          "fetch all values read by user3",
			userID:        "user3",
			expectedReads: nil,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			kvs, err := env.s.GetValuesReadByUser(tt.userID)
			require.NoError(t, err)
			require.ElementsMatch(t, tt.expectedReads, kvs)
		})
	}
}

func TestGetValuesWrittenByUser(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	defer env.cleanup()

	setup(t, env.s)

	tests := []struct {
		name           string
		userID         string
		expectedWrites []*types.KVWithMetadata
	}{
		{
			name:   "fetch all values written by user1",
			userID: "user1",
			expectedWrites: []*types.KVWithMetadata{
				{
					Key:   "key1",
					Value: []byte("value1"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 1,
							TxNum:    0,
						},
					},
				},
				{
					Key:   "key2",
					Value: []byte("value1"),
					Metadata: &types.Metadata{
						AccessControl: &types.AccessControl{
							ReadWriteUsers: map[string]bool{
								"user1": true,
								"user2": true,
							},
						},
						Version: &types.Version{
							BlockNum: 1,
							TxNum:    1,
						},
					},
				},
				{
					Key:   "key1",
					Value: []byte("value2"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 2,
							TxNum:    0,
						},
					},
				},
			},
		},
		{
			name:   "fetch all values written by user2",
			userID: "user2",
			expectedWrites: []*types.KVWithMetadata{
				{
					Key:   "key1",
					Value: []byte("value3"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 2,
							TxNum:    1,
						},
					},
				},
				{
					Key:   "key1",
					Value: []byte("value4"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 3,
							TxNum:    0,
						},
					},
				},
				{
					Key:   "key1",
					Value: []byte("value5"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 4,
							TxNum:    0,
						},
					},
				},
				{
					Key:   "key2",
					Value: []byte("value2"),
					Metadata: &types.Metadata{
						AccessControl: &types.AccessControl{
							ReadWriteUsers: map[string]bool{
								"user1": true,
								"user2": true,
							},
						},
						Version: &types.Version{
							BlockNum: 3,
							TxNum:    0,
						},
					},
				},
			},
		},
		{
			name:           "fetch all values read by user3",
			userID:         "user3",
			expectedWrites: nil,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			kvs, err := env.s.GetValuesWrittenByUser(tt.userID)
			require.NoError(t, err)
			require.ElementsMatch(t, tt.expectedWrites, kvs)
		})
	}
}

func TestGetNextValues(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	defer env.cleanup()

	setup(t, env.s)

	tests := []struct {
		name           string
		dbName         string
		key            string
		version        *types.Version
		limit          int
		expectedValues []*types.ValueWithMetadata
	}{
		{
			name:   "all next values of key1, value1",
			dbName: "db1",
			key:    "key1",
			version: &types.Version{
				BlockNum: 1,
				TxNum:    0,
			},
			limit: -1,
			expectedValues: []*types.ValueWithMetadata{
				{
					Value: []byte("value2"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 2,
							TxNum:    0,
						},
					},
				},
				{
					Value: []byte("value3"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 2,
							TxNum:    1,
						},
					},
				},
				{
					Value: []byte("value4"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 3,
							TxNum:    0,
						},
					},
				},
				{
					Value: []byte("value5"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 4,
							TxNum:    0,
						},
					},
				},
			},
		},
		{
			name:   "all next values of key1, value2",
			dbName: "db1",
			key:    "key1",
			version: &types.Version{
				BlockNum: 2,
				TxNum:    0,
			},
			limit: -1,
			expectedValues: []*types.ValueWithMetadata{
				{
					Value: []byte("value3"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 2,
							TxNum:    1,
						},
					},
				},
				{
					Value: []byte("value4"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 3,
							TxNum:    0,
						},
					},
				},
				{
					Value: []byte("value5"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 4,
							TxNum:    0,
						},
					},
				},
			},
		},
		{
			name:   "all next values of key1, value4",
			dbName: "db1",
			key:    "key1",
			version: &types.Version{
				BlockNum: 3,
				TxNum:    0,
			},
			limit: -1,
			expectedValues: []*types.ValueWithMetadata{
				{
					Value: []byte("value5"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 4,
							TxNum:    0,
						},
					},
				},
			},
		},
		{
			name:   "all next values of key1, value5",
			dbName: "db1",
			key:    "key1",
			version: &types.Version{
				BlockNum: 4,
				TxNum:    0,
			},
			limit:          -1,
			expectedValues: []*types.ValueWithMetadata{},
		},
		{
			name:   "next 2 values of key1, value1",
			dbName: "db1",
			key:    "key1",
			version: &types.Version{
				BlockNum: 1,
				TxNum:    0,
			},
			limit: 2,
			expectedValues: []*types.ValueWithMetadata{
				{
					Value: []byte("value2"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 2,
							TxNum:    0,
						},
					},
				},
				{
					Value: []byte("value3"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 2,
							TxNum:    1,
						},
					},
				},
			},
		},
		{
			name:   "next values of non-existing key",
			dbName: "db1",
			key:    "key3",
			version: &types.Version{
				BlockNum: 3,
				TxNum:    0,
			},
			limit:          -1,
			expectedValues: nil,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			values, err := env.s.GetNextValues(tt.dbName, tt.key, tt.version, tt.limit)
			require.NoError(t, err)
			require.ElementsMatch(t, tt.expectedValues, values)
		})
	}
}

func TestGetPreviousValues(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	defer env.cleanup()

	setup(t, env.s)

	tests := []struct {
		name           string
		dbName         string
		key            string
		version        *types.Version
		limit          int
		expectedValues []*types.ValueWithMetadata
	}{
		{
			name:   "all previous values of key1, value4",
			dbName: "db1",
			key:    "key1",
			version: &types.Version{
				BlockNum: 3,
				TxNum:    0,
			},
			limit: -1,
			expectedValues: []*types.ValueWithMetadata{
				{
					Value: []byte("value1"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 1,
							TxNum:    0,
						},
					},
				},
				{
					Value: []byte("value2"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 2,
							TxNum:    0,
						},
					},
				},
				{
					Value: []byte("value3"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 2,
							TxNum:    1,
						},
					},
				},
			},
		},
		{
			name:   "all previous values of key1, value3",
			dbName: "db1",
			key:    "key1",
			version: &types.Version{
				BlockNum: 2,
				TxNum:    1,
			},
			limit: -1,
			expectedValues: []*types.ValueWithMetadata{
				{
					Value: []byte("value1"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 1,
							TxNum:    0,
						},
					},
				},
				{
					Value: []byte("value2"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 2,
							TxNum:    0,
						},
					},
				},
			},
		},
		{
			name:   "all previous values of key1, value1",
			dbName: "db1",
			key:    "key1",
			version: &types.Version{
				BlockNum: 1,
				TxNum:    0,
			},
			limit:          -1,
			expectedValues: []*types.ValueWithMetadata{},
		},
		{
			name:   "previous 2 values of key1, value4",
			dbName: "db1",
			key:    "key1",
			version: &types.Version{
				BlockNum: 3,
				TxNum:    0,
			},
			limit: 2,
			expectedValues: []*types.ValueWithMetadata{
				{
					Value: []byte("value2"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 2,
							TxNum:    0,
						},
					},
				},
				{
					Value: []byte("value3"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 2,
							TxNum:    1,
						},
					},
				},
			},
		},
		{
			name:   "previous 2 values of key1, value5",
			dbName: "db1",
			key:    "key1",
			version: &types.Version{
				BlockNum: 4,
				TxNum:    0,
			},
			limit: 2,
			expectedValues: []*types.ValueWithMetadata{
				{
					Value: []byte("value3"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 2,
							TxNum:    1,
						},
					},
				},
				{
					Value: []byte("value4"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 3,
							TxNum:    0,
						},
					},
				},
			},
		},
		{
			name:   "previous values of non-existing key",
			dbName: "db1",
			key:    "key3",
			version: &types.Version{
				BlockNum: 3,
				TxNum:    0,
			},
			limit:          -1,
			expectedValues: nil,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			values, err := env.s.GetPreviousValues(tt.dbName, tt.key, tt.version, tt.limit)
			require.NoError(t, err)
			require.ElementsMatch(t, tt.expectedValues, values)
		})
	}
}

func TestGetDeletedValues(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	defer env.cleanup()

	setup(t, env.s)

	tests := []struct {
		name           string
		dbName         string
		key            string
		expectedValues []*types.ValueWithMetadata
	}{
		{
			name:   "fetch all deleted values of key1",
			dbName: "db1",
			key:    "key1",
			expectedValues: []*types.ValueWithMetadata{
				{
					Value: []byte("value4"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 3,
							TxNum:    0,
						},
					},
				},
				{
					Value: []byte("value5"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 4,
							TxNum:    0,
						},
					},
				},
			},
		},
		{
			name:           "fetch all deleted values of key2",
			dbName:         "db1",
			key:            "key2",
			expectedValues: nil,
		},
		{
			name:           "fetch non-existing value",
			dbName:         "db1",
			key:            "key3",
			expectedValues: nil,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			value, err := env.s.GetDeletedValues(tt.dbName, tt.key)
			require.NoError(t, err)
			require.ElementsMatch(t, tt.expectedValues, value)
		})
	}
}

func TestGetValuesDeletedByUser(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	defer env.cleanup()

	setup(t, env.s)

	tests := []struct {
		name           string
		userID         string
		expectedWrites []*types.KVWithMetadata
	}{
		{
			name:   "fetch all values deleted by user2",
			userID: "user2",
			expectedWrites: []*types.KVWithMetadata{
				{
					Key:   "key1",
					Value: []byte("value4"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 3,
							TxNum:    0,
						},
					},
				},
				{
					Key:   "key1",
					Value: []byte("value5"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 4,
							TxNum:    0,
						},
					},
				},
			},
		},
		{
			name:           "fetch all values deleted by user3",
			userID:         "user3",
			expectedWrites: nil,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			kvs, err := env.s.GetValuesDeletedByUser(tt.userID)
			require.NoError(t, err)
			require.ElementsMatch(t, tt.expectedWrites, kvs)
		})
	}
}

func TestGetTxIDLocation(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	defer env.cleanup()

	setup(t, env.s)

	tests := []struct {
		txID        string
		expected    *TxIDLocation
		expectedErr string
	}{
		{
			txID: "tx2",
			expected: &TxIDLocation{
				BlockNum: 1,
				TxIndex:  1,
			},
		},
		{
			txID: "tx10",
			expected: &TxIDLocation{
				BlockNum: 1,
				TxIndex:  2,
			},
		},
		{
			txID:        "tx-not-found",
			expected:    nil,
			expectedErr: "TxID not found: tx-not-found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.txID, func(t *testing.T) {
			loc, err := env.s.GetTxIDLocation(tt.txID)
			if tt.expectedErr == "" {
				require.NoError(t, err)
				require.Equal(t, tt.expected, loc)
			} else {
				require.EqualError(t, err, tt.expectedErr)
			}
		})
	}
}

func TestGetMostRecentValueAtOrBelow(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	defer env.cleanup()

	setup(t, env.s)

	tests := []struct {
		name          string
		dbName        string
		key           string
		version       *types.Version
		expectedValue *types.ValueWithMetadata
	}{
		{
			name:   "value with exact version present in the beginning",
			dbName: "db1",
			key:    "key1",
			version: &types.Version{
				BlockNum: 1,
				TxNum:    0,
			},
			expectedValue: &types.ValueWithMetadata{
				Value: []byte("value1"),
				Metadata: &types.Metadata{
					Version: &types.Version{
						BlockNum: 1,
						TxNum:    0,
					},
				},
			},
		},
		{
			name:   "value with exact version present in the middle",
			dbName: "db1",
			key:    "key1",
			version: &types.Version{
				BlockNum: 2,
				TxNum:    1,
			},
			expectedValue: &types.ValueWithMetadata{
				Value: []byte("value3"),
				Metadata: &types.Metadata{
					Version: &types.Version{
						BlockNum: 2,
						TxNum:    1,
					},
				},
			},
		},
		{
			name:   "value with exact version present in the end",
			dbName: "db1",
			key:    "key1",
			version: &types.Version{
				BlockNum: 4,
				TxNum:    0,
			},
			expectedValue: &types.ValueWithMetadata{
				Value: []byte("value5"),
				Metadata: &types.Metadata{
					Version: &types.Version{
						BlockNum: 4,
						TxNum:    0,
					},
				},
			},
		},
		{
			name:   "value with lesser version present in the beginning",
			dbName: "db1",
			key:    "key1",
			version: &types.Version{
				BlockNum: 1,
				TxNum:    5,
			},
			expectedValue: &types.ValueWithMetadata{
				Value: []byte("value1"),
				Metadata: &types.Metadata{
					Version: &types.Version{
						BlockNum: 1,
						TxNum:    0,
					},
				},
			},
		},
		{
			name:   "value with lesser version present in the middle",
			dbName: "db1",
			key:    "key1",
			version: &types.Version{
				BlockNum: 2,
				TxNum:    10,
			},
			expectedValue: &types.ValueWithMetadata{
				Value: []byte("value3"),
				Metadata: &types.Metadata{
					Version: &types.Version{
						BlockNum: 2,
						TxNum:    1,
					},
				},
			},
		},
		{
			name:   "value with lesser version present in the end",
			dbName: "db1",
			key:    "key1",
			version: &types.Version{
				BlockNum: 6,
				TxNum:    3,
			},
			expectedValue: &types.ValueWithMetadata{
				Value: []byte("value5"),
				Metadata: &types.Metadata{
					Version: &types.Version{
						BlockNum: 4,
						TxNum:    0,
					},
				},
			},
		},
		{
			name:   "fetching non-existing value",
			dbName: "db1",
			key:    "key1",
			version: &types.Version{
				BlockNum: 0,
				TxNum:    0,
			},
			expectedValue: nil,
		},
		{
			name:          "fetch non-existing key",
			dbName:        "db1",
			key:           "key3",
			expectedValue: nil,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			value, err := env.s.GetMostRecentValueAtOrBelow(tt.dbName, tt.key, tt.version)
			require.NoError(t, err)
			require.Equal(t, tt.expectedValue, value)
		})
	}
}
