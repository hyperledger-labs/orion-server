// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package bcdb

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/IBM-Blockchain/bcdb-server/internal/provenance"
	"github.com/IBM-Blockchain/bcdb-server/pkg/logger"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
	"github.com/stretchr/testify/require"
)

type provenanceQueryProcessorTestEnv struct {
	p *provenanceQueryProcessor

	cleanup func(t *testing.T)
}

func newProvenanceQueryProcessorTestEnv(t *testing.T) *provenanceQueryProcessorTestEnv {
	path, err := ioutil.TempDir("/tmp", "provenanceQueryProcessor")
	require.NoError(t, err)

	c := &logger.Config{
		Level:         "info",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
	}
	logger, err := logger.New(c)
	require.NoError(t, err)

	provenanceStore, err := provenance.Open(
		&provenance.Config{
			StoreDir: path,
			Logger:   logger,
		},
	)
	require.NoError(t, err)

	cleanup := func(t *testing.T) {
		if err := provenanceStore.Close(); err != nil {
			t.Errorf("failed to close the provenance store: %v", err)
		}
		if err := os.RemoveAll(path); err != nil {
			t.Fatalf("failed to remove %s due to %v", path, err)
		}
	}

	return &provenanceQueryProcessorTestEnv{
		p: newProvenanceQueryProcessor(
			&provenanceQueryProcessorConfig{
				provenanceStore: provenanceStore,
				logger:          logger,
			}),
		cleanup: cleanup,
	}
}

func setupProvenanceStore(t *testing.T, s *provenance.Store) {
	block1TxsData := []*provenance.TxDataForProvenance{
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

	block2TxsData := []*provenance.TxDataForProvenance{
		{
			IsValid: true,
			DBName:  "db1",
			UserID:  "user1",
			TxID:    "tx3",
			Reads: []*provenance.KeyWithVersion{
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
	}

	block3TxsData := []*provenance.TxDataForProvenance{
		{
			IsValid: true,
			DBName:  "db1",
			UserID:  "user2",
			TxID:    "tx5",
			Reads: []*provenance.KeyWithVersion{
				{
					Key: "key1",
					Version: &types.Version{
						BlockNum: 2,
						TxNum:    0,
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
					TxNum:    0,
				},
				"key2": {
					BlockNum: 1,
					TxNum:    1,
				},
			},
		},
	}

	block4TxsData := []*provenance.TxDataForProvenance{
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

	block5TxsData := []*provenance.TxDataForProvenance{
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

	block6TxsData := []*provenance.TxDataForProvenance{
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

func TestGetValues(t *testing.T) {
	env := newProvenanceQueryProcessorTestEnv(t)
	defer env.cleanup(t)

	setupProvenanceStore(t, env.p.provenanceStore)

	tests := []struct {
		name            string
		dbName          string
		key             string
		expectedPayload *types.GetHistoricalDataResponse
	}{
		{
			name:   "fetch all values of key1",
			dbName: "db1",
			key:    "key1",
			expectedPayload: &types.GetHistoricalDataResponse{
				Values: []*types.ValueWithMetadata{
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
		},
		{
			name:   "fetch all values of non-existing key",
			dbName: "db1",
			key:    "key5",
			expectedPayload: &types.GetHistoricalDataResponse{
				Values: nil,
			},
		},
	}

	for _, tt := range tests {
		payload, err := env.p.GetValues(tt.dbName, tt.key)
		require.NoError(t, err)

		require.NotNil(t, payload)
		require.ElementsMatch(t, tt.expectedPayload.GetValues(), payload.GetValues())
	}
}

func TestGetDeletedValues(t *testing.T) {
	env := newProvenanceQueryProcessorTestEnv(t)
	defer env.cleanup(t)

	setupProvenanceStore(t, env.p.provenanceStore)

	tests := []struct {
		name             string
		dbName           string
		key              string
		expectedEnvelope *types.GetHistoricalDataResponse
	}{
		{
			name:   "fetch all deleted values of key1",
			dbName: "db1",
			key:    "key1",
			expectedEnvelope: &types.GetHistoricalDataResponse{
				Values: []*types.ValueWithMetadata{
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
		},
		{
			name:   "fetch all deleted values of non-existing key",
			dbName: "db1",
			key:    "key5",
			expectedEnvelope: &types.GetHistoricalDataResponse{
				Values: nil,
			},
		},
	}

	for _, tt := range tests {
		payload, err := env.p.GetDeletedValues(tt.dbName, tt.key)
		require.NoError(t, err)

		require.NotNil(t, payload)
		require.ElementsMatch(t, tt.expectedEnvelope.GetValues(), payload.GetValues())
	}
}

func TestGetPreviousValues(t *testing.T) {
	env := newProvenanceQueryProcessorTestEnv(t)
	defer env.cleanup(t)

	setupProvenanceStore(t, env.p.provenanceStore)

	tests := []struct {
		name            string
		dbName          string
		key             string
		version         *types.Version
		expectedPayload *types.GetHistoricalDataResponse
	}{
		{
			name:   "fetch the previous value of key1 at version{Blk 3, txNum 0}",
			dbName: "db1",
			key:    "key1",
			version: &types.Version{
				BlockNum: 3,
				TxNum:    0,
			},
			expectedPayload: &types.GetHistoricalDataResponse{
				Values: []*types.ValueWithMetadata{
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
		},
		{
			name:   "fetch the previous value of non-existing key",
			dbName: "db1",
			key:    "key5",
			version: &types.Version{
				BlockNum: 1,
				TxNum:    1,
			},
			expectedPayload: &types.GetHistoricalDataResponse{},
		},
	}

	for _, tt := range tests {
		payload, err := env.p.GetPreviousValues(tt.dbName, tt.key, tt.version)
		require.NoError(t, err)

		require.NotNil(t, payload)
		require.Equal(t, tt.expectedPayload, payload)
	}
}

func TestGetNextValues(t *testing.T) {
	env := newProvenanceQueryProcessorTestEnv(t)
	defer env.cleanup(t)

	setupProvenanceStore(t, env.p.provenanceStore)

	tests := []struct {
		name            string
		dbName          string
		key             string
		version         *types.Version
		expectedPayload *types.GetHistoricalDataResponse
	}{
		{
			name:   "fetch next value of key1 at version {Blk 2, txNum 0}",
			dbName: "db1",
			key:    "key1",
			version: &types.Version{
				BlockNum: 2,
				TxNum:    0,
			},
			expectedPayload: &types.GetHistoricalDataResponse{
				Values: []*types.ValueWithMetadata{
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
		},
		{
			name:   "fetch next value of non-existing key",
			dbName: "db1",
			key:    "key5",
			version: &types.Version{
				BlockNum: 2,
				TxNum:    1,
			},
			expectedPayload: &types.GetHistoricalDataResponse{
				Values: nil,
			},
		},
	}

	for _, tt := range tests {
		envelope, err := env.p.GetNextValues(tt.dbName, tt.key, tt.version)
		require.NoError(t, err)

		require.NotNil(t, envelope)
		require.Equal(t, tt.expectedPayload, envelope)
	}
}

func TestGetValueAt(t *testing.T) {
	env := newProvenanceQueryProcessorTestEnv(t)
	defer env.cleanup(t)

	setupProvenanceStore(t, env.p.provenanceStore)

	tests := []struct {
		name            string
		dbName          string
		key             string
		version         *types.Version
		expectedPayload *types.GetHistoricalDataResponse
	}{
		{
			name:   "fetch value of key1 at a particular version",
			dbName: "db1",
			key:    "key1",
			version: &types.Version{
				BlockNum: 3,
				TxNum:    0,
			},
			expectedPayload: &types.GetHistoricalDataResponse{
				Values: []*types.ValueWithMetadata{
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
		},
		{
			name:   "fetch value of non-existing key",
			dbName: "db1",
			key:    "key5",
			version: &types.Version{
				BlockNum: 2,
				TxNum:    1,
			},
			expectedPayload: &types.GetHistoricalDataResponse{
				Values: nil,
			},
		},
	}

	for _, tt := range tests {
		envelope, err := env.p.GetValueAt(tt.dbName, tt.key, tt.version)
		require.NoError(t, err)

		require.NotNil(t, envelope)
		require.Equal(t, tt.expectedPayload, envelope)
	}
}

func TestGetMostRecentValueAtOrBelow(t *testing.T) {
	env := newProvenanceQueryProcessorTestEnv(t)
	defer env.cleanup(t)

	setupProvenanceStore(t, env.p.provenanceStore)

	tests := []struct {
		name            string
		dbName          string
		key             string
		version         *types.Version
		expectedPayload *types.GetHistoricalDataResponse
	}{
		{
			name:   "fetch most recent value of key1 at or below a particular version",
			dbName: "db1",
			key:    "key1",
			version: &types.Version{
				BlockNum: 2,
				TxNum:    5,
			},
			expectedPayload: &types.GetHistoricalDataResponse{
				Values: []*types.ValueWithMetadata{
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
		},
		{
			name:   "fetch value of non-existing key",
			dbName: "db1",
			key:    "key5",
			version: &types.Version{
				BlockNum: 2,
				TxNum:    1,
			},
			expectedPayload: &types.GetHistoricalDataResponse{
				Values: nil,
			},
		},
	}

	for _, tt := range tests {
		envelope, err := env.p.GetMostRecentValueAtOrBelow(tt.dbName, tt.key, tt.version)
		require.NoError(t, err)

		require.NotNil(t, envelope)
		require.Equal(t, tt.expectedPayload, envelope)
	}
}

func TestGetReaders(t *testing.T) {
	env := newProvenanceQueryProcessorTestEnv(t)
	defer env.cleanup(t)

	setupProvenanceStore(t, env.p.provenanceStore)

	tests := []struct {
		name            string
		dbName          string
		key             string
		expectedPayload *types.GetDataReadersResponse
	}{
		{
			name:   "fetch readers of key1",
			dbName: "db1",
			key:    "key1",
			expectedPayload: &types.GetDataReadersResponse{
				ReadBy: map[string]uint32{
					"user1": 1,
					"user2": 1,
				},
			},
		},
		{
			name:   "fetch readers of non-existing key",
			dbName: "db1",
			key:    "key5",
			expectedPayload: &types.GetDataReadersResponse{
				ReadBy: nil,
			},
		},
	}

	for _, tt := range tests {
		envelope, err := env.p.GetReaders(tt.dbName, tt.key)
		require.NoError(t, err)

		require.NotNil(t, envelope)
		require.Equal(t, tt.expectedPayload, envelope)
	}
}

func TestGetWriters(t *testing.T) {
	env := newProvenanceQueryProcessorTestEnv(t)
	defer env.cleanup(t)

	setupProvenanceStore(t, env.p.provenanceStore)

	tests := []struct {
		name            string
		dbName          string
		key             string
		expectedPayload *types.GetDataWritersResponse
	}{
		{
			name:   "fetch readers of key1",
			dbName: "db1",
			key:    "key1",
			expectedPayload: &types.GetDataWritersResponse{
				WrittenBy: map[string]uint32{
					"user1": 2,
					"user2": 2,
				},
			},
		},
		{
			name:   "fetch readers of non-existing key",
			dbName: "db1",
			key:    "key5",
			expectedPayload: &types.GetDataWritersResponse{
				WrittenBy: nil,
			},
		},
	}

	for _, tt := range tests {
		envelope, err := env.p.GetWriters(tt.dbName, tt.key)
		require.NoError(t, err)
		require.NotNil(t, envelope)
		require.Equal(t, tt.expectedPayload, envelope)
	}
}

func TestGetValuesReadByUser(t *testing.T) {
	env := newProvenanceQueryProcessorTestEnv(t)
	defer env.cleanup(t)

	setupProvenanceStore(t, env.p.provenanceStore)

	tests := []struct {
		name            string
		user            string
		expectedPayload *types.GetDataProvenanceResponse
	}{
		{
			name: "fetch values read by user1",
			user: "user1",
			expectedPayload: &types.GetDataProvenanceResponse{
				KVs: []*types.KVWithMetadata{
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
		},
		{
			name: "fetch values read by user5",
			user: "user5",
			expectedPayload: &types.GetDataProvenanceResponse{
				KVs: nil,
			},
		},
	}

	for _, tt := range tests {
		envelope, err := env.p.GetValuesReadByUser(tt.user)
		require.NoError(t, err)

		require.NotNil(t, envelope)
		require.Equal(t, tt.expectedPayload, envelope)
	}
}

func TestGetValuesWrittenByUser(t *testing.T) {
	env := newProvenanceQueryProcessorTestEnv(t)
	defer env.cleanup(t)

	setupProvenanceStore(t, env.p.provenanceStore)

	tests := []struct {
		name            string
		user            string
		expectedPayload *types.GetDataProvenanceResponse
	}{
		{
			name: "fetch values read by user1",
			user: "user1",
			expectedPayload: &types.GetDataProvenanceResponse{
				KVs: []*types.KVWithMetadata{
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
		},
		{
			name: "fetch values read by user5",
			user: "user5",
			expectedPayload: &types.GetDataProvenanceResponse{
				KVs: nil,
			},
		},
	}

	for _, tt := range tests {
		payload, err := env.p.GetValuesWrittenByUser(tt.user)
		require.NoError(t, err)

		require.NotNil(t, payload)
		require.Equal(t, tt.expectedPayload, payload)
	}
}

func TestGetValuesDeletedByUser(t *testing.T) {
	env := newProvenanceQueryProcessorTestEnv(t)
	defer env.cleanup(t)

	setupProvenanceStore(t, env.p.provenanceStore)

	tests := []struct {
		name            string
		user            string
		expectedPayload *types.GetDataProvenanceResponse
	}{
		{
			name: "fetch values deleted by user1",
			user: "user2",
			expectedPayload: &types.GetDataProvenanceResponse{
				KVs: []*types.KVWithMetadata{
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
		},
		{
			name: "fetch values deleted by user5",
			user: "user5",
			expectedPayload: &types.GetDataProvenanceResponse{
				KVs: nil,
			},
		},
	}

	for _, tt := range tests {
		payload, err := env.p.GetValuesDeletedByUser(tt.user)
		require.NoError(t, err)

		require.NotNil(t, payload)
		require.Equal(t, tt.expectedPayload, payload)
	}
}

func TestGetTxSubmittedByUser(t *testing.T) {
	env := newProvenanceQueryProcessorTestEnv(t)
	defer env.cleanup(t)

	setupProvenanceStore(t, env.p.provenanceStore)

	tests := []struct {
		name            string
		user            string
		expectedPayload *types.GetTxIDsSubmittedByResponse
	}{
		{
			name: "fetch tx submitted by user",
			user: "user2",
			expectedPayload: &types.GetTxIDsSubmittedByResponse{
				TxIDs: []string{"tx5", "tx50", "tx6"},
			},
		},
		{
			name: "fetch tx submitted by user - empty",
			user: "user5",
			expectedPayload: &types.GetTxIDsSubmittedByResponse{
				TxIDs: nil,
			},
		},
	}

	for _, tt := range tests {
		payload, err := env.p.GetTxIDsSubmittedByUser(tt.user)
		require.NoError(t, err)

		require.NotNil(t, payload)
		require.Equal(t, tt.expectedPayload, payload)
	}
}
