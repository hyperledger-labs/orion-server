// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package bcdb

import (
	"testing"

	"github.com/golang/protobuf/proto"
	ierrors "github.com/hyperledger-labs/orion-server/internal/errors"
	"github.com/hyperledger-labs/orion-server/internal/identity"
	"github.com/hyperledger-labs/orion-server/internal/provenance"
	"github.com/hyperledger-labs/orion-server/internal/worldstate"
	"github.com/hyperledger-labs/orion-server/internal/worldstate/leveldb"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/stretchr/testify/require"
)

type provenanceQueryProcessorTestEnv struct {
	p       *provenanceQueryProcessor
	db      worldstate.DB
	cleanup func(t *testing.T)
}

func newProvenanceQueryProcessorTestEnv(t *testing.T) *provenanceQueryProcessorTestEnv {
	provenancePath := t.TempDir()

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
			StoreDir: provenancePath,
			Logger:   logger,
		},
	)
	if err != nil {
		t.Fatalf("failed to create a new provenance store, %v", err)
	}

	dbPath := t.TempDir()
	db, err := leveldb.Open(
		&leveldb.Config{
			DBRootDir: dbPath,
			Logger:    logger,
		},
	)
	if err != nil {
		t.Fatalf("failed to create a new leveldb instance, %v", err)
	}

	cleanup := func(t *testing.T) {
		if err := provenanceStore.Close(); err != nil {
			t.Errorf("failed to close the provenance store: %v", err)
		}

		if err := db.Close(); err != nil {
			t.Errorf("failed to close leveldb: %v", err)
		}
	}

	return &provenanceQueryProcessorTestEnv{
		p: newProvenanceQueryProcessor(
			&provenanceQueryProcessorConfig{
				provenanceStore: provenanceStore,
				identityQuerier: identity.NewQuerier(db),
				logger:          logger,
			}),
		db:      db,
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
		{
			IsValid: true,
			DBName:  "db2",
			UserID:  "user1",
			TxID:    "tx1-db2",
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
		{
			IsValid: true,
			DBName:  "db2",
			UserID:  "user1",
			TxID:    "tx3-db2",
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
		{
			IsValid: true,
			DBName:  "db2",
			UserID:  "user2",
			TxID:    "tx50",
			Deletes: map[string]*types.Version{
				"key1": {
					BlockNum: 2,
					TxNum:    0,
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
							BlockNum: 5,
							TxNum:    0,
						},
					},
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
					BlockNum: 5,
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

	setupUserForTest(t, "user1", false, env.db)
	setupUserForTest(t, "admin1", true, env.db)

	tests := []struct {
		name            string
		dbName          string
		key             string
		user            string
		expectedPayload *types.GetHistoricalDataResponse
		expectedError   error
	}{
		{
			name:            "fetch all values of key1",
			dbName:          "db1",
			key:             "key1",
			user:            "admin1",
			expectedPayload: &types.GetHistoricalDataResponse{Values: []*types.ValueWithMetadata{{Value: []byte("value1"), Metadata: &types.Metadata{Version: &types.Version{BlockNum: 1, TxNum: 0}}}, {Value: []byte("value2"), Metadata: &types.Metadata{Version: &types.Version{BlockNum: 2, TxNum: 0}}}, {Value: []byte("value4"), Metadata: &types.Metadata{Version: &types.Version{BlockNum: 3, TxNum: 0}}}, {Value: []byte("value5"), Metadata: &types.Metadata{Version: &types.Version{BlockNum: 5, TxNum: 0}}}}},
			expectedError:   nil,
		},
		{
			name:   "fetch all values of non-existing key",
			dbName: "db1",
			key:    "key5",
			user:   "admin1",
			expectedPayload: &types.GetHistoricalDataResponse{
				Values: nil,
			},
		},
		{
			name:            "fetching all values of key1 with non-admin user will result in an error",
			dbName:          "db1",
			key:             "key1",
			user:            "user1",
			expectedPayload: nil,
			expectedError:   &ierrors.PermissionErr{ErrMsg: "The querier [user1] is not an admin. Only an admin can query historical data"},
		},
	}

	for _, tt := range tests {
		payload, err := env.p.GetValues(tt.user, tt.dbName, tt.key)
		if tt.expectedError != nil {
			require.Equal(t, err, tt.expectedError)
			require.Nil(t, payload)
			continue
		}

		require.NoError(t, err)
		require.NotNil(t, payload)
		require.ElementsMatch(t, tt.expectedPayload.GetValues(), payload.GetValues())
	}
}

func TestGetDeletedValues(t *testing.T) {
	env := newProvenanceQueryProcessorTestEnv(t)
	defer env.cleanup(t)

	setupProvenanceStore(t, env.p.provenanceStore)

	setupUserForTest(t, "user1", false, env.db)
	setupUserForTest(t, "admin1", true, env.db)

	tests := []struct {
		name             string
		dbName           string
		key              string
		user             string
		expectedEnvelope *types.GetHistoricalDataResponse
		expectedError    error
	}{
		{
			name:   "fetch all deleted values of key1",
			dbName: "db1",
			key:    "key1",
			user:   "admin1",
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
								BlockNum: 5,
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
			user:   "admin1",
			expectedEnvelope: &types.GetHistoricalDataResponse{
				Values: nil,
			},
		},
		{
			name:             "fetching all deleted values key1 with non-admin user will result in an error",
			dbName:           "db1",
			key:              "key1",
			user:             "user1",
			expectedEnvelope: nil,
			expectedError:    &ierrors.PermissionErr{ErrMsg: "The querier [user1] is not an admin. Only an admin can query historical data"},
		},
	}

	for _, tt := range tests {
		payload, err := env.p.GetDeletedValues(tt.user, tt.dbName, tt.key)
		if tt.expectedError != nil {
			require.Equal(t, err, tt.expectedError)
			require.Nil(t, payload)
			continue
		}

		require.NoError(t, err)
		require.NotNil(t, payload)
		require.ElementsMatch(t, tt.expectedEnvelope.GetValues(), payload.GetValues())
	}
}

func TestGetPreviousValues(t *testing.T) {
	env := newProvenanceQueryProcessorTestEnv(t)
	defer env.cleanup(t)

	setupProvenanceStore(t, env.p.provenanceStore)

	setupUserForTest(t, "user1", false, env.db)
	setupUserForTest(t, "admin1", true, env.db)

	tests := []struct {
		name            string
		dbName          string
		key             string
		user            string
		version         *types.Version
		expectedPayload *types.GetHistoricalDataResponse
		expectedError   error
	}{
		{
			name:   "fetch the previous value of key1 at version{Blk 3, txNum 0}",
			dbName: "db1",
			key:    "key1",
			user:   "admin1",
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
			user:   "admin1",
			version: &types.Version{
				BlockNum: 1,
				TxNum:    1,
			},
			expectedPayload: &types.GetHistoricalDataResponse{},
		},
		{
			name:   "fetching the previous value of key1 at version{Blk 3, txNum 0} by an non-admin user will result in an error",
			dbName: "db1",
			key:    "key1",
			user:   "user1",
			version: &types.Version{
				BlockNum: 3,
				TxNum:    0,
			},
			expectedPayload: nil,
			expectedError:   &ierrors.PermissionErr{ErrMsg: "The querier [user1] is not an admin. Only an admin can query historical data"},
		},
	}

	for _, tt := range tests {
		payload, err := env.p.GetPreviousValues(tt.user, tt.dbName, tt.key, tt.version)
		if tt.expectedError != nil {
			require.Equal(t, err, tt.expectedError)
			require.Nil(t, payload)
			continue
		}

		require.NoError(t, err)
		require.NotNil(t, payload)
		require.Equal(t, tt.expectedPayload, payload)
	}
}

func TestGetNextValues(t *testing.T) {
	env := newProvenanceQueryProcessorTestEnv(t)
	defer env.cleanup(t)

	setupProvenanceStore(t, env.p.provenanceStore)

	setupUserForTest(t, "user1", false, env.db)
	setupUserForTest(t, "admin1", true, env.db)

	tests := []struct {
		name            string
		dbName          string
		key             string
		user            string
		version         *types.Version
		expectedPayload *types.GetHistoricalDataResponse
		expectedError   error
	}{
		{
			name:   "fetch next value of key1 at version {Blk 2, txNum 0}",
			dbName: "db1",
			key:    "key1",
			user:   "admin1",
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
								BlockNum: 5,
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
			user:   "admin1",
			version: &types.Version{
				BlockNum: 2,
				TxNum:    1,
			},
			expectedPayload: &types.GetHistoricalDataResponse{
				Values: nil,
			},
		},
		{
			name:   "fetching next value of key1 at version {Blk 2, txNum 0} by an non-admin user will result in an error",
			dbName: "db1",
			key:    "key1",
			user:   "user1",
			version: &types.Version{
				BlockNum: 2,
				TxNum:    0,
			},
			expectedPayload: nil,
			expectedError:   &ierrors.PermissionErr{ErrMsg: "The querier [user1] is not an admin. Only an admin can query historical data"},
		},
	}

	for _, tt := range tests {
		envelope, err := env.p.GetNextValues(tt.user, tt.dbName, tt.key, tt.version)
		if tt.expectedError != nil {
			require.Equal(t, err, tt.expectedError)
			require.Nil(t, envelope)
			continue
		}

		require.NoError(t, err)
		require.NotNil(t, envelope)
		require.Equal(t, tt.expectedPayload, envelope)
	}
}

func TestGetValueAt(t *testing.T) {
	env := newProvenanceQueryProcessorTestEnv(t)
	defer env.cleanup(t)

	setupProvenanceStore(t, env.p.provenanceStore)

	setupUserForTest(t, "user1", false, env.db)
	setupUserForTest(t, "admin1", true, env.db)

	tests := []struct {
		name            string
		dbName          string
		key             string
		user            string
		version         *types.Version
		expectedPayload *types.GetHistoricalDataResponse
		expectedError   error
	}{
		{
			name:   "fetch value of key1 at a particular version",
			dbName: "db1",
			key:    "key1",
			user:   "admin1",
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
			user:   "admin1",
			version: &types.Version{
				BlockNum: 2,
				TxNum:    1,
			},
			expectedPayload: &types.GetHistoricalDataResponse{
				Values: nil,
			},
		},
		{
			name:   "fetching value of key1 at a particular version by an non-admin user will result in an error",
			dbName: "db1",
			key:    "key1",
			user:   "user1",
			version: &types.Version{
				BlockNum: 3,
				TxNum:    0,
			},
			expectedPayload: nil,
			expectedError:   &ierrors.PermissionErr{ErrMsg: "The querier [user1] is not an admin. Only an admin can query historical data"},
		},
	}

	for _, tt := range tests {
		envelope, err := env.p.GetValueAt(tt.user, tt.dbName, tt.key, tt.version)
		if tt.expectedError != nil {
			require.Equal(t, err, tt.expectedError)
			require.Nil(t, envelope)
			continue
		}

		require.NoError(t, err)
		require.NotNil(t, envelope)
		require.Equal(t, tt.expectedPayload, envelope)
	}
}

func TestGetMostRecentValueAtOrBelow(t *testing.T) {
	env := newProvenanceQueryProcessorTestEnv(t)
	defer env.cleanup(t)

	setupProvenanceStore(t, env.p.provenanceStore)

	setupUserForTest(t, "user1", false, env.db)
	setupUserForTest(t, "admin1", true, env.db)

	tests := []struct {
		name            string
		dbName          string
		key             string
		user            string
		version         *types.Version
		expectedPayload *types.GetHistoricalDataResponse
		expectedError   error
	}{
		{
			name:   "fetch most recent value of key1 at or below a particular version",
			dbName: "db1",
			key:    "key1",
			user:   "admin1",
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
			user:   "admin1",
			version: &types.Version{
				BlockNum: 2,
				TxNum:    1,
			},
			expectedPayload: &types.GetHistoricalDataResponse{
				Values: nil,
			},
		},
		{
			name:   "fetching most recent value of key1 at or below a particular version by an non-admin user will result in an error",
			dbName: "db1",
			key:    "key1",
			user:   "user1",
			version: &types.Version{
				BlockNum: 2,
				TxNum:    5,
			},
			expectedPayload: nil,
			expectedError:   &ierrors.PermissionErr{ErrMsg: "The querier [user1] is not an admin. Only an admin can query historical data"},
		},
	}

	for _, tt := range tests {
		envelope, err := env.p.GetMostRecentValueAtOrBelow(tt.user, tt.dbName, tt.key, tt.version)
		if tt.expectedError != nil {
			require.Equal(t, err, tt.expectedError)
			require.Nil(t, envelope)
			continue
		}

		require.NoError(t, err)
		require.NotNil(t, envelope)
		require.Equal(t, tt.expectedPayload, envelope)
	}
}

func TestGetReaders(t *testing.T) {
	env := newProvenanceQueryProcessorTestEnv(t)
	defer env.cleanup(t)

	setupProvenanceStore(t, env.p.provenanceStore)

	setupUserForTest(t, "user1", false, env.db)
	setupUserForTest(t, "admin1", true, env.db)

	tests := []struct {
		name            string
		dbName          string
		key             string
		user            string
		expectedPayload *types.GetDataReadersResponse
		expectedError   error
	}{
		{
			name:   "fetch readers of key1",
			dbName: "db1",
			key:    "key1",
			user:   "admin1",
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
			user:   "admin1",
			expectedPayload: &types.GetDataReadersResponse{
				ReadBy: nil,
			},
		},
		{
			name:            "fetching readers of key1 by an non-admin user will result in an error",
			dbName:          "db1",
			key:             "key1",
			user:            "user1",
			expectedPayload: nil,
			expectedError:   &ierrors.PermissionErr{ErrMsg: "The querier [user1] is not an admin. Only an admin can query historical data"},
		},
	}

	for _, tt := range tests {
		envelope, err := env.p.GetReaders(tt.user, tt.dbName, tt.key)
		if tt.expectedError != nil {
			require.Equal(t, err, tt.expectedError)
			require.Nil(t, envelope)
			continue
		}

		require.NoError(t, err)
		require.NotNil(t, envelope)
		require.Equal(t, tt.expectedPayload, envelope)
	}
}

func TestGetWriters(t *testing.T) {
	env := newProvenanceQueryProcessorTestEnv(t)
	defer env.cleanup(t)

	setupProvenanceStore(t, env.p.provenanceStore)

	setupUserForTest(t, "user1", false, env.db)
	setupUserForTest(t, "admin1", true, env.db)

	tests := []struct {
		name            string
		dbName          string
		key             string
		user            string
		expectedPayload *types.GetDataWritersResponse
		expectedError   string
	}{
		{
			name:   "fetch readers of key1",
			dbName: "db1",
			key:    "key1",
			user:   "admin1",
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
			user:   "admin1",
			expectedPayload: &types.GetDataWritersResponse{
				WrittenBy: nil,
			},
		},
		{
			name:            "fetching readers of key1 by an non-admin user will result in an error",
			dbName:          "db1",
			key:             "key1",
			user:            "user1",
			expectedPayload: nil,
			expectedError:   "The querier [user1] is not an admin. Only an admin can query historical data",
		},
	}

	for _, tt := range tests {
		envelope, err := env.p.GetWriters(tt.user, tt.dbName, tt.key)
		if tt.expectedError != "" {
			require.EqualError(t, err, tt.expectedError)
			require.Nil(t, envelope)
			continue
		}

		require.NoError(t, err)
		require.NotNil(t, envelope)
		require.Equal(t, tt.expectedPayload, envelope)
	}
}

func setupUserForTest(t *testing.T, userID string, admin bool, db worldstate.DB) {
	user := &types.User{
		Id: userID,
		Privilege: &types.Privilege{
			Admin: admin,
		},
	}

	u, err := proto.Marshal(user)
	require.NoError(t, err)

	createUser := map[string]*worldstate.DBUpdates{
		worldstate.UsersDBName: {
			Writes: []*worldstate.KVWithMetadata{
				{
					Key:   string(identity.UserNamespace) + userID,
					Value: u,
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 2,
							TxNum:    1,
						},
					},
				},
			},
		},
	}
	require.NoError(t, db.Commit(createUser, 2))
}

func TestGetValuesReadByUser(t *testing.T) {
	env := newProvenanceQueryProcessorTestEnv(t)
	defer env.cleanup(t)

	setupProvenanceStore(t, env.p.provenanceStore)

	setupUserForTest(t, "user1", false, env.db)
	setupUserForTest(t, "admin1", true, env.db)
	setupUserForTest(t, "user5", false, env.db)

	tests := []struct {
		name            string
		targetUser      string
		querierUsers    []string
		expectedPayload *types.GetDataProvenanceResponse
		expectedError   error
	}{
		{
			name:         "fetch values read by user1",
			targetUser:   "user1",
			querierUsers: []string{"user1", "admin1"},
			expectedPayload: &types.GetDataProvenanceResponse{
				DBKeyValues: map[string]*types.KVsWithMetadata{
					"db1": {
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
					"db2": {
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
			},
		},
		{
			name:         "fetch values read by user5",
			targetUser:   "user5",
			querierUsers: []string{"user5", "admin1"},
			expectedPayload: &types.GetDataProvenanceResponse{
				DBKeyValues: nil,
			},
		},
		{
			name:            "fetch values read by user5 with no permission",
			targetUser:      "user5",
			querierUsers:    []string{"user1"},
			expectedPayload: nil,
			expectedError:   &ierrors.PermissionErr{ErrMsg: "The querier [user1] is neither an admin nor requesting operations performed by [user1]. Only an admin can query operations performed by other users."},
		},
	}

	for _, tt := range tests {
		for _, querier := range tt.querierUsers {
			envelope, err := env.p.GetValuesReadByUser(querier, tt.targetUser)
			if tt.expectedError != nil {
				require.Equal(t, err, tt.expectedError)
				require.Nil(t, envelope)
				continue
			}

			require.NoError(t, err)
			require.NotNil(t, envelope)
			require.Len(t, envelope.DBKeyValues, len(tt.expectedPayload.DBKeyValues))
			for dbName, expectedKVs := range tt.expectedPayload.DBKeyValues {
				require.ElementsMatch(t, expectedKVs.KVs, envelope.DBKeyValues[dbName].KVs)
			}
		}
	}
}

func TestGetValuesWrittenByUser(t *testing.T) {
	env := newProvenanceQueryProcessorTestEnv(t)
	defer env.cleanup(t)

	setupProvenanceStore(t, env.p.provenanceStore)

	setupUserForTest(t, "user1", false, env.db)
	setupUserForTest(t, "admin1", true, env.db)
	setupUserForTest(t, "user5", false, env.db)

	tests := []struct {
		name            string
		targetUser      string
		querierUsers    []string
		expectedPayload *types.GetDataProvenanceResponse
		expectedError   error
	}{
		{
			name:         "fetch values written by user1",
			targetUser:   "user1",
			querierUsers: []string{"user1", "admin1"},
			expectedPayload: &types.GetDataProvenanceResponse{
				DBKeyValues: map[string]*types.KVsWithMetadata{
					"db1": {
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
					"db2": {
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
			},
		},
		{
			name:       "fetch values written by user5",
			targetUser: "user5",
			expectedPayload: &types.GetDataProvenanceResponse{
				DBKeyValues: nil,
			},
		},
		{
			name:            "fetch values written by user1 with other users: error case",
			targetUser:      "user1",
			querierUsers:    []string{"user5"},
			expectedPayload: nil,
			expectedError:   &ierrors.PermissionErr{ErrMsg: "The querier [user5] is neither an admin nor requesting operations performed by [user5]. Only an admin can query operations performed by other users."},
		},
	}

	for _, tt := range tests {
		for _, querierUser := range tt.querierUsers {
			payload, err := env.p.GetValuesWrittenByUser(querierUser, tt.targetUser)
			if tt.expectedError != nil {
				require.Equal(t, err, tt.expectedError)
				require.Nil(t, payload)
				continue
			}

			require.NoError(t, err)
			require.NotNil(t, payload)
			require.Len(t, payload.DBKeyValues, len(tt.expectedPayload.DBKeyValues))
			for dbName, expectedKVs := range tt.expectedPayload.DBKeyValues {
				require.ElementsMatch(t, expectedKVs.KVs, payload.DBKeyValues[dbName].KVs)
			}
		}
	}
}

func TestGetValuesDeletedByUser(t *testing.T) {
	env := newProvenanceQueryProcessorTestEnv(t)
	defer env.cleanup(t)

	setupProvenanceStore(t, env.p.provenanceStore)

	setupUserForTest(t, "user2", false, env.db)
	setupUserForTest(t, "admin1", true, env.db)
	setupUserForTest(t, "user5", false, env.db)

	tests := []struct {
		name            string
		targetUser      string
		querierUsers    []string
		expectedPayload *types.GetDataProvenanceResponse
		expectedError   error
	}{
		{
			name:         "fetch values deleted by user1",
			targetUser:   "user2",
			querierUsers: []string{"user2", "admin1"},
			expectedPayload: &types.GetDataProvenanceResponse{
				DBKeyValues: map[string]*types.KVsWithMetadata{
					"db1": {
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
										BlockNum: 5,
										TxNum:    0,
									},
								},
							},
						},
					},
					"db2": {
						KVs: []*types.KVWithMetadata{
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
			},
		},
		{
			name:         "fetch values deleted by user5",
			targetUser:   "user5",
			querierUsers: []string{"user5", "admin1"},
			expectedPayload: &types.GetDataProvenanceResponse{
				DBKeyValues: nil,
			},
		},
		{
			name:            "fetch values deleted by user1 with other users: error case",
			targetUser:      "user2",
			querierUsers:    []string{"user5"},
			expectedPayload: nil,
			expectedError:   &ierrors.PermissionErr{ErrMsg: "The querier [user5] is neither an admin nor requesting operations performed by [user5]. Only an admin can query operations performed by other users."},
		},
	}

	for _, tt := range tests {
		for _, querierUser := range tt.querierUsers {
			payload, err := env.p.GetValuesDeletedByUser(querierUser, tt.targetUser)
			if tt.expectedError != nil {
				require.Equal(t, err, tt.expectedError)
				require.Nil(t, payload)
				continue
			}

			require.NoError(t, err)
			require.NotNil(t, payload)
			require.Len(t, payload.DBKeyValues, len(tt.expectedPayload.DBKeyValues))
			for dbName, expectedKVs := range tt.expectedPayload.DBKeyValues {
				require.ElementsMatch(t, expectedKVs.KVs, payload.DBKeyValues[dbName].KVs)
			}
		}
	}
}

func TestGetTxSubmittedByUser(t *testing.T) {
	env := newProvenanceQueryProcessorTestEnv(t)
	defer env.cleanup(t)

	setupProvenanceStore(t, env.p.provenanceStore)

	setupUserForTest(t, "user2", false, env.db)
	setupUserForTest(t, "admin1", true, env.db)
	setupUserForTest(t, "user5", false, env.db)

	tests := []struct {
		name            string
		targetUser      string
		querierUsers    []string
		expectedPayload *types.GetTxIDsSubmittedByResponse
		expectedError   error
	}{
		{
			name:         "fetch tx submitted by user",
			targetUser:   "user2",
			querierUsers: []string{"user2", "admin1"},
			expectedPayload: &types.GetTxIDsSubmittedByResponse{
				TxIDs: []string{"tx5", "tx50", "tx6"},
			},
		},
		{
			name:         "fetch tx submitted by user - empty",
			targetUser:   "user5",
			querierUsers: []string{"user5", "admin1"},
			expectedPayload: &types.GetTxIDsSubmittedByResponse{
				TxIDs: nil,
			},
		},
		{
			name:            "fetch tx submitted by user2 from other user - error case",
			targetUser:      "user2",
			querierUsers:    []string{"user5"},
			expectedPayload: nil,
			expectedError:   &ierrors.PermissionErr{ErrMsg: "The querier [user5] is neither an admin nor requesting operations performed by [user5]. Only an admin can query operations performed by other users."},
		},
	}

	for _, tt := range tests {
		for _, querierUser := range tt.querierUsers {
			payload, err := env.p.GetTxIDsSubmittedByUser(querierUser, tt.targetUser)
			if tt.expectedError != nil {
				require.Equal(t, err, tt.expectedError)
				require.Nil(t, payload)
				continue
			}

			require.NoError(t, err)
			require.NotNil(t, payload)
			require.Equal(t, tt.expectedPayload, payload)
		}
	}
}

func TestDisabledStore(t *testing.T) {
	env := newProvenanceQueryProcessorTestEnv(t)
	defer env.cleanup(t)

	env.p.provenanceStore = nil

	_, err := env.p.GetReaders("user", "db", "key")
	require.EqualError(t, err, "provenance store is disabled on this server")
	_, err = env.p.GetWriters("user", "db", "key")
	require.EqualError(t, err, "provenance store is disabled on this server")

	_, err = env.p.GetValues("user", "db", "key")
	require.EqualError(t, err, "provenance store is disabled on this server")
	_, err = env.p.GetDeletedValues("user", "db", "key")
	require.EqualError(t, err, "provenance store is disabled on this server")
	_, err = env.p.GetNextValues("user", "db", "key", &types.Version{BlockNum: 1, TxNum: 1})
	require.EqualError(t, err, "provenance store is disabled on this server")
	_, err = env.p.GetPreviousValues("user", "db", "key", &types.Version{BlockNum: 1, TxNum: 1})
	require.EqualError(t, err, "provenance store is disabled on this server")
	_, err = env.p.GetMostRecentValueAtOrBelow("user", "db", "key", &types.Version{BlockNum: 1, TxNum: 1})
	require.EqualError(t, err, "provenance store is disabled on this server")
	_, err = env.p.GetValueAt("user", "db", "key", &types.Version{BlockNum: 1, TxNum: 1})
	require.EqualError(t, err, "provenance store is disabled on this server")

	_, err = env.p.GetValuesDeletedByUser("user", "user")
	require.EqualError(t, err, "provenance store is disabled on this server")
	_, err = env.p.GetValuesReadByUser("user", "user")
	require.EqualError(t, err, "provenance store is disabled on this server")
	_, err = env.p.GetValuesWrittenByUser("user", "user")
	require.EqualError(t, err, "provenance store is disabled on this server")

	_, err = env.p.GetTxIDsSubmittedByUser("user", "user")
	require.EqualError(t, err, "provenance store is disabled on this server")
}
