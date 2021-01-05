package bcdb

import (
	"crypto/x509"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/server/internal/provenance"
	"github.ibm.com/blockchaindb/server/pkg/logger"
	"github.ibm.com/blockchaindb/server/pkg/server/testutils"
	"github.ibm.com/blockchaindb/server/pkg/types"
)

type provenanceQueryProcessorTestEnv struct {
	p       *provenanceQueryProcessor
	cert    *x509.Certificate
	cleanup func(t *testing.T)
}

func newProvenanceQueryProcessorTestEnv(t *testing.T) *provenanceQueryProcessorTestEnv {
	nodeID := "test-node-id1"
	cryptoPath := testutils.GenerateTestClientCrypto(t, []string{nodeID})
	nodeCert, nodeSigner := testutils.LoadTestClientCrypto(t, cryptoPath, nodeID)

	path, err := ioutil.TempDir("/tmp", "provenanceQueryProcessor")
	require.NoError(t, err)

	c := &logger.Config{
		Level:         "debug",
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
				nodeID:          nodeID,
				signer:          nodeSigner,
				provenanceStore: provenanceStore,
				logger:          logger,
			}),
		cert:    nodeCert,
		cleanup: cleanup,
	}
}

func setupProvenanceStore(t *testing.T, p *provenance.Store) {
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
			DBName:  "db1",
			UserID:  "user2",
			TxID:    "tx4",
			Reads: []*provenance.KeyWithVersion{
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

	require.NoError(t, p.Commit(1, block1TxsData))
	require.NoError(t, p.Commit(2, block2TxsData))
	require.NoError(t, p.Commit(3, block3TxsData))
}

func TestGetValues(t *testing.T) {
	t.Parallel()
	env := newProvenanceQueryProcessorTestEnv(t)
	defer env.cleanup(t)

	setupProvenanceStore(t, env.p.provenanceStore)

	tests := []struct {
		name             string
		dbName           string
		key              string
		expectedEnvelope *types.GetHistoricalDataResponseEnvelope
	}{
		{
			name:   "fetch all values of key1",
			dbName: "db1",
			key:    "key1",
			expectedEnvelope: &types.GetHistoricalDataResponseEnvelope{
				Payload: &types.GetHistoricalDataResponse{
					Header: &types.ResponseHeader{
						NodeID: env.p.nodeID,
					},
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
			},
		},
		{
			name:   "fetch all values of non-existing key",
			dbName: "db1",
			key:    "key5",
			expectedEnvelope: &types.GetHistoricalDataResponseEnvelope{
				Payload: &types.GetHistoricalDataResponse{
					Header: &types.ResponseHeader{
						NodeID: env.p.nodeID,
					},
					Values: nil,
				},
			},
		},
	}

	for _, tt := range tests {
		envelope, err := env.p.GetValues(tt.dbName, tt.key)
		require.NoError(t, err)

		require.NotNil(t, envelope)
		require.ElementsMatch(t, tt.expectedEnvelope.GetPayload().GetValues(), envelope.GetPayload().GetValues())
		require.Equal(t, tt.expectedEnvelope.GetPayload().GetHeader(), envelope.GetPayload().GetHeader())
		testutils.VerifyPayloadSignature(t, env.cert.Raw, envelope.GetPayload(), envelope.GetSignature())
	}
}

func TestGetPreviousValues(t *testing.T) {
	t.Parallel()
	env := newProvenanceQueryProcessorTestEnv(t)
	defer env.cleanup(t)

	setupProvenanceStore(t, env.p.provenanceStore)

	tests := []struct {
		name             string
		dbName           string
		key              string
		version          *types.Version
		expectedEnvelope *types.GetHistoricalDataResponseEnvelope
	}{
		{
			name:   "fetch the previous value of key1 at version{Blk 2, txNum 1}",
			dbName: "db1",
			key:    "key1",
			version: &types.Version{
				BlockNum: 2,
				TxNum:    1,
			},
			expectedEnvelope: &types.GetHistoricalDataResponseEnvelope{
				Payload: &types.GetHistoricalDataResponse{
					Header: &types.ResponseHeader{
						NodeID: env.p.nodeID,
					},
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
		},
		{
			name:   "fetch the previous value of non-existing key",
			dbName: "db1",
			key:    "key5",
			version: &types.Version{
				BlockNum: 1,
				TxNum:    1,
			},
			expectedEnvelope: &types.GetHistoricalDataResponseEnvelope{
				Payload: &types.GetHistoricalDataResponse{
					Header: &types.ResponseHeader{
						NodeID: env.p.nodeID,
					},
				},
			},
		},
	}

	for _, tt := range tests {
		envelope, err := env.p.GetPreviousValues(tt.dbName, tt.key, tt.version)
		require.NoError(t, err)

		require.NotNil(t, envelope)
		require.Equal(t, tt.expectedEnvelope.GetPayload(), envelope.GetPayload())
		testutils.VerifyPayloadSignature(t, env.cert.Raw, envelope.GetPayload(), envelope.GetSignature())
	}
}

func TestGetNextValues(t *testing.T) {
	t.Parallel()
	env := newProvenanceQueryProcessorTestEnv(t)
	defer env.cleanup(t)

	setupProvenanceStore(t, env.p.provenanceStore)

	tests := []struct {
		name             string
		dbName           string
		key              string
		version          *types.Version
		expectedEnvelope *types.GetHistoricalDataResponseEnvelope
	}{
		{
			name:   "fetch next value of key1 at version {Blk 2, txNum 1}",
			dbName: "db1",
			key:    "key1",
			version: &types.Version{
				BlockNum: 2,
				TxNum:    1,
			},
			expectedEnvelope: &types.GetHistoricalDataResponseEnvelope{
				Payload: &types.GetHistoricalDataResponse{
					Header: &types.ResponseHeader{
						NodeID: env.p.nodeID,
					},
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
		},
		{
			name:   "fetch next value of non-existing key",
			dbName: "db1",
			key:    "key5",
			version: &types.Version{
				BlockNum: 2,
				TxNum:    1,
			},
			expectedEnvelope: &types.GetHistoricalDataResponseEnvelope{
				Payload: &types.GetHistoricalDataResponse{
					Header: &types.ResponseHeader{
						NodeID: env.p.nodeID,
					},
					Values: nil,
				},
			},
		},
	}

	for _, tt := range tests {
		envelope, err := env.p.GetNextValues(tt.dbName, tt.key, tt.version)
		require.NoError(t, err)

		require.NotNil(t, envelope)
		require.Equal(t, tt.expectedEnvelope.GetPayload(), envelope.GetPayload())
		testutils.VerifyPayloadSignature(t, env.cert.Raw, envelope.GetPayload(), envelope.GetSignature())
	}
}

func TestGetValueAt(t *testing.T) {
	t.Parallel()
	env := newProvenanceQueryProcessorTestEnv(t)
	defer env.cleanup(t)

	setupProvenanceStore(t, env.p.provenanceStore)

	tests := []struct {
		name             string
		dbName           string
		key              string
		version          *types.Version
		expectedEnvelope *types.GetHistoricalDataResponseEnvelope
	}{
		{
			name:   "fetch value of key1 at a particular version",
			dbName: "db1",
			key:    "key1",
			version: &types.Version{
				BlockNum: 2,
				TxNum:    1,
			},
			expectedEnvelope: &types.GetHistoricalDataResponseEnvelope{
				Payload: &types.GetHistoricalDataResponse{
					Header: &types.ResponseHeader{
						NodeID: env.p.nodeID,
					},
					Values: []*types.ValueWithMetadata{
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
			expectedEnvelope: &types.GetHistoricalDataResponseEnvelope{
				Payload: &types.GetHistoricalDataResponse{
					Header: &types.ResponseHeader{
						NodeID: env.p.nodeID,
					},
					Values: nil,
				},
			},
		},
	}

	for _, tt := range tests {
		envelope, err := env.p.GetValueAt(tt.dbName, tt.key, tt.version)
		require.NoError(t, err)

		require.NotNil(t, envelope)
		require.Equal(t, tt.expectedEnvelope.GetPayload(), envelope.GetPayload())
		testutils.VerifyPayloadSignature(t, env.cert.Raw, envelope.GetPayload(), envelope.GetSignature())
	}
}

func TestGetReaders(t *testing.T) {
	t.Parallel()
	env := newProvenanceQueryProcessorTestEnv(t)
	defer env.cleanup(t)

	setupProvenanceStore(t, env.p.provenanceStore)

	tests := []struct {
		name             string
		dbName           string
		key              string
		expectedEnvelope *types.GetDataReadersResponseEnvelope
	}{
		{
			name:   "fetch readers of key1",
			dbName: "db1",
			key:    "key1",
			expectedEnvelope: &types.GetDataReadersResponseEnvelope{
				Payload: &types.GetDataReadersResponse{
					Header: &types.ResponseHeader{
						NodeID: env.p.nodeID,
					},
					ReadBy: map[string]uint32{
						"user1": 1,
						"user2": 1,
					},
				},
			},
		},
		{
			name:   "fetch readers of non-existing key",
			dbName: "db1",
			key:    "key5",
			expectedEnvelope: &types.GetDataReadersResponseEnvelope{
				Payload: &types.GetDataReadersResponse{
					Header: &types.ResponseHeader{
						NodeID: env.p.nodeID,
					},
					ReadBy: nil,
				},
			},
		},
	}

	for _, tt := range tests {
		envelope, err := env.p.GetReaders(tt.dbName, tt.key)
		require.NoError(t, err)

		require.NotNil(t, envelope)
		require.Equal(t, tt.expectedEnvelope.GetPayload(), envelope.GetPayload())
		testutils.VerifyPayloadSignature(t, env.cert.Raw, envelope.GetPayload(), envelope.GetSignature())
	}
}

func TestGetWriters(t *testing.T) {
	t.Parallel()
	env := newProvenanceQueryProcessorTestEnv(t)
	defer env.cleanup(t)

	setupProvenanceStore(t, env.p.provenanceStore)

	tests := []struct {
		name             string
		dbName           string
		key              string
		expectedEnvelope *types.GetDataWritersResponseEnvelope
	}{
		{
			name:   "fetch readers of key1",
			dbName: "db1",
			key:    "key1",
			expectedEnvelope: &types.GetDataWritersResponseEnvelope{
				Payload: &types.GetDataWritersResponse{
					Header: &types.ResponseHeader{
						NodeID: env.p.nodeID,
					},
					WrittenBy: map[string]uint32{
						"user1": 2,
						"user2": 2,
					},
				},
			},
		},
		{
			name:   "fetch readers of non-existing key",
			dbName: "db1",
			key:    "key5",
			expectedEnvelope: &types.GetDataWritersResponseEnvelope{
				Payload: &types.GetDataWritersResponse{
					Header: &types.ResponseHeader{
						NodeID: env.p.nodeID,
					},
					WrittenBy: nil,
				},
			},
		},
	}

	for _, tt := range tests {
		envelope, err := env.p.GetWriters(tt.dbName, tt.key)
		require.NoError(t, err)
		require.NotNil(t, envelope)
		require.Equal(t, tt.expectedEnvelope.GetPayload(), envelope.GetPayload())
		testutils.VerifyPayloadSignature(t, env.cert.Raw, envelope.GetPayload(), envelope.GetSignature())
	}
}

func TestGetValuesReadByUser(t *testing.T) {
	t.Parallel()
	env := newProvenanceQueryProcessorTestEnv(t)
	defer env.cleanup(t)

	setupProvenanceStore(t, env.p.provenanceStore)

	tests := []struct {
		name             string
		user             string
		expectedEnvelope *types.GetDataReadByResponseEnvelope
	}{
		{
			name: "fetch values read by user1",
			user: "user1",
			expectedEnvelope: &types.GetDataReadByResponseEnvelope{
				Payload: &types.GetDataReadByResponse{
					Header: &types.ResponseHeader{
						NodeID: env.p.nodeID,
					},
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
				Signature: nil,
			},
		},
		{
			name: "fetch values read by user5",
			user: "user5",
			expectedEnvelope: &types.GetDataReadByResponseEnvelope{
				Payload: &types.GetDataReadByResponse{
					Header: &types.ResponseHeader{
						NodeID: env.p.nodeID,
					},
					KVs: nil,
				},
				Signature: nil,
			},
		},
	}

	for _, tt := range tests {
		envelope, err := env.p.GetValuesReadByUser(tt.user)
		require.NoError(t, err)

		require.NotNil(t, envelope)
		require.Equal(t, tt.expectedEnvelope.GetPayload(), envelope.GetPayload())
		testutils.VerifyPayloadSignature(t, env.cert.Raw, envelope.GetPayload(), envelope.GetSignature())
	}
}

func TestGetValuesWrittenByUser(t *testing.T) {
	t.Parallel()
	env := newProvenanceQueryProcessorTestEnv(t)
	defer env.cleanup(t)

	setupProvenanceStore(t, env.p.provenanceStore)

	tests := []struct {
		name             string
		user             string
		expectedEnvelope *types.GetDataWrittenByResponseEnvelope
	}{
		{
			name: "fetch values read by user1",
			user: "user1",
			expectedEnvelope: &types.GetDataWrittenByResponseEnvelope{
				Payload: &types.GetDataWrittenByResponse{
					Header: &types.ResponseHeader{
						NodeID: env.p.nodeID,
					},
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
		},
		{
			name: "fetch values read by user5",
			user: "user5",
			expectedEnvelope: &types.GetDataWrittenByResponseEnvelope{
				Payload: &types.GetDataWrittenByResponse{
					Header: &types.ResponseHeader{
						NodeID: env.p.nodeID,
					},
					KVs: nil,
				},
				Signature: nil,
			},
		},
	}

	for _, tt := range tests {
		envelope, err := env.p.GetValuesWrittenByUser(tt.user)
		require.NoError(t, err)

		require.NotNil(t, envelope)
		require.Equal(t, tt.expectedEnvelope.GetPayload(), envelope.GetPayload())
		testutils.VerifyPayloadSignature(t, env.cert.Raw, envelope.GetPayload(), envelope.GetSignature())
	}
}

func TestGetTxSubmittedByUser(t *testing.T) {
	t.Parallel()
	env := newProvenanceQueryProcessorTestEnv(t)
	defer env.cleanup(t)

	setupProvenanceStore(t, env.p.provenanceStore)

	tests := []struct {
		name             string
		user             string
		expectedEnvelope *types.GetTxIDsSubmittedByResponseEnvelope
	}{
		{
			name: "fetch tx submitted by user",
			user: "user2",
			expectedEnvelope: &types.GetTxIDsSubmittedByResponseEnvelope{
				Payload: &types.GetTxIDsSubmittedByResponse{
					Header: &types.ResponseHeader{
						NodeID: env.p.nodeID,
					},
					TxIDs: []string{"tx4", "tx5"},
				},
			},
		},
		{
			name: "fetch tx submitted by user - empty",
			user: "user5",
			expectedEnvelope: &types.GetTxIDsSubmittedByResponseEnvelope{
				Payload: &types.GetTxIDsSubmittedByResponse{
					Header: &types.ResponseHeader{
						NodeID: env.p.nodeID,
					},
					TxIDs: nil,
				},
			},
		},
	}

	for _, tt := range tests {
		envelope, err := env.p.GetTxIDsSubmittedByUser(tt.user)
		require.NoError(t, err)

		require.NotNil(t, envelope)
		require.Equal(t, tt.expectedEnvelope.GetPayload(), envelope.GetPayload())
		testutils.VerifyPayloadSignature(t, env.cert.Raw, envelope.GetPayload(), envelope.GetSignature())
	}
}
