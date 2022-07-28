// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package queries

import (
	"io/ioutil"
	"sort"
	"testing"
	"time"

	"github.com/hyperledger-labs/orion-server/pkg/constants"
	"github.com/hyperledger-labs/orion-server/pkg/server/testutils"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/hyperledger-labs/orion-server/test/setup"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestProvenanceQueries(t *testing.T) {
	dir, err := ioutil.TempDir("", "int-test")
	require.NoError(t, err)

	nPort, pPort := getPorts(1)
	setupConfig := &setup.Config{
		NumberOfServers:     1,
		TestDirAbsolutePath: dir,
		BDBBinaryPath:       "../../bin/bdb",
		CmdTimeout:          10 * time.Second,
		BaseNodePort:        nPort,
		BasePeerPort:        pPort,
	}
	// defer os.RemoveAll(dir)
	c, err := setup.NewCluster(setupConfig)
	require.NoError(t, err)
	defer c.ShutdownAndCleanup()

	require.NoError(t, c.Start())
	leaderIndex := -1
	require.Eventually(t, func() bool {
		leaderIndex = c.AgreedLeader(t, 0)
		return leaderIndex >= 0
	}, 30*time.Second, 100*time.Millisecond)

	s := c.Servers[0]

	// creating two databases -- db1 and db2
	setup.CreateDatabases(t, s, []string{"db1", "db2"}, nil)

	// creating two users -- alice and bob
	aliceCert, _ := testutils.LoadTestCrypto(t, c.GetUserCertDir(), "alice")
	bobCert, _ := testutils.LoadTestCrypto(t, c.GetUserCertDir(), "bob")
	setup.CreateUsers(t, s, []*types.UserWrite{
		{
			User: &types.User{
				Id:          "alice",
				Certificate: aliceCert.Raw,
				Privilege: &types.Privilege{
					DbPermission: map[string]types.Privilege_Access{
						"db1": types.Privilege_ReadWrite,
						"db2": types.Privilege_ReadWrite,
					},
				},
			},
		},
		{
			User: &types.User{
				Id:          "bob",
				Certificate: bobCert.Raw,
				Privilege: &types.Privilege{
					DbPermission: map[string]types.Privilege_Access{
						"db1": types.Privilege_ReadWrite,
						"db2": types.Privilege_ReadWrite,
					},
				},
			},
		},
	})

	prepareDataForProvenanceQueries(t, s)

	t.Run("Get all values", func(t *testing.T) {
		expectedKey1Values := &types.GetHistoricalDataResponse{
			Header: &types.ResponseHeader{
				NodeId: s.ID(),
			},
			Values: []*types.ValueWithMetadata{
				{
					Value: []byte("key1value1"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 4,
							TxNum:    0,
						},
					},
				},
				{
					Value: []byte("key1value2"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 5,
							TxNum:    0,
						},
					},
				},
				{
					Value: []byte("key1value3"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 8,
							TxNum:    0,
						},
					},
				},
			},
		}
		resp, err := s.GetAllValues(t, "db1", "key1", "admin")
		require.NoError(t, err)
		sort.Slice(resp.GetResponse().GetValues(), func(i, j int) bool {
			return resp.GetResponse().GetValues()[i].Metadata.Version.BlockNum < resp.GetResponse().GetValues()[j].Metadata.Version.BlockNum
		})
		require.True(t, proto.Equal(expectedKey1Values, resp.GetResponse()))

		expectedKey2Values := &types.GetHistoricalDataResponse{
			Header: &types.ResponseHeader{
				NodeId: s.ID(),
			},
			Values: []*types.ValueWithMetadata{
				{
					Value: []byte("key2value1"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 4,
							TxNum:    0,
						},
						AccessControl: &types.AccessControl{
							ReadWriteUsers: map[string]bool{"alice": true},
						},
					},
				},
				{
					Value: []byte("key2value2"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 6,
							TxNum:    0,
						},
					},
				},
				{
					Value: []byte("key2value3"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 7,
							TxNum:    0,
						},
					},
				},
			},
		}
		resp, err = s.GetAllValues(t, "db1", "key2", "admin")
		require.NoError(t, err)
		sort.Slice(resp.GetResponse().GetValues(), func(i, j int) bool {
			return resp.GetResponse().GetValues()[i].Metadata.Version.BlockNum < resp.GetResponse().GetValues()[j].Metadata.Version.BlockNum
		})
		require.True(t, proto.Equal(expectedKey2Values, resp.GetResponse()))
	})

	t.Run("Get value at", func(t *testing.T) {
		expectedKey2Values := &types.GetHistoricalDataResponse{
			Header: &types.ResponseHeader{
				NodeId: s.ID(),
			},
			Values: []*types.ValueWithMetadata{
				{
					Value: []byte("key2value2"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 6,
							TxNum:    0,
						},
					},
				},
			},
		}

		ver := &types.Version{
			BlockNum: 6,
			TxNum:    0,
		}
		resp, err := s.GetValueAt(t, "db1", "key2", "admin", ver)
		require.NoError(t, err)
		require.True(t, proto.Equal(expectedKey2Values, resp.GetResponse()))
	})

	t.Run("Get next values", func(t *testing.T) {
		expectedKey2Values := &types.GetHistoricalDataResponse{
			Header: &types.ResponseHeader{
				NodeId: s.ID(),
			},
			Values: []*types.ValueWithMetadata{
				{
					Value: []byte("key2value3"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 7,
							TxNum:    0,
						},
					},
				},
			},
		}

		ver := &types.Version{
			BlockNum: 6,
			TxNum:    0,
		}
		resp, err := s.GetNextValues(t, "db1", "key2", "admin", ver)
		require.NoError(t, err)
		require.True(t, proto.Equal(expectedKey2Values, resp.GetResponse()))

		ver = &types.Version{
			BlockNum: 7,
			TxNum:    0,
		}
		resp, err = s.GetNextValues(t, "db1", "key2", "admin", ver)
		require.NoError(t, err)
		require.Nil(t, resp.GetResponse().GetValues())
	})

	t.Run("Get next values with gap due to deletes", func(t *testing.T) {
		expectedKey2Values := &types.GetHistoricalDataResponse{
			Header: &types.ResponseHeader{
				NodeId: s.ID(),
			},
			Values: []*types.ValueWithMetadata{
				{
					Value: []byte("key1value2"), // deleted
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 5,
							TxNum:    0,
						},
					},
				},
				{
					Value: []byte("key1value3"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 8,
							TxNum:    0,
						},
					},
				},
			},
		}

		ver := &types.Version{
			BlockNum: 4,
			TxNum:    0,
		}
		resp, err := s.GetNextValues(t, "db1", "key1", "admin", ver)
		require.NoError(t, err)
		sort.Slice(resp.GetResponse().GetValues(), func(i, j int) bool {
			return resp.GetResponse().GetValues()[i].Metadata.Version.BlockNum < resp.GetResponse().GetValues()[j].Metadata.Version.BlockNum
		})
		require.True(t, proto.Equal(expectedKey2Values, resp.GetResponse()))
	})

	t.Run("Get previous values", func(t *testing.T) {
		expectedKey2Values := &types.GetHistoricalDataResponse{
			Header: &types.ResponseHeader{
				NodeId: s.ID(),
			},
			Values: []*types.ValueWithMetadata{
				{
					Value: []byte("key2value1"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 4,
							TxNum:    0,
						},
						AccessControl: &types.AccessControl{
							ReadWriteUsers: map[string]bool{"alice": true},
						},
					},
				},
				{
					Value: []byte("key2value2"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 6,
							TxNum:    0,
						},
					},
				},
			},
		}

		ver := &types.Version{
			BlockNum: 7,
			TxNum:    0,
		}
		resp, err := s.GetPreviousValues(t, "db1", "key2", "admin", ver)
		require.NoError(t, err)
		sort.Slice(resp.GetResponse().GetValues(), func(i, j int) bool {
			return resp.GetResponse().GetValues()[i].Metadata.Version.BlockNum < resp.GetResponse().GetValues()[j].Metadata.Version.BlockNum
		})
		require.True(t, proto.Equal(expectedKey2Values, resp.GetResponse()))

		ver = &types.Version{
			BlockNum: 4,
			TxNum:    0,
		}
		resp, err = s.GetPreviousValues(t, "db1", "key2", "admin", ver)
		require.NoError(t, err)
		require.Nil(t, resp.GetResponse().GetValues())
	})

	t.Run("Get previous values with gap due to deletes", func(t *testing.T) {
		expectedKey2Values := &types.GetHistoricalDataResponse{
			Header: &types.ResponseHeader{
				NodeId: s.ID(),
			},
			Values: []*types.ValueWithMetadata{
				{
					Value: []byte("key1value1"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 4,
							TxNum:    0,
						},
					},
				},
				{
					Value: []byte("key1value2"), // deleted
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 5,
							TxNum:    0,
						},
					},
				},
			},
		}

		ver := &types.Version{
			BlockNum: 8,
			TxNum:    0,
		}
		resp, err := s.GetPreviousValues(t, "db1", "key1", "admin", ver)
		require.NoError(t, err)
		sort.Slice(resp.GetResponse().GetValues(), func(i, j int) bool {
			return resp.GetResponse().GetValues()[i].Metadata.Version.BlockNum < resp.GetResponse().GetValues()[j].Metadata.Version.BlockNum
		})
		require.True(t, proto.Equal(expectedKey2Values, resp.GetResponse()))
	})

	t.Run("Get Most Recent Value at or Below", func(t *testing.T) {
		expectedKey3Values := &types.GetHistoricalDataResponse{
			Header: &types.ResponseHeader{
				NodeId: s.ID(),
			},
			Values: []*types.ValueWithMetadata{
				{
					Value: []byte("key3value2"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 7,
							TxNum:    0,
						},
						AccessControl: &types.AccessControl{
							ReadWriteUsers: map[string]bool{"alice": true},
						},
					},
				},
			},
		}

		ver := &types.Version{
			BlockNum: 10,
			TxNum:    7,
		}
		// Note that only alice has the read and write permission on the key3 but
		// bob is able to read key3 from the provenance store because we have not
		// implemented ACL in the provenance store
		resp, err := s.GetMostRecentValueAtOrBelow(t, "db2", "key3", "admin", ver)
		require.NoError(t, err)
		require.True(t, proto.Equal(expectedKey3Values, resp.GetResponse()))
		// require.ElementsMatch(t, expectedKey3Values, resp.GetResponse().GetValues())

		ver = &types.Version{
			BlockNum: 7,
			TxNum:    0,
		}
		resp, err = s.GetMostRecentValueAtOrBelow(t, "db2", "key3", "admin", ver)
		require.NoError(t, err)
		require.True(t, proto.Equal(expectedKey3Values, resp.GetResponse()))

		expectedKey3Values = &types.GetHistoricalDataResponse{
			Header: &types.ResponseHeader{
				NodeId: s.ID(),
			},
			Values: []*types.ValueWithMetadata{
				{
					Value: []byte("key3value1"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 4,
							TxNum:    0,
						},
					},
				},
			},
		}

		ver = &types.Version{
			BlockNum: 6,
			TxNum:    0,
		}
		resp, err = s.GetMostRecentValueAtOrBelow(t, "db2", "key3", "admin", ver)
		require.NoError(t, err)
		require.True(t, proto.Equal(expectedKey3Values, resp.GetResponse()))
	})

	t.Run("Get Deleted Values", func(t *testing.T) {
		expectedKey1Values := &types.GetHistoricalDataResponse{
			Header: &types.ResponseHeader{
				NodeId: s.ID(),
			},
			Values: []*types.ValueWithMetadata{
				{
					Value: []byte("key1value2"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 5,
							TxNum:    0,
						},
					},
				},
			},
		}

		resp, err := s.GetDeletedValues(t, "db1", "key1", "admin")
		require.NoError(t, err)
		require.True(t, proto.Equal(expectedKey1Values, resp.GetResponse()))
	})

	t.Run("Get Values Read By", func(t *testing.T) {
		expectedAliceReads := &types.GetDataProvenanceResponse{
			Header: &types.ResponseHeader{
				NodeId: s.ID(),
			},
			DBKeyValues: map[string]*types.KVsWithMetadata{
				"db1": {
					KVs: []*types.KVWithMetadata{
						{
							Key:   "key1",
							Value: []byte("key1value2"),
							Metadata: &types.Metadata{
								Version: &types.Version{
									BlockNum: 5,
									TxNum:    0,
								},
							},
						},
						{
							Key:   "key2",
							Value: []byte("key2value1"),
							Metadata: &types.Metadata{
								Version: &types.Version{
									BlockNum: 4,
									TxNum:    0,
								},
								AccessControl: &types.AccessControl{
									ReadWriteUsers: map[string]bool{"alice": true},
								},
							},
						},
						{
							Key:   "key2",
							Value: []byte("key2value2"),
							Metadata: &types.Metadata{
								Version: &types.Version{
									BlockNum: 6,
									TxNum:    0,
								},
							},
						},
					},
				},
				"db2": {
					KVs: []*types.KVWithMetadata{
						{
							Key:   "key3",
							Value: []byte("key3value1"),
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
		}

		resp, err := s.GetValuesReadByUser(t, "alice", "alice")
		require.NoError(t, err)
		actualDBsKVs := resp.GetResponse().GetDBKeyValues()
		require.NotNil(t, actualDBsKVs)
		require.Len(t, actualDBsKVs, len(expectedAliceReads.DBKeyValues))

		for dbName, expectedKVs := range expectedAliceReads.DBKeyValues {
			require.True(t, proto.Equal(expectedKVs, resp.GetResponse().GetDBKeyValues()[dbName]))
		}
	})

	t.Run("Get Values Written By", func(t *testing.T) {
		expectedAliceWrites := &types.GetDataProvenanceResponse{
			Header: &types.ResponseHeader{
				NodeId: s.ID(),
			},
			DBKeyValues: map[string]*types.KVsWithMetadata{
				"db1": {
					KVs: []*types.KVWithMetadata{
						{
							Key:   "key1",
							Value: []byte("key1value1"),
							Metadata: &types.Metadata{
								Version: &types.Version{
									BlockNum: 4,
									TxNum:    0,
								},
							},
						},
						{
							Key:   "key2",
							Value: []byte("key2value1"),
							Metadata: &types.Metadata{
								Version: &types.Version{
									BlockNum: 4,
									TxNum:    0,
								},
								AccessControl: &types.AccessControl{
									ReadWriteUsers: map[string]bool{"alice": true},
								},
							},
						},
						{
							Key:   "key2",
							Value: []byte("key2value2"),
							Metadata: &types.Metadata{
								Version: &types.Version{
									BlockNum: 6,
									TxNum:    0,
								},
							},
						},
						{
							Key:   "key2",
							Value: []byte("key2value3"),
							Metadata: &types.Metadata{
								Version: &types.Version{
									BlockNum: 7,
									TxNum:    0,
								},
							},
						},
					},
				},
				"db2": {
					KVs: []*types.KVWithMetadata{
						{
							Key:   "key3",
							Value: []byte("key3value1"),
							Metadata: &types.Metadata{
								Version: &types.Version{
									BlockNum: 4,
									TxNum:    0,
								},
							},
						},
						{
							Key:   "key3",
							Value: []byte("key3value2"),
							Metadata: &types.Metadata{
								Version: &types.Version{
									BlockNum: 7,
									TxNum:    0,
								},
								AccessControl: &types.AccessControl{
									ReadWriteUsers: map[string]bool{"alice": true},
								},
							},
						},
					},
				},
			},
		}

		resp, err := s.GetValuesWrittenByUser(t, "alice", "alice")
		require.NoError(t, err)
		actualDBsKVs := resp.GetResponse().GetDBKeyValues()
		require.NotNil(t, actualDBsKVs)
		require.Len(t, actualDBsKVs, len(expectedAliceWrites.DBKeyValues))

		for dbName, expectedKVs := range expectedAliceWrites.DBKeyValues {
			require.True(t, proto.Equal(expectedKVs, resp.GetResponse().GetDBKeyValues()[dbName]))
		}
	})

	t.Run("Get Values Deleted By", func(t *testing.T) {
		expectedAliceDeletes := &types.GetDataProvenanceResponse{
			Header: &types.ResponseHeader{
				NodeId: s.ID(),
			},
			DBKeyValues: map[string]*types.KVsWithMetadata{
				"db1": {
					KVs: []*types.KVWithMetadata{
						{
							Key:   "key1",
							Value: []byte("key1value2"),
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
		}

		resp, err := s.GetValuesDeletedByUser(t, "alice", "alice")
		require.NoError(t, err)
		actualDBsKVs := resp.GetResponse().GetDBKeyValues()
		require.NotNil(t, actualDBsKVs)
		require.Len(t, actualDBsKVs, len(expectedAliceDeletes.DBKeyValues))

		for dbName, expectedKVs := range expectedAliceDeletes.DBKeyValues {
			require.True(t, proto.Equal(expectedKVs, resp.GetResponse().GetDBKeyValues()[dbName]))
		}
	})

	t.Run("Get Readers", func(t *testing.T) {
		testCases := []struct {
			dbName          string
			key             string
			expectedReaders map[string]uint32
		}{
			{
				dbName: "db1",
				key:    "key1",
				expectedReaders: map[string]uint32{
					"alice": 1,
					"bob":   1,
				},
			},
			{
				dbName: "db1",
				key:    "key2",
				expectedReaders: map[string]uint32{
					"alice": 2,
				},
			},
			{
				dbName: "db2",
				key:    "key3",
				expectedReaders: map[string]uint32{
					"alice": 1,
				},
			},
		}

		for _, tt := range testCases {
			resp, err := s.GetReaders(t, tt.dbName, tt.key, "admin")
			require.NoError(t, err)
			require.Equal(t, tt.expectedReaders, resp.GetResponse().GetReadBy())
		}
	})

	t.Run("Get Writers", func(t *testing.T) {
		testCases := []struct {
			dbName          string
			key             string
			expectedWriters map[string]uint32
		}{
			{
				dbName: "db1",
				key:    "key1",
				expectedWriters: map[string]uint32{
					"alice": 1,
					"bob":   2,
				},
			},
			{
				dbName: "db2",
				key:    "key3",
				expectedWriters: map[string]uint32{
					"alice": 2,
				},
			},
		}

		for _, tt := range testCases {
			resp, err := s.GetWriters(t, tt.dbName, tt.key, "admin")
			require.NoError(t, err)
			require.Equal(t, tt.expectedWriters, resp.GetResponse().GetWrittenBy())
		}
	})

	t.Run("Get TxIDs", func(t *testing.T) {
		testCases := []struct {
			userId        string
			expectedTxIDs []string
		}{
			{
				userId:        "alice",
				expectedTxIDs: []string{"tx1", "tx3", "tx4"},
			},
			{
				userId:        "bob",
				expectedTxIDs: []string{"tx2", "tx5"},
			},
		}

		for _, tt := range testCases {
			resp, err := s.GetTxIDsSubmittedBy(t, tt.userId, tt.userId)
			require.NoError(t, err)
			require.Equal(t, tt.expectedTxIDs, resp.GetResponse().GetTxIDs())
		}
	})
}

// 1. Store key1 with open access to all users in database 1
// 2. Store key2 with read-write access only for alice in database 1
// 3. Store key3 with open acess to all users in database 2
// 4. Bob reads key1 and update the same in database 1
// 5. Alice reads key1 and key2, update key2 while deleting key1 in database 1
// 6. Alice reads key2 from database 1 and key3 from database 2 and updates key2 and key3
// 7. Bob writes key1 in database 1
func prepareDataForProvenanceQueries(t *testing.T, s *setup.Server) {
	// 1. Store key1 with open access to all users in database 1
	// 2. Store key2 with read-write access only for alice in database 1
	// 3. Store key3 with open acess to all users in database 2
	dataTx := &types.DataTx{
		MustSignUserIds: []string{
			"alice",
		},
		TxId: "tx1",
		DbOperations: []*types.DBOperation{
			{
				DbName: "db1",
				DataWrites: []*types.DataWrite{
					{
						Key:   "key1",
						Value: []byte("key1value1"),
					},
					{
						Key:   "key2",
						Value: []byte("key2value1"),
						Acl: &types.AccessControl{
							ReadWriteUsers: map[string]bool{"alice": true},
						},
					},
				},
			},
			{
				DbName: "db2",
				DataWrites: []*types.DataWrite{
					{
						Key:   "key3",
						Value: []byte("key3value1"),
					},
				},
			},
		},
	}

	aliceSigner, err := s.Signer("alice")
	require.NoError(t, err)

	_, err = s.SubmitTransaction(t, constants.PostDataTx, &types.DataTxEnvelope{
		Payload:    dataTx,
		Signatures: map[string][]byte{"alice": testutils.SignatureFromTx(t, aliceSigner, dataTx)},
	})
	require.NoError(t, err)

	// 4. Bob reads key1 and update the same in database 1
	resp, err := s.QueryData(t, "db1", "key1", "bob")
	require.NoError(t, err)
	key1Version := resp.GetResponse().GetMetadata().GetVersion()
	require.NotNil(t, key1Version)

	dataTx = &types.DataTx{
		MustSignUserIds: []string{
			"bob",
		},
		TxId: "tx2",
		DbOperations: []*types.DBOperation{
			{
				DbName: "db1",
				DataReads: []*types.DataRead{
					{
						Key:     "key1",
						Version: key1Version,
					},
				},
				DataWrites: []*types.DataWrite{
					{
						Key:   "key1",
						Value: []byte("key1value2"),
					},
				},
			},
		},
	}

	bobSigner, err := s.Signer("bob")
	require.NoError(t, err)

	_, err = s.SubmitTransaction(t, constants.PostDataTx, &types.DataTxEnvelope{
		Payload:    dataTx,
		Signatures: map[string][]byte{"bob": testutils.SignatureFromTx(t, bobSigner, dataTx)},
	})
	require.NoError(t, err)

	// 5. Alice reads key1 and key2, update key2 while deleting key1 in database 1
	resp, err = s.QueryData(t, "db1", "key1", "alice")
	require.NoError(t, err)
	key1Version = resp.GetResponse().GetMetadata().GetVersion()
	require.NotNil(t, key1Version)

	resp, err = s.QueryData(t, "db1", "key2", "alice")
	require.NoError(t, err)
	key2Version := resp.GetResponse().GetMetadata().GetVersion()
	require.NotNil(t, key2Version)

	dataTx = &types.DataTx{
		MustSignUserIds: []string{
			"alice",
		},
		TxId: "tx3",
		DbOperations: []*types.DBOperation{
			{
				DbName: "db1",
				DataReads: []*types.DataRead{
					{
						Key:     "key1",
						Version: key1Version,
					},
					{
						Key:     "key2",
						Version: key2Version,
					},
				},
				DataWrites: []*types.DataWrite{
					{
						Key:   "key2",
						Value: []byte("key2value2"),
					},
				},
				DataDeletes: []*types.DataDelete{
					{
						Key: "key1",
					},
				},
			},
		},
	}

	_, err = s.SubmitTransaction(t, constants.PostDataTx, &types.DataTxEnvelope{
		Payload:    dataTx,
		Signatures: map[string][]byte{"alice": testutils.SignatureFromTx(t, aliceSigner, dataTx)},
	})
	require.NoError(t, err)

	// 6. Alice reads key2 from database 1 and key3 from database 2 and updates key2 and key3
	resp, err = s.QueryData(t, "db1", "key2", "alice")
	require.NoError(t, err)
	key2Version = resp.GetResponse().GetMetadata().GetVersion()
	require.NotNil(t, key2Version)

	resp, err = s.QueryData(t, "db2", "key3", "alice")
	require.NoError(t, err)
	key3Version := resp.GetResponse().GetMetadata().GetVersion()
	require.NotNil(t, key3Version)

	dataTx = &types.DataTx{
		MustSignUserIds: []string{
			"alice",
		},
		TxId: "tx4",
		DbOperations: []*types.DBOperation{
			{
				DbName: "db1",
				DataReads: []*types.DataRead{
					{
						Key:     "key2",
						Version: key2Version,
					},
				},
				DataWrites: []*types.DataWrite{
					{
						Key:   "key2",
						Value: []byte("key2value3"),
					},
				},
			},
			{
				DbName: "db2",
				DataReads: []*types.DataRead{
					{
						Key:     "key3",
						Version: key3Version,
					},
				},
				DataWrites: []*types.DataWrite{
					{
						Key:   "key3",
						Value: []byte("key3value2"),
						Acl: &types.AccessControl{
							ReadWriteUsers: map[string]bool{"alice": true},
						},
					},
				},
			},
		},
	}

	_, err = s.SubmitTransaction(t, constants.PostDataTx, &types.DataTxEnvelope{
		Payload:    dataTx,
		Signatures: map[string][]byte{"alice": testutils.SignatureFromTx(t, aliceSigner, dataTx)},
	})
	require.NoError(t, err)

	// 7. Bob writes key1 in database 1
	resp, err = s.QueryData(t, "db1", "key1", "bob")
	require.NoError(t, err)
	require.Nil(t, resp.GetResponse().GetMetadata().GetVersion())

	dataTx = &types.DataTx{
		MustSignUserIds: []string{
			"bob",
		},
		TxId: "tx5",
		DbOperations: []*types.DBOperation{
			{
				DbName: "db1",
				DataReads: []*types.DataRead{
					{
						Key:     "key1",
						Version: nil,
					},
				},
				DataWrites: []*types.DataWrite{
					{
						Key:   "key1",
						Value: []byte("key1value3"),
					},
				},
			},
		},
	}

	_, err = s.SubmitTransaction(t, constants.PostDataTx, &types.DataTxEnvelope{
		Payload:    dataTx,
		Signatures: map[string][]byte{"bob": testutils.SignatureFromTx(t, bobSigner, dataTx)},
	})
	require.NoError(t, err)
}
