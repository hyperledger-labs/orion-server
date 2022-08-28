// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package queries

import (
	"io/ioutil"
	"os"
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

func TestJSONQueries(t *testing.T) {
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
	defer os.RemoveAll(dir)
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
	db1Index := map[string]types.IndexAttributeType{
		"attr1": types.IndexAttributeType_BOOLEAN,
		"attr2": types.IndexAttributeType_NUMBER,
		"attr3": types.IndexAttributeType_STRING,
	}
	db2Index := map[string]types.IndexAttributeType{
		"attr2": types.IndexAttributeType_NUMBER,
	}
	dbsIndex := map[string]*types.DBIndex{
		"db1": {
			AttributeAndType: db1Index,
		},
		"db2": {
			AttributeAndType: db2Index,
		},
	}
	setup.CreateDatabases(t, s, []string{"db1", "db2", "db3"}, dbsIndex)

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
						"db3": types.Privilege_Read,
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

	prepareDataForJSONQueries(t, s)

	defaultMetadata := &types.Metadata{
		Version: &types.Version{
			BlockNum: 4,
			TxNum:    0,
		},
	}

	t.Run("equal on boolean", func(t *testing.T) {
		query := `
		{
			"selector": {
				"attr1": {"$eq": true}
			}
		}
		`
		expectedKVsForAlice := &types.DataQueryResponse{
			Header: &types.ResponseHeader{
				NodeId: s.ID(),
			},
			KVs: []*types.KVWithMetadata{
				{
					Key:      "key1",
					Value:    []byte(v1JSON),
					Metadata: defaultMetadata,
				},
				{
					Key:      "key3",
					Value:    []byte(v3JSON),
					Metadata: defaultMetadata,
				},
				{
					Key:      "key6",
					Value:    []byte(v6JSON),
					Metadata: defaultMetadata,
				},
			},
		}

		expectedKVsForBob := &types.DataQueryResponse{
			Header: &types.ResponseHeader{
				NodeId: s.ID(),
			},
			KVs: []*types.KVWithMetadata{
				{
					Key:      "key1",
					Value:    []byte(v1JSON),
					Metadata: defaultMetadata,
				},
				{
					Key:      "key3",
					Value:    []byte(v3JSON),
					Metadata: defaultMetadata,
				},
				{
					Key:   "key5",
					Value: []byte(v5JSON),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 4,
							TxNum:    0,
						},
						AccessControl: &types.AccessControl{
							ReadWriteUsers: map[string]bool{"bob": true},
						},
					},
				},
				{
					Key:      "key6",
					Value:    []byte(v6JSON),
					Metadata: defaultMetadata,
				},
			},
		}

		resp, err := s.ExecuteJSONQuery(t, "alice", "db1", query)
		require.NoError(t, err)
		sort.Slice(resp.GetResponse().KVs, func(i, j int) bool {
			return resp.GetResponse().KVs[i].GetKey() < resp.GetResponse().KVs[j].GetKey()
		})
		require.True(t, proto.Equal(expectedKVsForAlice, resp.GetResponse()))

		resp, err = s.ExecuteJSONQuery(t, "bob", "db1", query)
		require.NoError(t, err)
		sort.Slice(resp.GetResponse().KVs, func(i, j int) bool {
			return resp.GetResponse().KVs[i].GetKey() < resp.GetResponse().KVs[j].GetKey()
		})
		require.True(t, proto.Equal(expectedKVsForBob, resp.GetResponse()))
	})

	t.Run("equal on string", func(t *testing.T) {
		query := `
		{
			"selector": {
				"attr3": {"$eq": "name3"}
			}
		}
		`
		expectedKVs := &types.DataQueryResponse{
			Header: &types.ResponseHeader{
				NodeId: s.ID(),
			},
			KVs: []*types.KVWithMetadata{
				{
					Key:      "key3",
					Value:    []byte(v3JSON),
					Metadata: defaultMetadata,
				},
			},
		}

		resp, err := s.ExecuteJSONQuery(t, "alice", "db1", query)
		require.NoError(t, err)
		require.True(t, proto.Equal(expectedKVs, resp.GetResponse()))
	})

	t.Run("equal on number", func(t *testing.T) {
		query := `
		{
			"selector": {
				"attr2": {"$eq": 100}
			}
		}
		`
		expectedKVs := &types.DataQueryResponse{
			Header: &types.ResponseHeader{
				NodeId: s.ID(),
			},
			KVs: []*types.KVWithMetadata{
				{
					Key:      "key6",
					Value:    []byte(v6JSON),
					Metadata: defaultMetadata,
				},
			},
		}

		resp, err := s.ExecuteJSONQuery(t, "alice", "db1", query)
		require.NoError(t, err)
		require.True(t, proto.Equal(expectedKVs, resp.GetResponse()))
	})

	t.Run("not equal on number", func(t *testing.T) {
		query := `
		{
			"selector": {
				"attr2": {
				"$neq": [100,110,0]
				}
			}
		}
		`

		expectedKVsForAlice := &types.DataQueryResponse{
			Header: &types.ResponseHeader{
				NodeId: s.ID(),
			},
			KVs: []*types.KVWithMetadata{
				{
					Key:      "key1",
					Value:    []byte(v1JSON),
					Metadata: defaultMetadata,
				},
				{
					Key:   "key2",
					Value: []byte(v2JSON),
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
					Key:      "key3",
					Value:    []byte(v3JSON),
					Metadata: defaultMetadata,
				},
			},
		}

		expectedKVsForBob := &types.DataQueryResponse{
			Header: &types.ResponseHeader{
				NodeId: s.ID(),
			},
			KVs: []*types.KVWithMetadata{
				{
					Key:      "key1",
					Value:    []byte(v1JSON),
					Metadata: defaultMetadata,
				},
				{
					Key:      "key3",
					Value:    []byte(v3JSON),
					Metadata: defaultMetadata,
				},
			},
		}

		resp, err := s.ExecuteJSONQuery(t, "alice", "db1", query)
		require.NoError(t, err)
		sort.Slice(resp.GetResponse().KVs, func(i, j int) bool {
			return resp.GetResponse().KVs[i].GetKey() < resp.GetResponse().KVs[j].GetKey()
		})
		require.True(t, proto.Equal(expectedKVsForAlice, resp.GetResponse()))

		resp, err = s.ExecuteJSONQuery(t, "bob", "db1", query)
		require.NoError(t, err)
		sort.Slice(resp.GetResponse().KVs, func(i, j int) bool {
			return resp.GetResponse().KVs[i].GetKey() < resp.GetResponse().KVs[j].GetKey()
		})
		require.True(t, proto.Equal(expectedKVsForBob, resp.GetResponse()))
	})

	t.Run("multiple conditions with and", func(t *testing.T) {
		query := `
		{
			"selector": {
				"$and": {
					"attr1": {
						"$neq": [false]
					},
					"attr2": {
						"$gte": -1,
						"$lte": 100
					},
					"attr3": {
						"$gt": "name1",
						"$lte": "name6"
					}
				}
			}
		}
		`
		expectedKVs := &types.DataQueryResponse{
			Header: &types.ResponseHeader{
				NodeId: s.ID(),
			},
			KVs: []*types.KVWithMetadata{
				{
					Key:      "key3",
					Value:    []byte(v3JSON),
					Metadata: defaultMetadata,
				},
				{
					Key:      "key6",
					Value:    []byte(v6JSON),
					Metadata: defaultMetadata,
				},
			},
		}

		resp, err := s.ExecuteJSONQuery(t, "alice", "db1", query)
		require.NoError(t, err)
		sort.Slice(resp.GetResponse().KVs, func(i, j int) bool {
			return resp.GetResponse().KVs[i].GetKey() < resp.GetResponse().KVs[j].GetKey()
		})
		require.True(t, proto.Equal(expectedKVs, resp.GetResponse()))
	})

	t.Run("multiple conditions with or", func(t *testing.T) {
		query := `
		{
			"selector": {
				"$or": {
					"attr1": {
						"$eq": false
					},
					"attr2": {
						"$gt": -1,
						"$lt": 100
					},
					"attr3": {
						"$gt": "name3",
						"$lte": "name5"
					}
				}
			}
		}
		`
		expectedKVs := &types.DataQueryResponse{
			Header: &types.ResponseHeader{
				NodeId: s.ID(),
			},
			KVs: []*types.KVWithMetadata{
				{
					Key:   "key2",
					Value: []byte(v2JSON),
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
					Key:      "key4",
					Value:    []byte(v4JSON),
					Metadata: defaultMetadata,
				},
			},
		}

		resp, err := s.ExecuteJSONQuery(t, "alice", "db1", query)
		require.NoError(t, err)
		sort.Slice(resp.GetResponse().KVs, func(i, j int) bool {
			return resp.GetResponse().KVs[i].GetKey() < resp.GetResponse().KVs[j].GetKey()
		})
		require.True(t, proto.Equal(expectedKVs, resp.GetResponse()))
	})

	t.Run("multiple conditions with or and neq", func(t *testing.T) {
		query := `
		{
			"selector": {
				"$or": {
					"attr2": {
						"$gt": -1,
						"$lt": 100,
						"$neq": [0]
					},
					"attr3": {
						"$gt": "name3",
						"$lte": "name5",
						"$neq": ["name4"]
					}
				}
			}
		}
		`
		expectedKVs := &types.DataQueryResponse{
			Header: &types.ResponseHeader{
				NodeId: s.ID(),
			},
			KVs: []*types.KVWithMetadata{
				{
					Key:   "key5",
					Value: []byte(v5JSON),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 4,
							TxNum:    0,
						},
						AccessControl: &types.AccessControl{
							ReadWriteUsers: map[string]bool{"bob": true},
						},
					},
				},
			},
		}

		resp, err := s.ExecuteJSONQuery(t, "bob", "db1", query)
		require.NoError(t, err)
		require.True(t, proto.Equal(expectedKVs, resp.GetResponse()))
	})

	t.Run("bad selector", func(t *testing.T) {
		query := `
		{
			"selector": {
				"attr2": {
					"$gt": -1,
					"$lt": 100,
					"$neq": [0]
				},
				"attr3": {
				}
			}
		}
		`
		resp, err := s.ExecuteJSONQuery(t, "bob", "db1", query)
		require.EqualError(t, err, "error while processing 'POST /data/db1/jsonquery' because no condition provided for the attribute [attr3]. All given attributes must have a condition")
		require.Nil(t, resp.GetResponse().GetKVs())
	})

	t.Run("query db with no index", func(t *testing.T) {
		query := `
		{
			"selector": {
				"attr2": {
					"$gt": -1,
					"$lt": 100,
					"$neq": [0]
				},
			}
		}
		`
		resp, err := s.ExecuteJSONQuery(t, "alice", "db3", query)
		require.EqualError(t, err, "error while processing 'POST /data/db3/jsonquery' because database _index_db3 does not exist")
		require.Nil(t, resp.GetResponse().GetKVs())
	})

	t.Run("empty result set", func(t *testing.T) {
		query := `
		{
			"selector": {
				"$and": {
					"attr2": {
						"$gt": -1,
						"$lt": 100,
						"$neq": [0]
					},
					"attr3": {
						"$gt": "name3",
						"$lte": "name5",
						"$neq": ["name4"]
					}
				}
			}
		}
		`
		resp, err := s.ExecuteJSONQuery(t, "bob", "db1", query)
		require.NoError(t, err)
		require.Nil(t, resp.GetResponse().KVs)
	})

	t.Run("query db with no permission", func(t *testing.T) {
		query := `
		{
			"selector": {
				"attr2": {
					"$gt": -1,
					"$lt": 100,
					"$neq": [0]
				},
			}
		}
		`
		resp, err := s.ExecuteJSONQuery(t, "bob", "db3", query)
		require.EqualError(t, err, "error while processing 'POST /data/db3/jsonquery' because the user [bob] has no permission to read from database [db3]")
		require.Nil(t, resp.GetResponse().GetKVs())
	})
}

var v1JSON = `{"attr1":true, "attr2": -10, "attr3": "name1"}`
var v2JSON = `{"attr1":false, "attr2": -110, "attr3": "name2"}` // only alice has access to this state
var v3JSON = `{"attr1":true, "attr2": -1, "attr3": "name3"}`
var v4JSON = `{"attr1":false, "attr2": 0, "attr3": "name4"}`
var v5JSON = `{"attr1":true, "attr2": 110, "attr3": "name5"}` // only bob has access to this state
var v6JSON = `{"attr1":true, "attr2": 100, "attr3": "name6"}`

func prepareDataForJSONQueries(t *testing.T, s *setup.Server) {
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
						Value: []byte(v1JSON),
					},
					{
						Key:   "key2",
						Value: []byte(v2JSON),
						Acl: &types.AccessControl{
							ReadWriteUsers: map[string]bool{"alice": true},
						},
					},
					{
						Key:   "key3",
						Value: []byte(v3JSON),
					},
					{
						Key:   "key4",
						Value: []byte(v4JSON),
					},
					{
						Key:   "key5",
						Value: []byte(v5JSON),
						Acl: &types.AccessControl{
							ReadWriteUsers: map[string]bool{"bob": true},
						},
					},
					{
						Key:   "key6",
						Value: []byte(v6JSON),
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
}
