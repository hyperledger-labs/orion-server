// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package txvalidation

import (
	"strings"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger-labs/orion-server/internal/identity"
	"github.com/hyperledger-labs/orion-server/internal/worldstate"
	"github.com/hyperledger-labs/orion-server/pkg/certificateauthority"
	"github.com/hyperledger-labs/orion-server/pkg/server/testutils"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestValidateConfigTx(t *testing.T) {
	t.Parallel()

	userID := "adminUser"
	cryptoDir := testutils.GenerateTestCrypto(t, []string{"adminUser", "nonAdminUser", "node"})
	adminCert, adminSigner := testutils.LoadTestCrypto(t, cryptoDir, "adminUser")
	nonAdminCert, nonAdminSigner := testutils.LoadTestCrypto(t, cryptoDir, "nonAdminUser")
	nodeCert, _ := testutils.LoadTestCrypto(t, cryptoDir, "node")
	caCert, caKey := testutils.LoadTestCA(t, cryptoDir, testutils.RootCAFileName)
	require.NotNil(t, caKey)

	setup := func(db worldstate.DB) {
		nonAdminUser := &types.User{
			Id:          "nonAdminUser",
			Certificate: nonAdminCert.Raw,
		}
		nonAdminUserSerialized, err := proto.Marshal(nonAdminUser)
		require.NoError(t, err)

		adminUser := &types.User{
			Id:          userID,
			Certificate: adminCert.Raw,
			Privilege: &types.Privilege{
				Admin: true,
			},
		}
		adminUserSerialized, err := proto.Marshal(adminUser)
		require.NoError(t, err)

		config := &types.ClusterConfig{
			Nodes: []*types.NodeConfig{
				{
					Id:          "node1",
					Address:     "127.0.0.1",
					Port:        6090,
					Certificate: nodeCert.Raw,
				},
			},
			Admins: []*types.Admin{
				{
					Id:          "admin1",
					Certificate: adminCert.Raw,
				},
			},
			CertAuthConfig: &types.CAConfig{
				Roots: [][]byte{caCert.Raw},
			},
			ConsensusConfig: &types.ConsensusConfig{
				Algorithm: "raft",
				Members: []*types.PeerConfig{
					{
						NodeId:   "node1",
						RaftId:   1,
						PeerHost: "127.0.0.1",
						PeerPort: 7090,
					},
				},
				Observers: nil,
				RaftConfig: &types.RaftConfig{
					TickInterval:   "100ms",
					ElectionTicks:  100,
					HeartbeatTicks: 10,
				},
			},
		}
		configSerialized, err := proto.Marshal(config)
		require.NoError(t, err)

		dbUpdates := map[string]*worldstate.DBUpdates{
			worldstate.UsersDBName: {
				Writes: []*worldstate.KVWithMetadata{
					{
						Key:   string(identity.UserNamespace) + "nonAdminUser",
						Value: nonAdminUserSerialized,
					},
					{
						Key:   string(identity.UserNamespace) + "adminUser",
						Value: adminUserSerialized,
					},
				},
			},
			worldstate.ConfigDBName: {
				Writes: []*worldstate.KVWithMetadata{
					{
						Key:   worldstate.ConfigKey,
						Value: configSerialized,
						Metadata: &types.Metadata{
							Version: &types.Version{BlockNum: 1, TxNum: 1},
						},
					},
				},
			},
		}

		require.NoError(t, db.Commit(dbUpdates, 1))
	}

	tests := []struct {
		name           string
		txEnv          *types.ConfigTxEnvelope
		expectedResult *types.ValidationInfo
	}{
		{
			name: "invalid: submitter does not have cluster admin privilege",
			txEnv: testutils.SignedConfigTxEnvelope(t, nonAdminSigner, &types.ConfigTx{
				UserId: "nonAdminUser",
			}),
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: "the user [nonAdminUser] has no privilege to perform cluster administrative operations",
			},
		},
		{
			name: "invalid: signature verification failure",
			txEnv: testutils.SignedConfigTxEnvelope(t, adminSigner, &types.ConfigTx{
				UserId: "nonAdminUser",
			}),
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_UNAUTHORISED,
				ReasonIfInvalid: "signature verification failed: x509: ECDSA verification failure",
			},
		},
		{
			name: "invalid: new config is empty",
			txEnv: testutils.SignedConfigTxEnvelope(t, adminSigner, &types.ConfigTx{
				UserId:    "adminUser",
				NewConfig: nil,
			}),
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "new config is empty. There must be at least single node and an admin in the cluster",
			},
		},
		{
			name: "invalid: CA config is empty",
			txEnv: testutils.SignedConfigTxEnvelope(t, adminSigner, &types.ConfigTx{
				UserId:    "adminUser",
				NewConfig: &types.ClusterConfig{},
			}),
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "CA config is empty. At least one root CA is required",
			},
		},
		{
			name: "invalid: node config is empty",
			txEnv: testutils.SignedConfigTxEnvelope(t, adminSigner, &types.ConfigTx{
				UserId: "adminUser",
				NewConfig: &types.ClusterConfig{
					CertAuthConfig: &types.CAConfig{
						Roots: [][]byte{caCert.Raw},
					},
				},
			}),
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "node config is empty. There must be at least single node in the cluster",
			},
		},
		{
			name: "invalid: admin config is empty",
			txEnv: testutils.SignedConfigTxEnvelope(t, adminSigner, &types.ConfigTx{
				UserId: "adminUser",
				NewConfig: &types.ClusterConfig{
					Nodes: []*types.NodeConfig{
						{
							Id:          "node1",
							Address:     "127.0.0.1",
							Port:        6090,
							Certificate: nodeCert.Raw,
						},
					},
					CertAuthConfig: &types.CAConfig{
						Roots: [][]byte{caCert.Raw},
					},
				},
			}),
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "admin config is empty. There must be at least single admin in the cluster",
			},
		},
		{
			name: "invalid: mvcc conflict",
			txEnv: testutils.SignedConfigTxEnvelope(t, adminSigner, &types.ConfigTx{
				UserId: "adminUser",
				ReadOldConfigVersion: &types.Version{
					BlockNum: 100,
					TxNum:    100,
				},
				NewConfig: &types.ClusterConfig{
					Nodes: []*types.NodeConfig{
						{
							Id:          "node1",
							Address:     "127.0.0.1",
							Port:        6090,
							Certificate: nodeCert.Raw,
						},
					},
					Admins: []*types.Admin{
						{
							Id:          "admin1",
							Certificate: adminCert.Raw,
						},
					},
					CertAuthConfig: &types.CAConfig{
						Roots: [][]byte{caCert.Raw},
					},
					ConsensusConfig: &types.ConsensusConfig{
						Algorithm: "raft",
						Members: []*types.PeerConfig{
							{
								NodeId:   "node1",
								RaftId:   1,
								PeerHost: "127.0.0.1",
								PeerPort: 7090,
							},
						},
						Observers: nil,
						RaftConfig: &types.RaftConfig{
							TickInterval:   "100ms",
							ElectionTicks:  100,
							HeartbeatTicks: 10,
						},
					},
				},
			}),
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_MVCC_CONFLICT_WITH_COMMITTED_STATE,
				ReasonIfInvalid: "mvcc conflict has occurred as the read old configuration does not match the committed version",
			},
		},
		{
			name: "valid: nodes update",
			txEnv: testutils.SignedConfigTxEnvelope(t, adminSigner, &types.ConfigTx{
				UserId: "adminUser",
				ReadOldConfigVersion: &types.Version{
					BlockNum: 1,
					TxNum:    1,
				},
				NewConfig: &types.ClusterConfig{
					Nodes: []*types.NodeConfig{
						{
							Id:          "node1",
							Address:     "127.0.0.1",
							Port:        666, //<<< changed
							Certificate: nodeCert.Raw,
						},
					},
					Admins: []*types.Admin{
						{
							Id:          "admin1",
							Certificate: adminCert.Raw,
						},
					},
					CertAuthConfig: &types.CAConfig{
						Roots: [][]byte{caCert.Raw},
					},
					ConsensusConfig: &types.ConsensusConfig{
						Algorithm: "raft",
						Members: []*types.PeerConfig{
							{
								NodeId:   "node1",
								RaftId:   1,
								PeerHost: "127.0.0.1",
								PeerPort: 7090,
							},
						},
						Observers: nil,
						RaftConfig: &types.RaftConfig{
							TickInterval:   "100ms",
							ElectionTicks:  100,
							HeartbeatTicks: 10,
						},
					},
				},
			}),
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
		{
			name: "invalid: ledger config: trie disabled changed",
			txEnv: testutils.SignedConfigTxEnvelope(t, adminSigner, &types.ConfigTx{
				UserId: "adminUser",
				ReadOldConfigVersion: &types.Version{
					BlockNum: 1,
					TxNum:    1,
				},
				NewConfig: &types.ClusterConfig{
					Nodes: []*types.NodeConfig{
						{
							Id:          "node1",
							Address:     "127.0.0.1",
							Port:        6090,
							Certificate: nodeCert.Raw,
						},
					},
					Admins: []*types.Admin{
						{
							Id:          "admin1",
							Certificate: adminCert.Raw,
						},
					},
					CertAuthConfig: &types.CAConfig{
						Roots: [][]byte{caCert.Raw},
					},
					ConsensusConfig: &types.ConsensusConfig{
						Algorithm: "raft",
						Members: []*types.PeerConfig{
							{
								NodeId:   "node1",
								RaftId:   1,
								PeerHost: "127.0.0.1",
								PeerPort: 7090,
							},
						},
						Observers: nil,
						RaftConfig: &types.RaftConfig{
							TickInterval:   "100ms",
							ElectionTicks:  100,
							HeartbeatTicks: 10,
						},
					},
					LedgerConfig: &types.LedgerConfig{StateMerklePatriciaTrieDisabled: true},
				},
			}),
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "LedgerConfig.StateMerklePatriciaTrieDisabled cannot be changed",
			},
		},
		{
			name: "invalid: too many consensus peer membership changes",
			txEnv: testutils.SignedConfigTxEnvelope(t, adminSigner, &types.ConfigTx{
				UserId: "adminUser",
				ReadOldConfigVersion: &types.Version{
					BlockNum: 1,
					TxNum:    1,
				},
				NewConfig: &types.ClusterConfig{
					Nodes: []*types.NodeConfig{
						{
							Id:          "node1",
							Address:     "127.0.0.1",
							Port:        6090,
							Certificate: nodeCert.Raw,
						},
						{
							Id:          "node2",
							Address:     "127.0.0.1",
							Port:        6091,
							Certificate: nodeCert.Raw,
						},
						{
							Id:          "node3",
							Address:     "127.0.0.1",
							Port:        6092,
							Certificate: nodeCert.Raw,
						},
					},
					Admins: []*types.Admin{
						{
							Id:          "admin1",
							Certificate: adminCert.Raw,
						},
					},
					CertAuthConfig: &types.CAConfig{
						Roots: [][]byte{caCert.Raw},
					},
					ConsensusConfig: &types.ConsensusConfig{
						Algorithm: "raft",
						Members: []*types.PeerConfig{
							{
								NodeId:   "node1",
								RaftId:   1,
								PeerHost: "127.0.0.1",
								PeerPort: 7090,
							},
							{
								NodeId:   "node2",
								RaftId:   2,
								PeerHost: "127.0.0.1",
								PeerPort: 7091,
							},
							{
								NodeId:   "node3",
								RaftId:   3,
								PeerHost: "127.0.0.1",
								PeerPort: 7092,
							},
						},
						Observers: nil,
						RaftConfig: &types.RaftConfig{
							TickInterval:   "100ms",
							ElectionTicks:  100,
							HeartbeatTicks: 10,
						},
					},
				},
			}),
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "error in ConsensusConfig: cannot make more than one membership change at a time: 2 added, 0 removed",
			},
		},
		{
			name: "valid: consensus peer membership change",
			txEnv: testutils.SignedConfigTxEnvelope(t, adminSigner, &types.ConfigTx{
				UserId: "adminUser",
				ReadOldConfigVersion: &types.Version{
					BlockNum: 1,
					TxNum:    1,
				},
				NewConfig: &types.ClusterConfig{
					Nodes: []*types.NodeConfig{
						{
							Id:          "node1",
							Address:     "127.0.0.1",
							Port:        6090,
							Certificate: nodeCert.Raw,
						},
						{
							Id:          "node2",
							Address:     "127.0.0.1",
							Port:        6091,
							Certificate: nodeCert.Raw,
						},
					},
					Admins: []*types.Admin{
						{
							Id:          "admin1",
							Certificate: adminCert.Raw,
						},
					},
					CertAuthConfig: &types.CAConfig{
						Roots: [][]byte{caCert.Raw},
					},
					ConsensusConfig: &types.ConsensusConfig{
						Algorithm: "raft",
						Members: []*types.PeerConfig{
							{
								NodeId:   "node1",
								RaftId:   1,
								PeerHost: "127.0.0.1",
								PeerPort: 7090,
							},
							{
								NodeId:   "node2",
								RaftId:   2,
								PeerHost: "127.0.0.1",
								PeerPort: 7091,
							},
						},
						Observers: nil,
						RaftConfig: &types.RaftConfig{
							TickInterval:   "100ms",
							ElectionTicks:  100,
							HeartbeatTicks: 10,
						},
					},
				},
			}),
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
		{
			name: "valid: consensus peer endpoint update",
			txEnv: testutils.SignedConfigTxEnvelope(t, adminSigner, &types.ConfigTx{
				UserId: "adminUser",
				ReadOldConfigVersion: &types.Version{
					BlockNum: 1,
					TxNum:    1,
				},
				NewConfig: &types.ClusterConfig{
					Nodes: []*types.NodeConfig{
						{
							Id:          "node1",
							Address:     "127.0.0.1",
							Port:        6090,
							Certificate: nodeCert.Raw,
						},
					},
					Admins: []*types.Admin{
						{
							Id:          "admin1",
							Certificate: adminCert.Raw,
						},
					},
					CertAuthConfig: &types.CAConfig{
						Roots: [][]byte{caCert.Raw},
					},
					ConsensusConfig: &types.ConsensusConfig{
						Algorithm: "raft",
						Members: []*types.PeerConfig{
							{
								NodeId:   "node1",
								RaftId:   1,
								PeerHost: "127.0.0.1",
								PeerPort: 666, //<<< changed
							},
						},
						Observers: nil,
						RaftConfig: &types.RaftConfig{
							TickInterval:   "100ms",
							ElectionTicks:  100,
							HeartbeatTicks: 10,
						},
					},
				},
			}),
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
		{
			name: "valid: consensus raft config",
			txEnv: testutils.SignedConfigTxEnvelope(t, adminSigner, &types.ConfigTx{
				UserId: "adminUser",
				ReadOldConfigVersion: &types.Version{
					BlockNum: 1,
					TxNum:    1,
				},
				NewConfig: &types.ClusterConfig{
					Nodes: []*types.NodeConfig{
						{
							Id:          "node1",
							Address:     "127.0.0.1",
							Port:        6090,
							Certificate: nodeCert.Raw,
						},
					},
					Admins: []*types.Admin{
						{
							Id:          "admin1",
							Certificate: adminCert.Raw,
						},
					},
					CertAuthConfig: &types.CAConfig{
						Roots: [][]byte{caCert.Raw},
					},
					ConsensusConfig: &types.ConsensusConfig{
						Algorithm: "raft",
						Members: []*types.PeerConfig{
							{
								NodeId:   "node1",
								RaftId:   1,
								PeerHost: "127.0.0.1",
								PeerPort: 7090,
							},
						},
						Observers: nil,
						RaftConfig: &types.RaftConfig{
							TickInterval:   "100ms",
							ElectionTicks:  101, //<<< changed
							HeartbeatTicks: 11,  //<<< changed
						},
					},
				},
			}),
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
		{
			name: "valid",
			txEnv: testutils.SignedConfigTxEnvelope(t, adminSigner, &types.ConfigTx{
				UserId: "adminUser",
				ReadOldConfigVersion: &types.Version{
					BlockNum: 1,
					TxNum:    1,
				},
				NewConfig: &types.ClusterConfig{
					Nodes: []*types.NodeConfig{
						{
							Id:          "node1",
							Address:     "127.0.0.1",
							Port:        6090,
							Certificate: nodeCert.Raw,
						},
					},
					Admins: []*types.Admin{
						{
							Id:          "admin1",
							Certificate: adminCert.Raw,
						},
					},
					CertAuthConfig: &types.CAConfig{
						Roots: [][]byte{caCert.Raw},
					},
					ConsensusConfig: &types.ConsensusConfig{
						Algorithm: "raft",
						Members: []*types.PeerConfig{
							{
								NodeId:   "node1",
								RaftId:   1,
								PeerHost: "127.0.0.1",
								PeerPort: 7090,
							},
						},
						Observers: nil,
						RaftConfig: &types.RaftConfig{
							TickInterval:   "100ms",
							ElectionTicks:  100,
							HeartbeatTicks: 10,
						},
					},
				},
			}),
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newValidatorTestEnv(t)
			defer env.cleanup()

			setup(env.db)

			result, err := env.validator.configTxValidator.Validate(tt.txEnv)
			require.NoError(t, err)
			require.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestValidateCAConfig(t *testing.T) {
	t.Parallel()

	cryptoDir := testutils.GenerateTestCrypto(t, []string{"node"})
	nodeCert, _ := testutils.LoadTestCrypto(t, cryptoDir, "node")
	caCert, _ := testutils.LoadTestCA(t, cryptoDir, testutils.RootCAFileName)

	//TODO add additional test cases once we implement: https://github.ibm.com/blockchaindb/server/issues/358
	tests := []struct {
		name           string
		caConfig       *types.CAConfig
		expectedResult *types.ValidationInfo
	}{
		{
			name:     "invalid: empty CA config",
			caConfig: nil,
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "CA config is empty. At least one root CA is required",
			},
		},
		{
			name:     "invalid: empty CA config roots",
			caConfig: &types.CAConfig{},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "CA config Roots is empty. At least one root CA is required",
			},
		},
		{
			name:     "invalid: bad root certificate",
			caConfig: &types.CAConfig{Roots: [][]byte{[]byte("bad-certificate")}},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "CA certificate collection cannot be created: x509: malformed certificate",
			},
		},
		{
			name:     "invalid: not a CA certificate",
			caConfig: &types.CAConfig{Roots: [][]byte{nodeCert.Raw}},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "CA certificate collection cannot be created: certificate is missing the CA property, SN:",
			},
		},
		{
			name:     "valid root CA",
			caConfig: &types.CAConfig{Roots: [][]byte{caCert.Raw}},
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, caCertCollection := validateCAConfig(tt.caConfig)

			matchValidationInfo := func() bool {
				if result.Flag != tt.expectedResult.Flag {
					return false
				}
				if result.ReasonIfInvalid == tt.expectedResult.ReasonIfInvalid {
					return true
				}
				if strings.HasPrefix(result.ReasonIfInvalid, tt.expectedResult.ReasonIfInvalid) {
					return true
				}
				return false
			}
			require.Condition(t, matchValidationInfo, "result: %v, didn't match expected: %v", result, tt.expectedResult)

			if tt.expectedResult.Flag == types.Flag_VALID {
				require.NotNil(t, caCertCollection)
			} else {
				require.Nil(t, caCertCollection)
			}
		})
	}
}

func TestValidateNodeConfig(t *testing.T) {
	t.Parallel()

	cryptoDir := testutils.GenerateTestCrypto(t, []string{"node"})
	nodeCert, _ := testutils.LoadTestCrypto(t, cryptoDir, "node")
	caCert, _ := testutils.LoadTestCA(t, cryptoDir, testutils.RootCAFileName)
	caCertCollection, err := certificateauthority.NewCACertCollection([][]byte{caCert.Raw}, nil)
	require.NoError(t, err)

	tests := []struct {
		name           string
		nodes          []*types.NodeConfig
		expectedResult *types.ValidationInfo
	}{
		{
			name:  "invalid: empty node entries",
			nodes: nil,
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "node config is empty. There must be at least single node in the cluster",
			},
		},
		{
			name: "invalid: an admin entry is empty",
			nodes: []*types.NodeConfig{
				nil,
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there is an empty node entry in the node config",
			},
		},
		{
			name: "invalid: empty nodeID",
			nodes: []*types.NodeConfig{
				{
					Id: "",
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there is a node in the node config with an empty ID. A valid nodeID must be an non-empty string",
			},
		},
		{
			name: "invalid: node IP address is empty",
			nodes: []*types.NodeConfig{
				{
					Id:          "node1",
					Address:     "",
					Port:        6060,
					Certificate: nodeCert.Raw,
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the node [node1] has an empty host",
			},
		},
		{
			name: "invalid: node address is not valid",
			nodes: []*types.NodeConfig{
				{
					Id:          "node1",
					Address:     "127/0/0/1",
					Port:        6060,
					Certificate: nodeCert.Raw,
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the node [node1] has an invalid host [127/0/0/1]",
			},
		},
		{
			name: "invalid: node port is not valid",
			nodes: []*types.NodeConfig{
				{
					Id:          "node1",
					Address:     "127.0.0.1",
					Port:        66000,
					Certificate: nodeCert.Raw,
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the node [node1] has an invalid port number [66000]",
			},
		},
		{
			name: "invalid: node port is missing",
			nodes: []*types.NodeConfig{
				{
					Id:          "node1",
					Address:     "127.0.0.1",
					Certificate: nodeCert.Raw,
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the node [node1] has an invalid port number [0]",
			},
		},
		{
			name: "invalid: node certificate is not valid",
			nodes: []*types.NodeConfig{
				{
					Id:          "node1",
					Address:     "127.0.0.1",
					Port:        6090,
					Certificate: []byte("random"),
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the node [node1] has an invalid certificate: error parsing certificate: x509: malformed certificate",
			},
		},
		{
			name: "invalid: duplicate node IDs in the node config",
			nodes: []*types.NodeConfig{
				{
					Id:          "node1",
					Address:     "127.0.0.1",
					Port:        6090,
					Certificate: nodeCert.Raw,
				},
				{
					Id:          "node1",
					Address:     "127.0.0.1",
					Port:        6091,
					Certificate: nodeCert.Raw,
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there are two nodes with the same ID [node1] in the node config. The node IDs must be unique",
			},
		},
		{
			name: "invalid: duplicate node EPs in the node config",
			nodes: []*types.NodeConfig{
				{
					Id:          "node1",
					Address:     "127.0.0.1",
					Port:        6090,
					Certificate: nodeCert.Raw,
				},
				{
					Id:          "node2",
					Address:     "127.0.0.1",
					Port:        6090,
					Certificate: nodeCert.Raw,
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there are two nodes with the same Host:Port [127.0.0.1:6090] in the node config. Endpoints must be unique",
			},
		},
		{
			name: "valid host name",
			nodes: []*types.NodeConfig{
				{
					Id:          "node1",
					Address:     "example.com",
					Port:        6090,
					Certificate: nodeCert.Raw,
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
		{
			name: "valid IP",
			nodes: []*types.NodeConfig{
				{
					Id:          "node1",
					Address:     "127.0.0.1",
					Port:        6090,
					Certificate: nodeCert.Raw,
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := validateNodeConfig(tt.nodes, caCertCollection)
			require.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestValidateAdminConfig(t *testing.T) {
	t.Parallel()

	cryptoDir := testutils.GenerateTestCrypto(t, []string{"admin"})
	adminCert, _ := testutils.LoadTestCrypto(t, cryptoDir, "admin")
	caCert, _ := testutils.LoadTestCA(t, cryptoDir, testutils.RootCAFileName)
	caCertCollection, err := certificateauthority.NewCACertCollection([][]byte{caCert.Raw}, nil)
	require.NoError(t, err)

	tests := []struct {
		name           string
		admins         []*types.Admin
		expectedResult *types.ValidationInfo
	}{
		{
			name:   "invalid: empty admin entries",
			admins: nil,
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "admin config is empty. There must be at least single admin in the cluster",
			},
		},
		{
			name: "invalid: an admin entry is empty",
			admins: []*types.Admin{
				nil,
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there is an empty admin entry in the admin config",
			},
		},
		{
			name: "invalid: the adminID cannot be empty",
			admins: []*types.Admin{
				{
					Id: "",
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there is an admin in the admin config with an empty ID. A valid adminID must be an non-empty string",
			},
		},
		{
			name: "invalid: admin certificate is not valid",
			admins: []*types.Admin{
				{
					Id:          "admin1",
					Certificate: adminCert.Raw,
				},
				{
					Id:          "admin2",
					Certificate: []byte("random"),
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the admin [admin2] has an invalid certificate: error parsing certificate: x509: malformed certificate",
			},
		},
		{
			name: "invalid: dulplicate adminID in the admin config",
			admins: []*types.Admin{
				{
					Id:          "admin1",
					Certificate: adminCert.Raw,
				},
				{
					Id:          "admin1",
					Certificate: adminCert.Raw,
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there are two admins with the same ID [admin1] in the admin config. The admin IDs must be unique",
			},
		},
		{
			name: "valid",
			admins: []*types.Admin{
				{
					Id:          "admin1",
					Certificate: adminCert.Raw,
				},
				{
					Id:          "admin2",
					Certificate: adminCert.Raw,
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := validateAdminConfig(tt.admins, caCertCollection)
			require.Equal(t, tt.expectedResult, result)
		})
	}
}

// TODO
func TestValidateConsensusConfig(t *testing.T) {
	t.Parallel()

	peer1 := &types.PeerConfig{
		NodeId:   "node1",
		RaftId:   1,
		PeerHost: "10.10.10.10",
		PeerPort: 6090,
	}

	tests := []struct {
		name            string
		consensusConfig *types.ConsensusConfig
		expectedResult  *types.ValidationInfo
	}{
		{
			name:            "invalid: empty",
			consensusConfig: nil,
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "Consensus config is empty.",
			},
		},
		{
			name: "invalid: unsupported algorithm",
			consensusConfig: &types.ConsensusConfig{
				Algorithm: "solo",
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "Consensus config Algorithm 'solo' is not supported.",
			},
		},
		{
			name: "invalid: no members",
			consensusConfig: &types.ConsensusConfig{
				Algorithm: "raft",
				Members:   nil,
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "Consensus config has no member peers. At least one member peer is required.",
			},
		},

		{
			name: "invalid: empty member",
			consensusConfig: &types.ConsensusConfig{
				Algorithm: "raft",
				Members:   []*types.PeerConfig{nil, nil},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "Consensus config has an empty member entry.",
			},
		},
		{
			name: "invalid: member empty nodeID",
			consensusConfig: &types.ConsensusConfig{
				Algorithm: "raft",
				Members: []*types.PeerConfig{
					{
						NodeId: "",
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "Consensus config has a member with an empty ID. A valid nodeID must be an non-empty string.",
			},
		},
		{
			name: "invalid: member host is empty",
			consensusConfig: &types.ConsensusConfig{
				Algorithm: "raft",
				Members: []*types.PeerConfig{
					{
						NodeId:   "node1",
						RaftId:   1,
						PeerHost: "",
						PeerPort: 7090,
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "Consensus config has a member [node1] with an empty host",
			},
		},
		{
			name: "invalid: member host is not valid",
			consensusConfig: &types.ConsensusConfig{
				Algorithm: "raft",
				Members: []*types.PeerConfig{
					{
						NodeId:   "node1",
						RaftId:   1,
						PeerHost: "server.com/data",
						PeerPort: 6090,
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "Consensus config has a member [node1] with an invalid host [server.com/data]",
			},
		},
		{
			name: "invalid: duplicate member IDs",
			consensusConfig: &types.ConsensusConfig{
				Algorithm: "raft",
				Members: []*types.PeerConfig{
					{
						NodeId:   "node1",
						RaftId:   1,
						PeerHost: "10.10.10.10",
						PeerPort: 6090,
					},
					{
						NodeId:   "node1",
						RaftId:   2,
						PeerHost: "server.com",
						PeerPort: 6091,
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "Consensus config has two members with the same ID [node1], the node IDs must be unique.",
			},
		},
		{
			name: "invalid: duplicate member EPs",
			consensusConfig: &types.ConsensusConfig{
				Algorithm: "raft",
				Members: []*types.PeerConfig{
					{
						NodeId:   "node1",
						RaftId:   1,
						PeerHost: "10.10.10.10",
						PeerPort: 6090,
					},
					{
						NodeId:   "node2",
						RaftId:   2,
						PeerHost: "10.10.10.10",
						PeerPort: 6090,
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "Consensus config has two members with the same Host:Port [10.10.10.10:6090], endpoints must be unique.",
			},
		},
		{
			name: "invalid: duplicate raft IDs",
			consensusConfig: &types.ConsensusConfig{
				Algorithm: "raft",
				Members: []*types.PeerConfig{
					{
						NodeId:   "node1",
						RaftId:   1,
						PeerHost: "10.10.10.10",
						PeerPort: 6090,
					},
					{
						NodeId:   "node2",
						RaftId:   1,
						PeerHost: "server.com",
						PeerPort: 6091,
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "Consensus config has two members with the same Raft ID [1], Raft IDs must be unique.",
			},
		},
		{
			name: "invalid: member with raft ID 0",
			consensusConfig: &types.ConsensusConfig{
				Algorithm: "raft",
				Members: []*types.PeerConfig{
					{
						NodeId:   "node1",
						RaftId:   0,
						PeerHost: "10.10.10.10",
						PeerPort: 6090,
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "Consensus config has a member [node1] with Raft ID 0, must be >0.",
			},
		},
		//=== observers
		{
			name: "invalid: empty observer",
			consensusConfig: &types.ConsensusConfig{
				Algorithm: "raft",
				Members:   []*types.PeerConfig{peer1},
				Observers: []*types.PeerConfig{nil, nil},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "Consensus config has an empty observer entry.",
			},
		},
		{
			name: "invalid: observer empty nodeID",
			consensusConfig: &types.ConsensusConfig{
				Algorithm: "raft",
				Members:   []*types.PeerConfig{peer1},
				Observers: []*types.PeerConfig{
					{
						NodeId: "",
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "Consensus config has an observer with an empty ID. A valid nodeID must be an non-empty string.",
			},
		},
		{
			name: "invalid: observer host is empty",
			consensusConfig: &types.ConsensusConfig{
				Algorithm: "raft",
				Members:   []*types.PeerConfig{peer1},
				Observers: []*types.PeerConfig{
					{
						NodeId:   "node2",
						RaftId:   0,
						PeerHost: "",
						PeerPort: 7091,
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "Consensus config has an observer [node2] with an empty host",
			},
		},
		{
			name: "invalid: observer host is not valid",
			consensusConfig: &types.ConsensusConfig{
				Algorithm: "raft",
				Members:   []*types.PeerConfig{peer1},
				Observers: []*types.PeerConfig{
					{
						NodeId:   "node2",
						RaftId:   0,
						PeerHost: "server.com/data",
						PeerPort: 6091,
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "Consensus config has an observer [node2] with an invalid host [server.com/data]",
			},
		},
		{
			name: "invalid: duplicate observer IDs",
			consensusConfig: &types.ConsensusConfig{
				Algorithm: "raft",
				Members:   []*types.PeerConfig{peer1},
				Observers: []*types.PeerConfig{
					{
						NodeId:   "node2",
						RaftId:   0,
						PeerHost: "10.10.10.10",
						PeerPort: 6091,
					},
					{
						NodeId:   "node2",
						RaftId:   0,
						PeerHost: "server.com",
						PeerPort: 6092,
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "Consensus config has two peers with the same ID [node2], the node IDs must be unique.",
			},
		},
		{
			name: "invalid: duplicate observer EPs",
			consensusConfig: &types.ConsensusConfig{
				Algorithm: "raft",
				Members:   []*types.PeerConfig{peer1},
				Observers: []*types.PeerConfig{
					{
						NodeId:   "node2",
						RaftId:   0,
						PeerHost: "10.10.10.10",
						PeerPort: 6091,
					},
					{
						NodeId:   "node3",
						RaftId:   0,
						PeerHost: "10.10.10.10",
						PeerPort: 6091,
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "Consensus config has two peers with the same Host:Port [10.10.10.10:6091], endpoints must be unique.",
			},
		},
		{
			name: "invalid: member with raft ID >0",
			consensusConfig: &types.ConsensusConfig{
				Algorithm: "raft",
				Members:   []*types.PeerConfig{peer1},
				Observers: []*types.PeerConfig{
					{
						NodeId:   "node2",
						RaftId:   2,
						PeerHost: "server.com",
						PeerPort: 6091,
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "Consensus config has an observer [node2] with Raft ID >0.",
			},
		},
		//=== Raft
		{
			name: "invalid: raft config empty",
			consensusConfig: &types.ConsensusConfig{
				Algorithm: "raft",
				Members:   []*types.PeerConfig{peer1},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "Consensus config RaftConfig is empty.",
			},
		},
		{
			name: "invalid: raft config no interval",
			consensusConfig: &types.ConsensusConfig{
				Algorithm: "raft",
				Members:   []*types.PeerConfig{peer1},
				RaftConfig: &types.RaftConfig{
					ElectionTicks:  100,
					HeartbeatTicks: 10,
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "Consensus config RaftConfig.TickInterval is empty.",
			},
		},
		{
			name: "invalid: raft config election ticks",
			consensusConfig: &types.ConsensusConfig{
				Algorithm: "raft",
				Members:   []*types.PeerConfig{peer1},
				RaftConfig: &types.RaftConfig{
					TickInterval:   "10s",
					HeartbeatTicks: 10,
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "Consensus config RaftConfig.ElectionTicks is 0.",
			},
		},
		{
			name: "invalid: raft config heartbeat ticks",
			consensusConfig: &types.ConsensusConfig{
				Algorithm: "raft",
				Members:   []*types.PeerConfig{peer1},
				RaftConfig: &types.RaftConfig{
					TickInterval:   "10s",
					ElectionTicks:  10,
					HeartbeatTicks: 0,
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "Consensus config RaftConfig.HeartbeatTicks is 0.",
			},
		},

		//=== valid
		{
			name: "valid",
			consensusConfig: &types.ConsensusConfig{
				Algorithm: "raft",
				Members: []*types.PeerConfig{
					{
						NodeId:   "node1",
						RaftId:   1,
						PeerHost: "10.10.10.10",
						PeerPort: 6090,
					},
					{
						NodeId:   "node2",
						RaftId:   2,
						PeerHost: "server.com",
						PeerPort: 6091,
					},
				},
				Observers: []*types.PeerConfig{
					{
						NodeId:   "node3",
						RaftId:   0,
						PeerHost: "10.10.10.10",
						PeerPort: 6092,
					},
					{
						NodeId:   "node4",
						RaftId:   0,
						PeerHost: "server.com",
						PeerPort: 6093,
					},
				},
				RaftConfig: &types.RaftConfig{
					TickInterval:   "100ms",
					ElectionTicks:  100,
					HeartbeatTicks: 10,
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := validateConsensusConfig(tt.consensusConfig)
			require.Equal(t, tt.expectedResult, result)
		})
	}
}

// TODO
func TestValidateMembersNodesMatch(t *testing.T) {
	t.Parallel()

	peer1 := &types.PeerConfig{
		NodeId:   "node1",
		RaftId:   1,
		PeerHost: "10.10.10.10",
		PeerPort: 6090,
	}
	node1 := &types.NodeConfig{
		Id:          "node1",
		Address:     "11.11.11.11",
		Port:        7090,
		Certificate: []byte{1, 2, 3, 4},
	}

	peer2 := &types.PeerConfig{
		NodeId:   "node2",
		RaftId:   2,
		PeerHost: "10.10.10.10",
		PeerPort: 6091,
	}
	node2 := &types.NodeConfig{
		Id:          "node2",
		Address:     "11.11.11.11",
		Port:        7091,
		Certificate: []byte{1, 2, 3, 4},
	}

	tests := []struct {
		name           string
		nodes          []*types.NodeConfig
		members        []*types.PeerConfig
		expectedResult *types.ValidationInfo
	}{
		{
			name:    "exact match",
			nodes:   []*types.NodeConfig{node1, node2},
			members: []*types.PeerConfig{peer1, peer2},
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
		{
			name:    "invalid: more nodes",
			nodes:   []*types.NodeConfig{node1, node2},
			members: []*types.PeerConfig{peer2},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "ClusterConfig.Nodes must be the same length as ClusterConfig.ConsensusConfig.Members, and Nodes set must include all Members",
			},
		},
		{
			name:    "invalid: more peers",
			nodes:   []*types.NodeConfig{node1},
			members: []*types.PeerConfig{peer1, peer2},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "ClusterConfig.Nodes must be the same length as ClusterConfig.ConsensusConfig.Members, and Nodes set must include all Members",
			},
		},
		{
			name:  "invalid: node-peer endpoint clash",
			nodes: []*types.NodeConfig{node1},
			members: []*types.PeerConfig{
				{
					NodeId:   "node1",
					RaftId:   1,
					PeerHost: node1.Address,
					PeerPort: node1.Port,
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "ClusterConfig node [node1] and respective peer have the same endpoint [11.11.11.11:7090], node and peer endpoints must be unique.",
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := validateMembersNodesMatch(tt.members, tt.nodes)
			require.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestMVCCOnConfigTx(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		readVersion     *types.Version
		currentMetadata *types.Metadata
		expectedResult  *types.ValidationInfo
	}{
		{
			name: "invalid: readVersion does not match committed version as there is no existing config",
			readVersion: &types.Version{
				BlockNum: 1,
				TxNum:    1,
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_MVCC_CONFLICT_WITH_COMMITTED_STATE,
				ReasonIfInvalid: "mvcc conflict has occurred as the read old configuration does not match the committed version",
			},
		},
		{
			name:        "invalid: readVersion does not match committed version",
			readVersion: &types.Version{BlockNum: 1, TxNum: 1},
			currentMetadata: &types.Metadata{
				Version: &types.Version{BlockNum: 2, TxNum: 1},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_MVCC_CONFLICT_WITH_COMMITTED_STATE,
				ReasonIfInvalid: "mvcc conflict has occurred as the read old configuration does not match the committed version",
			},
		},
		{
			name: "valid as the read and committed version are the same",
			readVersion: &types.Version{
				BlockNum: 5,
				TxNum:    1,
			},
			currentMetadata: &types.Metadata{
				Version: &types.Version{BlockNum: 5, TxNum: 1},
			},
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
		{
			name:            "valid as there is no committed config and read version is also nil",
			readVersion:     nil,
			currentMetadata: nil,
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newValidatorTestEnv(t)
			defer env.cleanup()

			result, err := env.validator.configTxValidator.mvccValidation(tt.readVersion, tt.currentMetadata)
			require.NoError(t, err)
			require.Equal(t, tt.expectedResult, result)
		})
	}
}
