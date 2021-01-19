package blockprocessor

import (
	"strings"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/server/internal/certificateauthority"
	"github.ibm.com/blockchaindb/server/internal/identity"
	"github.ibm.com/blockchaindb/server/internal/worldstate"
	"github.ibm.com/blockchaindb/server/pkg/server/testutils"
	"github.ibm.com/blockchaindb/server/pkg/types"
)

func TestValidateConfigTx(t *testing.T) {
	t.Parallel()

	userID := "adminUser"
	cryptoDir := testutils.GenerateTestClientCrypto(t, []string{"adminUser", "nonAdminUser", "node"})
	adminCert, adminSigner := testutils.LoadTestClientCrypto(t, cryptoDir, "adminUser")
	nonAdminCert, nonAdminSigner := testutils.LoadTestClientCrypto(t, cryptoDir, "nonAdminUser")
	nodeCert, _ := testutils.LoadTestClientCrypto(t, cryptoDir, "node")
	caCert, caKey := testutils.LoadTestClientCA(t, cryptoDir, testutils.RootCAFileName)
	require.NotNil(t, caKey)

	setup := func(db worldstate.DB) {
		nonAdminUser := &types.User{
			ID:          "nonAdminUser",
			Certificate: nonAdminCert.Raw,
		}
		nonAdminUserSerialized, err := proto.Marshal(nonAdminUser)
		require.NoError(t, err)

		adminUser := &types.User{
			ID:          userID,
			Certificate: adminCert.Raw,
			Privilege: &types.Privilege{
				ClusterAdministration: true,
			},
		}
		adminUserSerialized, err := proto.Marshal(adminUser)
		require.NoError(t, err)

		newUsers := []*worldstate.DBUpdates{
			{
				DBName: worldstate.UsersDBName,
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
		}

		require.NoError(t, db.Commit(newUsers, 1))
	}

	tests := []struct {
		name           string
		txEnv          *types.ConfigTxEnvelope
		expectedResult *types.ValidationInfo
	}{
		{
			name: "invalid: submitter does not have cluster admin privilege",
			txEnv: testutils.SignedConfigTxEnvelope(t, nonAdminSigner, &types.ConfigTx{
				UserID: "nonAdminUser",
			}),
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: "the user [nonAdminUser] has no privilege to perform cluster administrative operations",
			},
		},
		{
			name: "invalid: signature verification failure",
			txEnv: testutils.SignedConfigTxEnvelope(t, adminSigner, &types.ConfigTx{
				UserID: "nonAdminUser",
			}),
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_UNAUTHORISED,
				ReasonIfInvalid: "signature verification failed: x509: ECDSA verification failure",
			},
		},
		{
			name: "invalid: new config is empty",
			txEnv: testutils.SignedConfigTxEnvelope(t, adminSigner, &types.ConfigTx{
				UserID:    "adminUser",
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
				UserID:    "adminUser",
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
				UserID:    "adminUser",
				NewConfig: &types.ClusterConfig{RootCACertificate: caCert.Raw},
			}),
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "node config is empty. There must be at least single node in the cluster",
			},
		},
		{
			name: "invalid: admin config is empty",
			txEnv: testutils.SignedConfigTxEnvelope(t, adminSigner, &types.ConfigTx{
				UserID: "adminUser",
				NewConfig: &types.ClusterConfig{
					Nodes: []*types.NodeConfig{
						{
							ID:          "node1",
							Address:     "127.0.0.1",
							Certificate: nodeCert.Raw,
						},
					},
					RootCACertificate: caCert.Raw,
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
				UserID: "adminUser",
				ReadOldConfigVersion: &types.Version{
					BlockNum: 100,
					TxNum:    100,
				},
				NewConfig: &types.ClusterConfig{
					Nodes: []*types.NodeConfig{
						{
							ID:          "node1",
							Address:     "127.0.0.1",
							Certificate: nodeCert.Raw,
						},
					},
					Admins: []*types.Admin{
						{
							ID:          "admin1",
							Certificate: adminCert.Raw,
						},
					},
					RootCACertificate: caCert.Raw,
				},
			}),
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_MVCC_CONFLICT_WITH_COMMITTED_STATE,
				ReasonIfInvalid: "mvcc conflict has occurred as the read old configuration does not match the committed version",
			},
		},
		{
			name: "valid",
			txEnv: testutils.SignedConfigTxEnvelope(t, adminSigner, &types.ConfigTx{
				UserID:               "adminUser",
				ReadOldConfigVersion: nil,
				NewConfig: &types.ClusterConfig{
					Nodes: []*types.NodeConfig{
						{
							ID:          "node1",
							Address:     "127.0.0.1",
							Certificate: nodeCert.Raw,
						},
					},
					Admins: []*types.Admin{
						{
							ID:          "admin1",
							Certificate: adminCert.Raw,
						},
					},
					RootCACertificate: caCert.Raw,
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

			result, err := env.validator.configTxValidator.validate(tt.txEnv)
			require.NoError(t, err)
			require.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestValidateCAConfig(t *testing.T) {
	t.Parallel()

	cryptoDir := testutils.GenerateTestClientCrypto(t, []string{"node"})
	nodeCert, _ := testutils.LoadTestClientCrypto(t, cryptoDir, "node")
	caCert, _ := testutils.LoadTestClientCA(t, cryptoDir, testutils.RootCAFileName)

	//TODO add additional test cases once we implement: https://github.ibm.com/blockchaindb/server/issues/358
	tests := []struct {
		name           string
		caCert         []byte
		expectedResult *types.ValidationInfo
	}{
		{
			name:   "invalid: empty CA cert",
			caCert: nil,
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "CA config is empty. At least one root CA is required",
			},
		},
		{
			name:   "invalid: bad certificate",
			caCert: []byte("bad-certificate"),
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "CA certificate collection cannot be created: asn1: structure error: tags don't match (16 vs {class:1 tag:2 length:97 isCompound:true}) {optional:false explicit:false application:false private:false defaultValue:<nil> tag:<nil> stringType:0 timeType:0 set:false omitEmpty:false} certificate @2",
			},
		},
		{
			name:   "invalid: not a CA certificate",
			caCert: nodeCert.Raw,
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "CA certificate collection cannot be created: certificate is missing the CA property, SN:",
			},
		},
		{
			name:   "valid CA",
			caCert: caCert.Raw,
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			config := &types.ClusterConfig{RootCACertificate: tt.caCert}
			result, caCertCollection := validateCAConfig(config)

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
			require.Condition(t, matchValidationInfo)

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

	cryptoDir := testutils.GenerateTestClientCrypto(t, []string{"node"})
	nodeCert, _ := testutils.LoadTestClientCrypto(t, cryptoDir, "node")
	caCert, _ := testutils.LoadTestClientCA(t, cryptoDir, testutils.RootCAFileName)
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
					ID: "",
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
					ID:      "node1",
					Address: "",
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the node [node1] has an empty ip address",
			},
		},
		{
			name: "invalid: node IP address is not valid",
			nodes: []*types.NodeConfig{
				{
					ID:      "node1",
					Address: "127.0.0",
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the node [node1] has an invalid ip address [127.0.0]",
			},
		},
		{
			name: "invalid: node certificate is not valid",
			nodes: []*types.NodeConfig{
				{
					ID:          "node1",
					Address:     "127.0.0.1",
					Certificate: []byte("random"),
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the node [node1] has an invalid certificate: error parsing certificate: asn1: structure error: tags don't match (16 vs {class:1 tag:18 length:97 isCompound:true}) {optional:false explicit:false application:false private:false defaultValue:<nil> tag:<nil> stringType:0 timeType:0 set:false omitEmpty:false} certificate @2",
			},
		},
		{
			name: "invalid: duplicate nodes in the node config",
			nodes: []*types.NodeConfig{
				{
					ID:          "node1",
					Address:     "127.0.0.1",
					Certificate: nodeCert.Raw,
				},
				{
					ID:          "node1",
					Address:     "127.0.0.1",
					Certificate: nodeCert.Raw,
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there are two nodes with the same ID [node1] in the node config. The node IDs must be unique",
			},
		},
		{
			name: "valid",
			nodes: []*types.NodeConfig{
				{
					ID:          "node1",
					Address:     "127.0.0.1",
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

	cryptoDir := testutils.GenerateTestClientCrypto(t, []string{"admin"})
	adminCert, _ := testutils.LoadTestClientCrypto(t, cryptoDir, "admin")
	caCert, _ := testutils.LoadTestClientCA(t, cryptoDir, testutils.RootCAFileName)
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
					ID: "",
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
					ID:          "admin1",
					Certificate: adminCert.Raw,
				},
				{
					ID:          "admin2",
					Certificate: []byte("random"),
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the admin [admin2] has an invalid certificate: error parsing certificate: asn1: structure error: tags don't match (16 vs {class:1 tag:18 length:97 isCompound:true}) {optional:false explicit:false application:false private:false defaultValue:<nil> tag:<nil> stringType:0 timeType:0 set:false omitEmpty:false} certificate @2",
			},
		},
		{
			name: "invalid: dulplicate adminID in the admin config",
			admins: []*types.Admin{
				{
					ID:          "admin1",
					Certificate: adminCert.Raw,
				},
				{
					ID:          "admin1",
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
					ID:          "admin1",
					Certificate: adminCert.Raw,
				},
				{
					ID:          "admin2",
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

func TestMVCCOnConfigTx(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		setup          func(db worldstate.DB)
		readVersion    *types.Version
		expectedResult *types.ValidationInfo
	}{
		{
			name:  "invalid: readVersion does not match committed version as there is no existing config",
			setup: func(db worldstate.DB) {},
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
			name: "invalid: readVersion does not match committed version",
			setup: func(db worldstate.DB) {
				config := []*worldstate.DBUpdates{
					{
						DBName: worldstate.ConfigDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key: worldstate.ConfigKey,
								Metadata: &types.Metadata{
									Version: &types.Version{
										BlockNum: 5,
										TxNum:    1,
									},
								},
							},
						},
					},
				}

				require.NoError(t, db.Commit(config, 5))
			},
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
			name: "valid as the read and committed version are the same",
			setup: func(db worldstate.DB) {
				config := []*worldstate.DBUpdates{
					{
						DBName: worldstate.ConfigDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key: worldstate.ConfigKey,
								Metadata: &types.Metadata{
									Version: &types.Version{
										BlockNum: 5,
										TxNum:    1,
									},
								},
							},
						},
					},
				}

				require.NoError(t, db.Commit(config, 5))
			},
			readVersion: &types.Version{
				BlockNum: 5,
				TxNum:    1,
			},
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
		{
			name:        "valid as there is no committed config and read version is also nil",
			setup:       func(db worldstate.DB) {},
			readVersion: nil,
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

			tt.setup(env.db)

			result, err := env.validator.configTxValidator.mvccValidation(tt.readVersion)
			require.NoError(t, err)
			require.Equal(t, tt.expectedResult, result)
		})
	}
}
