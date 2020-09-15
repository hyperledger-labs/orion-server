package blockprocessor

import (
	"encoding/pem"
	"io/ioutil"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/pkg/identity"
	"github.ibm.com/blockchaindb/server/pkg/worldstate"
)

func TestValidateConfigTx(t *testing.T) {
	t.Parallel()

	cert, err := ioutil.ReadFile("./testdata/sample.cert")
	require.NoError(t, err)
	dcCert, _ := pem.Decode(cert)

	setup := func(db worldstate.DB) {
		nonAdminUser := &types.User{
			ID: "nonAdminUser",
		}
		nonAdminUserSerialized, err := proto.Marshal(nonAdminUser)
		require.NoError(t, err)

		adminUser := &types.User{
			ID: "adminUser",
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

		require.NoError(t, db.Commit(newUsers))
	}

	tests := []struct {
		name           string
		tx             *types.ConfigTx
		expectedResult *types.ValidationInfo
	}{
		{
			name: "invalid: submitter does not have cluster admin privilege",
			tx: &types.ConfigTx{
				UserID: "nonAdminUser",
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: "the user [nonAdminUser] has no privilege to perform cluster administrative operations",
			},
		},
		{
			name: "invalid: node config is empty",
			tx: &types.ConfigTx{
				UserID:    "adminUser",
				NewConfig: &types.ClusterConfig{},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "node config is empty. There must be at least single node in the cluster",
			},
		},
		{
			name: "invalid: admin config is empty",
			tx: &types.ConfigTx{
				UserID: "adminUser",
				NewConfig: &types.ClusterConfig{
					Nodes: []*types.NodeConfig{
						{
							ID:          "node1",
							Address:     "127.0.0.1",
							Certificate: dcCert.Bytes,
						},
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "admin config is empty. There must be at least single admin in the cluster",
			},
		},
		{
			name: "invalid: mvcc conflict",
			tx: &types.ConfigTx{
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
							Certificate: dcCert.Bytes,
						},
					},
					Admins: []*types.Admin{
						{
							ID:          "admin1",
							Certificate: dcCert.Bytes,
						},
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_MVCC_CONFLICT_WITH_COMMITTED_STATE,
				ReasonIfInvalid: "mvcc conflict has occurred as the read old configuration does not match the committed version",
			},
		},
		{
			name: "valid",
			tx: &types.ConfigTx{
				UserID:               "adminUser",
				ReadOldConfigVersion: nil,
				NewConfig: &types.ClusterConfig{
					Nodes: []*types.NodeConfig{
						{
							ID:          "node1",
							Address:     "127.0.0.1",
							Certificate: dcCert.Bytes,
						},
					},
					Admins: []*types.Admin{
						{
							ID:          "admin1",
							Certificate: dcCert.Bytes,
						},
					},
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

			env := newValidatorTestEnv(t)
			defer env.cleanup()

			setup(env.db)

			result, err := env.validator.configTxValidator.validate(tt.tx)
			require.NoError(t, err)
			require.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestValidateNodeConfig(t *testing.T) {
	t.Parallel()

	cert, err := ioutil.ReadFile("./testdata/sample.cert")
	require.NoError(t, err)
	dcCert, _ := pem.Decode(cert)

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
				ReasonIfInvalid: "the node [node1] has an invalid certificate: Error = asn1: structure error: tags don't match (16 vs {class:1 tag:18 length:97 isCompound:true}) {optional:false explicit:false application:false private:false defaultValue:<nil> tag:<nil> stringType:0 timeType:0 set:false omitEmpty:false} certificate @2",
			},
		},
		{
			name: "invalid: duplicate nodes in the node config",
			nodes: []*types.NodeConfig{
				{
					ID:          "node1",
					Address:     "127.0.0.1",
					Certificate: dcCert.Bytes,
				},
				{
					ID:          "node1",
					Address:     "127.0.0.1",
					Certificate: dcCert.Bytes,
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
					Certificate: dcCert.Bytes,
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

			result := validateNodeConfig(tt.nodes)
			require.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestValidateAdminConfig(t *testing.T) {
	t.Parallel()

	cert, err := ioutil.ReadFile("./testdata/sample.cert")
	require.NoError(t, err)
	dcCert, _ := pem.Decode(cert)

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
					Certificate: dcCert.Bytes,
				},
				{
					ID:          "admin2",
					Certificate: []byte("random"),
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the admin [admin2] has an invalid certificate: asn1: structure error: tags don't match (16 vs {class:1 tag:18 length:97 isCompound:true}) {optional:false explicit:false application:false private:false defaultValue:<nil> tag:<nil> stringType:0 timeType:0 set:false omitEmpty:false} certificate @2",
			},
		},
		{
			name: "invalid: dulplicate adminID in the admin config",
			admins: []*types.Admin{
				{
					ID:          "admin1",
					Certificate: dcCert.Bytes,
				},
				{
					ID:          "admin1",
					Certificate: dcCert.Bytes,
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
					Certificate: dcCert.Bytes,
				},
				{
					ID:          "admin2",
					Certificate: dcCert.Bytes,
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

			result := validateAdminConfig(tt.admins)
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

				require.NoError(t, db.Commit(config))
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

				require.NoError(t, db.Commit(config))
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
