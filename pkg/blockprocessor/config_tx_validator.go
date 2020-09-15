package blockprocessor

import (
	"crypto/x509"
	"net"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.ibm.com/blockchaindb/library/pkg/logger"
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/pkg/identity"
	"github.ibm.com/blockchaindb/server/pkg/worldstate"
)

type configTxValidator struct {
	db              worldstate.DB
	identityQuerier *identity.Querier
	logger          *logger.SugarLogger
}

func (v *configTxValidator) validate(tx *types.ConfigTx) (*types.ValidationInfo, error) {
	hasPerm, err := v.identityQuerier.HasClusterAdministrationPrivilege(tx.UserID)
	if err != nil {
		return nil, errors.WithMessagef(err, "error while checking cluster administrative privilege for user [%s]", tx.UserID)
	}
	if !hasPerm {
		return &types.ValidationInfo{
			Flag:            types.Flag_INVALID_NO_PERMISSION,
			ReasonIfInvalid: "the user [" + tx.UserID + "] has no privilege to perform cluster administrative operations",
		}, nil
	}

	if r := validateNodeConfig(tx.NewConfig.Nodes); r.Flag != types.Flag_VALID {
		return r, nil
	}

	if r := validateAdminConfig(tx.NewConfig.Admins); r.Flag != types.Flag_VALID {
		return r, nil
	}

	return v.mvccValidation(tx.ReadOldConfigVersion)
}

func validateNodeConfig(nodes []*types.NodeConfig) *types.ValidationInfo {
	if len(nodes) == 0 {
		return &types.ValidationInfo{
			Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
			ReasonIfInvalid: "node config is empty. There must be at least single node in the cluster",
		}
	}

	nodeIDs := make(map[string]bool)

	for _, n := range nodes {
		switch {
		case n == nil:
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there is an empty node entry in the node config",
			}

		case n.ID == "":
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there is a node in the node config with an empty ID. A valid nodeID must be an non-empty string",
			}

		case n.Address == "":
			// TODO: ip address must be unique as well
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the node [" + n.ID + "] has an empty ip address",
			}

		case net.ParseIP(n.Address) == nil:
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the node [" + n.ID + "] has an invalid ip address [" + n.Address + "]",
			}

		default:
			// TODO: should the certificate be unique as well?
			if _, err := x509.ParseCertificate(n.Certificate); err != nil {
				return &types.ValidationInfo{
					Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
					ReasonIfInvalid: "the node [" + n.ID + "] has an invalid certificate: Error = " + err.Error(),
				}
			}
		}

		if nodeIDs[n.ID] {
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there are two nodes with the same ID [" + n.ID + "] in the node config. The node IDs must be unique",
			}
		}
		nodeIDs[n.ID] = true
	}

	return &types.ValidationInfo{
		Flag: types.Flag_VALID,
	}
}

func validateAdminConfig(admins []*types.Admin) *types.ValidationInfo {
	if len(admins) == 0 {
		return &types.ValidationInfo{
			Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
			ReasonIfInvalid: "admin config is empty. There must be at least single admin in the cluster",
		}
	}

	adminIDs := make(map[string]bool)

	for _, a := range admins {
		switch {
		case a == nil:
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there is an empty admin entry in the admin config",
			}

		case a.ID == "":
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there is an admin in the admin config with an empty ID. A valid adminID must be an non-empty string",
			}
		default:
			if _, err := x509.ParseCertificate(a.Certificate); err != nil {
				return &types.ValidationInfo{
					Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
					ReasonIfInvalid: "the admin [" + a.ID + "] has an invalid certificate: " + err.Error(),
				}
			}
		}

		if adminIDs[a.ID] {
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there are two admins with the same ID [" + a.ID + "] in the admin config. The admin IDs must be unique",
			}
		}
		adminIDs[a.ID] = true
	}

	return &types.ValidationInfo{
		Flag: types.Flag_VALID,
	}
}

func (v *configTxValidator) mvccValidation(readOldConfigVersion *types.Version) (*types.ValidationInfo, error) {
	_, metadata, err := v.db.GetConfig()
	if err != nil {
		return nil, errors.WithMessage(err, "error while executing mvcc validation on read config")
	}

	if !proto.Equal(metadata.GetVersion(), readOldConfigVersion) {
		return &types.ValidationInfo{
			Flag:            types.Flag_INVALID_MVCC_CONFLICT_WITH_COMMITTED_STATE,
			ReasonIfInvalid: "mvcc conflict has occurred as the read old configuration does not match the committed version",
		}, nil
	}

	return &types.ValidationInfo{
		Flag: types.Flag_VALID,
	}, nil
}
