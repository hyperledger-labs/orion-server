// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package blockprocessor

import (
	"fmt"
	"net"
	"net/url"
	"time"

	"github.com/IBM-Blockchain/bcdb-server/internal/identity"
	"github.com/IBM-Blockchain/bcdb-server/internal/replication"
	"github.com/IBM-Blockchain/bcdb-server/internal/worldstate"
	"github.com/IBM-Blockchain/bcdb-server/pkg/certificateauthority"
	"github.com/IBM-Blockchain/bcdb-server/pkg/logger"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
)

type configTxValidator struct {
	db              worldstate.DB
	identityQuerier *identity.Querier
	sigValidator    *txSigValidator
	logger          *logger.SugarLogger
}

func (v *configTxValidator) validate(txEnv *types.ConfigTxEnvelope) (*types.ValidationInfo, error) {
	valInfo, err := v.sigValidator.validate(txEnv.Payload.UserId, txEnv.Signature, txEnv.Payload)
	if err != nil || valInfo.Flag != types.Flag_VALID {
		return valInfo, err
	}

	tx := txEnv.Payload
	hasPerm, err := v.identityQuerier.HasAdministrationPrivilege(tx.UserId)
	if err != nil {
		return nil, errors.WithMessagef(err, "error while checking cluster administrative privilege for user [%s]", tx.UserId)
	}
	if !hasPerm {
		return &types.ValidationInfo{
			Flag:            types.Flag_INVALID_NO_PERMISSION,
			ReasonIfInvalid: "the user [" + tx.UserId + "] has no privilege to perform cluster administrative operations",
		}, nil
	}

	if tx.NewConfig == nil {
		return &types.ValidationInfo{
			Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
			ReasonIfInvalid: "new config is empty. There must be at least single node and an admin in the cluster",
		}, nil
	}

	vi, caCertCollection := validateCAConfig(tx.NewConfig.CertAuthConfig)
	if vi.Flag != types.Flag_VALID {
		return vi, nil
	}

	if r := validateNodeConfig(tx.NewConfig.Nodes, caCertCollection); r.Flag != types.Flag_VALID {
		return r, nil
	}

	if r := validateAdminConfig(tx.NewConfig.Admins, caCertCollection); r.Flag != types.Flag_VALID {
		return r, nil
	}

	if r := validateConsensusConfig(tx.NewConfig.ConsensusConfig); r.Flag != types.Flag_VALID {
		return r, nil
	}

	if r := validateMembersNodesMatch(tx.NewConfig.ConsensusConfig.Members, tx.NewConfig.Nodes); r.Flag != types.Flag_VALID {
		return r, nil
	}

	vi, err = v.mvccValidation(tx.ReadOldConfigVersion)
	if err != nil {
		return nil, err
	}
	if vi.Flag != types.Flag_VALID {
		return vi, nil
	}

	clusterConfig, _, err := v.db.GetConfig()
	if err != nil {
		return nil, err
	}

	return validateConfigUpdateRules(clusterConfig, tx.NewConfig)
}

func validateCAConfig(caConfig *types.CAConfig) (*types.ValidationInfo, *certificateauthority.CACertCollection) {
	if caConfig == nil {
		return &types.ValidationInfo{
			Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
			ReasonIfInvalid: "CA config is empty. At least one root CA is required",
		}, nil
	}
	if len(caConfig.Roots) == 0 {
		return &types.ValidationInfo{
			Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
			ReasonIfInvalid: "CA config Roots is empty. At least one root CA is required",
		}, nil
	}

	caCertCollection, err := certificateauthority.NewCACertCollection(caConfig.Roots, caConfig.Intermediates)
	if err != nil {
		return &types.ValidationInfo{
			Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
			ReasonIfInvalid: fmt.Sprintf("CA certificate collection cannot be created: %s", err.Error()),
		}, nil
	}

	err = caCertCollection.VerifyCollection()
	if err != nil {
		return &types.ValidationInfo{
			Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
			ReasonIfInvalid: fmt.Sprintf("CA certificate collection is invalid: %s", err.Error()),
		}, nil
	}

	return &types.ValidationInfo{
		Flag: types.Flag_VALID,
	}, caCertCollection
}

func validateNodeConfig(nodes []*types.NodeConfig, caCertCollection *certificateauthority.CACertCollection) *types.ValidationInfo {
	if len(nodes) == 0 {
		return &types.ValidationInfo{
			Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
			ReasonIfInvalid: "node config is empty. There must be at least single node in the cluster",
		}
	}

	nodeIDsSet := make(map[string]bool)
	hostPortSet := make(map[string]bool)

	for _, n := range nodes {
		switch {
		case n == nil:
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there is an empty node entry in the node config",
			}

		case n.Id == "":
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there is a node in the node config with an empty ID. A valid nodeID must be an non-empty string",
			}

		case n.Address == "":
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the node [" + n.Id + "] has an empty ip address",
			}

		case net.ParseIP(n.Address) == nil:
			// TODO allow host names for cluster nodes
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the node [" + n.Id + "] has an invalid ip address [" + n.Address + "]",
			}

		case n.Port == 0 || n.Port >= (1<<16):
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: fmt.Sprintf("the node [%s] has an invalid port number [%d]", n.Id, n.Port),
			}

		default:
			if err := caCertCollection.VerifyLeafCert(n.Certificate); err != nil {
				return &types.ValidationInfo{
					Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
					ReasonIfInvalid: "the node [" + n.Id + "] has an invalid certificate: " + err.Error(),
				}
			}
		}

		// node ID must be unique
		if nodeIDsSet[n.Id] {
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there are two nodes with the same ID [" + n.Id + "] in the node config. The node IDs must be unique",
			}
		}
		nodeIDsSet[n.Id] = true

		// node host:port must be unique as well
		hostPort := fmt.Sprintf("%s:%d", n.Address, n.Port)
		if hostPortSet[hostPort] {
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there are two nodes with the same Host:Port [" + hostPort + "] in the node config. Endpoints must be unique",
			}
		}
		hostPortSet[hostPort] = true
	}

	return &types.ValidationInfo{
		Flag: types.Flag_VALID,
	}
}

func validateAdminConfig(admins []*types.Admin, caCertCollection *certificateauthority.CACertCollection) *types.ValidationInfo {
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

		case a.Id == "":
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there is an admin in the admin config with an empty ID. A valid adminID must be an non-empty string",
			}
		default:
			if err := caCertCollection.VerifyLeafCert(a.Certificate); err != nil {
				return &types.ValidationInfo{
					Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
					ReasonIfInvalid: "the admin [" + a.Id + "] has an invalid certificate: " + err.Error(),
				}
			}
		}

		if adminIDs[a.Id] {
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there are two admins with the same ID [" + a.Id + "] in the admin config. The admin IDs must be unique",
			}
		}
		adminIDs[a.Id] = true
	}

	return &types.ValidationInfo{
		Flag: types.Flag_VALID,
	}
}

// validate the internal consistency of the ConsensusConfig
func validateConsensusConfig(consensusConf *types.ConsensusConfig) *types.ValidationInfo {
	switch {
	case consensusConf == nil:
		return &types.ValidationInfo{
			Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
			ReasonIfInvalid: "Consensus config is empty.",
		}

	case consensusConf.Algorithm != "raft":
		return &types.ValidationInfo{
			Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
			ReasonIfInvalid: fmt.Sprintf("Consensus config Algorithm '%s' is not supported.", consensusConf.Algorithm),
		}

	case len(consensusConf.Members) == 0:
		return &types.ValidationInfo{
			Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
			ReasonIfInvalid: "Consensus config has no member peers. At least one member peer is required.",
		}
	}

	nodeIDsSet := make(map[string]bool)
	hostPortSet := make(map[string]bool)
	raftIDSet := make(map[uint64]bool)

	for _, m := range consensusConf.Members {
		switch {
		case m == nil:
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "Consensus config has an empty member entry.",
			}

		case m.NodeId == "":
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "Consensus config has a member with an empty ID. A valid nodeID must be an non-empty string.",
			}

		case m.RaftId == 0:
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: fmt.Sprintf("Consensus config has a member [%s] with Raft ID 0, must be >0.", m.NodeId),
			}
		}

		if err := validateHostPort(m.PeerHost, m.PeerPort); err != nil {
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: fmt.Sprintf("Consensus config has a member [%s] with %s", m.NodeId, err.Error()),
			}
		}

		// node ID must be unique
		if nodeIDsSet[m.NodeId] {
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "Consensus config has two members with the same ID [" + m.NodeId + "], the node IDs must be unique.",
			}
		}
		nodeIDsSet[m.NodeId] = true

		// peer raft IDs must be unique
		if raftIDSet[m.RaftId] {
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: fmt.Sprintf("Consensus config has two members with the same Raft ID [%d], Raft IDs must be unique.", m.RaftId),
			}
		}
		raftIDSet[m.RaftId] = true

		// peer host:port must be unique
		hostPort := fmt.Sprintf("%s:%d", m.PeerHost, m.PeerPort)
		if hostPortSet[hostPort] {
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "Consensus config has two members with the same Host:Port [" + hostPort + "], endpoints must be unique.",
			}
		}
		hostPortSet[hostPort] = true
	}

	for _, o := range consensusConf.Observers {
		switch {
		case o == nil:
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "Consensus config has an empty observer entry.",
			}

		case o.NodeId == "":
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "Consensus config has an observer with an empty ID. A valid nodeID must be an non-empty string.",
			}

		case o.RaftId != 0:
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: fmt.Sprintf("Consensus config has an observer [%s] with Raft ID >0.", o.NodeId),
			}
		}

		if err := validateHostPort(o.PeerHost, o.PeerPort); err != nil {
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: fmt.Sprintf("Consensus config has an observer [%s] with %s", o.NodeId, err.Error()),
			}
		}

		// node ID must be unique, across members as well
		if nodeIDsSet[o.NodeId] {
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "Consensus config has two peers with the same ID [" + o.NodeId + "], the node IDs must be unique.",
			}
		}
		nodeIDsSet[o.NodeId] = true

		// peer host:port must be unique, across members as well
		hostPort := fmt.Sprintf("%s:%d", o.PeerHost, o.PeerPort)
		if hostPortSet[hostPort] {
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "Consensus config has two peers with the same Host:Port [" + hostPort + "], endpoints must be unique.",
			}
		}
		hostPortSet[hostPort] = true
	}

	switch {
	case consensusConf.RaftConfig == nil:
		return &types.ValidationInfo{
			Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
			ReasonIfInvalid: "Consensus config RaftConfig is empty.",
		}

	case consensusConf.RaftConfig.TickInterval == "":
		return &types.ValidationInfo{
			Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
			ReasonIfInvalid: "Consensus config RaftConfig.TickInterval is empty.",
		}

	case consensusConf.RaftConfig.HeartbeatTicks == 0:
		return &types.ValidationInfo{
			Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
			ReasonIfInvalid: "Consensus config RaftConfig.HeartbeatTicks is 0.",
		}

	case consensusConf.RaftConfig.ElectionTicks == 0:
		return &types.ValidationInfo{
			Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
			ReasonIfInvalid: "Consensus config RaftConfig.ElectionTicks is 0.",
		}
	}

	if d, err := time.ParseDuration(consensusConf.RaftConfig.TickInterval); err != nil {
		return &types.ValidationInfo{
			Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
			ReasonIfInvalid: "Consensus config RaftConfig.TickInterval is invalid: " + err.Error(),
		}
	} else if d <= 0 {
		return &types.ValidationInfo{
			Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
			ReasonIfInvalid: "Consensus config RaftConfig.TickInterval is invalid: " + consensusConf.RaftConfig.TickInterval,
		}
	}

	return &types.ValidationInfo{
		Flag: types.Flag_VALID,
	}
}

func validateMembersNodesMatch(members []*types.PeerConfig, nodes []*types.NodeConfig) *types.ValidationInfo {
	if len(nodes) != len(members) {
		return &types.ValidationInfo{
			Flag: types.Flag_INVALID_INCORRECT_ENTRIES,
			ReasonIfInvalid: fmt.Sprintf(
				"ClusterConfig.Nodes must be the same length as ClusterConfig.ConsensusConfig.Members, and Nodes set must include all Members"),
		}
	}

	nodesMap := make(map[string]*types.NodeConfig)
	for _, n := range nodes {
		nodesMap[n.Id] = n
	}

	for _, m := range members {
		if n, ok := nodesMap[m.NodeId]; !ok {
			return &types.ValidationInfo{
				Flag: types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: fmt.Sprintf(
					"ClusterConfig.Nodes set does not include ClusterConfig.ConsensusConfig.Members peer [%s], Nodes set must include all Members.", m.NodeId),
			}
		} else {
			nHostPort := fmt.Sprintf("%s:%d", n.Address, n.Port)
			mHostPort := fmt.Sprintf("%s:%d", m.PeerHost, m.PeerPort)
			if nHostPort == mHostPort {
				return &types.ValidationInfo{
					Flag: types.Flag_INVALID_INCORRECT_ENTRIES,
					ReasonIfInvalid: fmt.Sprintf(
						"ClusterConfig node [%s] and respective peer have the same endpoint [%s], node and peer endpoints must be unique.", m.NodeId, nHostPort),
				}
			}
		}
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

func validateHostPort(host string, port uint32) error {
	if host == "" {
		return errors.New("an empty host")
	}

	// Check for either an IP address or a hostname
	if net.ParseIP(host) == nil {
		u, err := url.Parse("bogus://" + host)
		if err != nil {
			return errors.New(fmt.Sprintf("an invalid host [%s]", host))
		}
		if u.Hostname() != host {
			return errors.New(fmt.Sprintf("an invalid host [%s]", host))
		}
	}

	if port == 0 || port >= (1<<16) {
		return errors.Errorf("an invalid port number [%d]", port)

	}

	return nil
}

func validateConfigUpdateRules(currentConfig, updatedConfig *types.ClusterConfig) (*types.ValidationInfo, error) {
	//TODO add rules for safe cluster re-config
	nodes, consensus, _, _ := replication.ClassifyClusterReConfig(currentConfig, updatedConfig)
	if nodes || consensus {
		return &types.ValidationInfo{
			Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
			ReasonIfInvalid: "dynamic cluster re-config of Nodes & ConsensusConfig is not yet supported",
		}, nil
	}

	return &types.ValidationInfo{
		Flag: types.Flag_VALID,
	}, nil
}
