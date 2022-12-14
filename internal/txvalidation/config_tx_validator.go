// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package txvalidation

import (
	"fmt"
	"hash/crc32"
	"net"
	"net/url"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger-labs/orion-server/internal/identity"
	"github.com/hyperledger-labs/orion-server/internal/replication"
	"github.com/hyperledger-labs/orion-server/internal/worldstate"
	"github.com/hyperledger-labs/orion-server/pkg/certificateauthority"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/pkg/errors"
)

type ConfigTxValidator struct {
	db              worldstate.DB
	identityQuerier *identity.Querier
	sigValidator    *txSigValidator
	logger          *logger.SugarLogger
}

func (v *ConfigTxValidator) Validate(txEnv *types.ConfigTxEnvelope) (*types.ValidationInfo, error) {
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

	vi := validateConfig(tx.NewConfig)
	if vi.Flag != types.Flag_VALID {
		return vi, nil
	}

	clusterConfig, configMetadata, err := v.db.GetConfig()
	if err != nil {
		return nil, err
	}

	vi, err = v.mvccValidation(tx.ReadOldConfigVersion, configMetadata)
	if err != nil {
		return nil, err
	}
	if vi.Flag != types.Flag_VALID {
		return vi, nil
	}

	return v.validateConfigTransitionRules(clusterConfig, tx.NewConfig)
}

func (v *ConfigTxValidator) validateGenesis(txEnv *types.ConfigTxEnvelope) ([]*types.ValidationInfo, error) {
	configTx := txEnv.Payload

	vi := validateConfig(configTx.NewConfig)
	if vi.Flag != types.Flag_VALID {
		return nil, errors.Errorf("genesis block cannot be invalid: reason for invalidation [%s]", vi.ReasonIfInvalid)
	}

	return []*types.ValidationInfo{{Flag: types.Flag_VALID}}, nil
}

func validateConfig(config *types.ClusterConfig) *types.ValidationInfo {
	vi, caCertCollection := validateCAConfig(config.CertAuthConfig)
	if vi.Flag != types.Flag_VALID {
		return vi
	}

	if vi = validateNodeConfig(config.Nodes, caCertCollection); vi.Flag != types.Flag_VALID {
		return vi
	}

	if vi = validateAdminConfig(config.Admins, caCertCollection); vi.Flag != types.Flag_VALID {
		return vi
	}

	if vi = validateConsensusConfig(config.ConsensusConfig); vi.Flag != types.Flag_VALID {
		return vi
	}

	if vi = validateMembersNodesMatch(config.ConsensusConfig.Members, config.Nodes); vi.Flag != types.Flag_VALID {
		return vi
	}

	return vi
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

		default:
			if err := caCertCollection.VerifyLeafCert(n.Certificate); err != nil {
				return &types.ValidationInfo{
					Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
					ReasonIfInvalid: "the node [" + n.Id + "] has an invalid certificate: " + err.Error(),
				}
			}
		}

		if err := validateHostPort(n.Address, n.Port); err != nil {
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the node [" + n.Id + "] has " + err.Error(),
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

func (v *ConfigTxValidator) mvccValidation(readOldConfigVersion *types.Version, currentConfigMetadata *types.Metadata) (*types.ValidationInfo, error) {
	if !proto.Equal(currentConfigMetadata.GetVersion(), readOldConfigVersion) {
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

// validate whether the transition from currentConfig to updatedConfig is valid and safe.
func (v *ConfigTxValidator) validateConfigTransitionRules(currentConfig, updatedConfig *types.ClusterConfig) (*types.ValidationInfo, error) {
	nodes, consensus, ca, admins, ledger := replication.ClassifyClusterReConfig(currentConfig, updatedConfig)

	if nodes {
		v.logger.Debugf("ClusterConfig Nodes changed: current: %s; updated: %s", nodeConfigSliceToString(currentConfig.Nodes), nodeConfigSliceToString(updatedConfig.Nodes))
		// TODO add rules for nodes re-config safety
	}
	if ca {
		v.logger.Debugf("ClusterConfig CA changed: current: %v; updated: %v", currentConfig.CertAuthConfig, updatedConfig.CertAuthConfig)
		// TODO add rules for CA re-config safety: https://github.com/hyperledger-labs/orion-server/issues/154
	}

	if admins {
		v.logger.Debugf("ClusterConfig Admins changed: current: %v; updated: %v", currentConfig.Admins, updatedConfig.Admins)
		// TODO add rules for admin re-config safety: https://github.com/hyperledger-labs/orion-server/issues/262
	}

	if consensus {
		err := replication.VerifyConsensusReConfig(currentConfig.GetConsensusConfig(), updatedConfig.GetConsensusConfig(), v.logger)
		if err != nil {
			v.logger.Errorf("ClusterConfig ConsensusConfig validation failed: error: %s", err)
			v.logger.Debugf("ClusterConfig ConsensusConfig rejected change request: current: %v; updated: %v", currentConfig.ConsensusConfig, updatedConfig.ConsensusConfig)
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: fmt.Sprintf("error in ConsensusConfig: %s", err.Error()),
			}, nil
		}
		v.logger.Infof("ClusterConfig ConsensusConfig changed: current: %v; updated: %v", currentConfig.ConsensusConfig, updatedConfig.ConsensusConfig)
	}

	if ledger {
		v.logger.Debugf("ClusterConfig Ledger config changed: current: %+v; updated: %+v", currentConfig.LedgerConfig, updatedConfig.LedgerConfig)
		if currentConfig.GetLedgerConfig().GetStateMerklePatriciaTrieDisabled() != updatedConfig.GetLedgerConfig().GetStateMerklePatriciaTrieDisabled() {
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "LedgerConfig.StateMerklePatriciaTrieDisabled cannot be changed",
			}, nil
		}
	}

	return &types.ValidationInfo{
		Flag: types.Flag_VALID,
	}, nil
}

func nodeConfigToString(n *types.NodeConfig) string {
	return fmt.Sprintf("Id: %s, Address: %s, Port: %d, Cert-hash: %x", n.Id, n.Address, n.Port, crc32.ChecksumIEEE(n.Certificate))
}

func nodeConfigSliceToString(nodes []*types.NodeConfig) string {
	str := "["
	for i, n := range nodes {
		str = str + nodeConfigToString(n)
		if i != len(nodes)-1 {
			str = str + "; "
		}
	}
	str = str + "]"
	return str
}
