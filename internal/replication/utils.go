// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package replication

import (
	"fmt"
	"hash/crc64"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
)

func raftPeers(config *types.ClusterConfig) []raft.Peer {
	var peers []raft.Peer
	for _, m := range config.ConsensusConfig.Members {
		peers = append(peers, raft.Peer{ID: m.GetRaftId()})
	}
	return peers
}

func raftEntryString(e raftpb.Entry) string {
	h := crc64.New(crc64.MakeTable(crc64.ISO))
	h.Write(e.Data)
	return fmt.Sprintf("{Term:%d Index:%d Type:%v Data(len):%d Data(hash):%X}",
		e.Term, e.Index, e.Type, len(e.Data), h.Sum64())
}

// VerifyConsensusReConfig checks the configuration changes in types.ConsensusConfig.
//
// This method checks that the changes between one ConsensusConfig to the next are safe, because some mutations might
// cause a permanent loss of quorum in the cluster, something that is very difficult to recover from.
// - Members can be added or removed (membership change) one member at a time
// - Members' endpoints cannot be changed together with a membership change
// - Members' endpoints can be updated one at a time
// - An existing member cannot change its Raft ID (it must be removed from the cluster and added again as a new member)
// - The Raft ID of a new member must be unique - therefore it must be larger than MaxRaftId
//
// We assume that both the current and updated ClusterConfig are internally consistent, specifically, that the Nodes
// and the ConsensusConfig.Members arrays match by NodeId in each.
func VerifyConsensusReConfig(currentConfig, updatedConfig *types.ConsensusConfig, lg *logger.SugarLogger) error {
	addedPeers, removedPeers, changedPeers, err := detectPeerConfigChanges(currentConfig, updatedConfig)
	if err != nil {
		return err
	}

	if len(addedPeers)+len(removedPeers) > 1 {
		return errors.Errorf("cannot make more than one membership change at a time: %d added, %d removed", len(addedPeers), len(removedPeers))
	}

	if (len(addedPeers)+len(removedPeers) == 1) && len(changedPeers) > 1 {
		return errors.Errorf("cannot update peer endpoints while making membership changes: %d added, %d removed, %d updated", len(addedPeers), len(removedPeers), len(changedPeers))
	}

	if !proto.Equal(currentConfig.RaftConfig, updatedConfig.RaftConfig) {
		lg.Warning("ConsensusConfig RaftConfig changed, the new RaftConfig will be applied after server restart!")
	}

	return nil
}

func detectPeerConfigChanges(currentConfig, updatedConfig *types.ConsensusConfig) (addedPeers, removedPeers, changedPeers []*types.PeerConfig, err error) {
	currPeers := make(map[string]*types.PeerConfig)
	for _, m := range currentConfig.Members {
		currPeers[m.NodeId] = m
	}

	updtPeers := make(map[string]*types.PeerConfig)
	for _, updtMember := range updatedConfig.Members {
		updtPeers[updtMember.NodeId] = updtMember
		if currMember, ok := currPeers[updtMember.NodeId]; ok {
			// existing peer
			if !proto.Equal(updtMember, currMember) {
				if updtMember.RaftId != currMember.RaftId {
					return nil, nil, nil,
						errors.Errorf("cannot change the RaftId of an existing peer: NodeId=%s, current=%d, updated=%d",
							currMember.NodeId, currMember.RaftId, updtMember.RaftId)
				}
				changedPeers = append(changedPeers, updtMember) //endpoint changed
			}
		} else {
			// added peer
			if updtMember.RaftId <= currentConfig.RaftConfig.MaxRaftId {
				return nil, nil, nil,
					errors.Errorf("the RaftId of a new peer must be unique,  > MaxRaftId [%d]; but: NodeId=%s, RaftID=%d",
						currentConfig.RaftConfig.MaxRaftId, updtMember.NodeId, updtMember.RaftId)
			}
			addedPeers = append(addedPeers, updtMember)
		}
	}

	for _, currMember := range currPeers {
		if _, ok := updtPeers[currMember.NodeId]; !ok {
			// removed peer
			removedPeers = append(removedPeers, currMember)
		}
	}

	return
}

// ClassifyClusterReConfig detects the kind of changes that happened in the ClusterConfig.
// We assume that both the current and updated config are internally consistent (valid), but not necessarily with
// respect to each other.
func ClassifyClusterReConfig(currentConfig, updatedConfig *types.ClusterConfig) (nodes, consensus, ca, admins, ledger bool) {
	nodes = changedNodes(currentConfig.GetNodes(), updatedConfig.GetNodes())

	curConsensus := currentConfig.GetConsensusConfig()
	updConsensus := updatedConfig.GetConsensusConfig()
	consensus = !proto.Equal(curConsensus, updConsensus)

	curCA := currentConfig.GetCertAuthConfig()
	updCA := updatedConfig.GetCertAuthConfig()
	ca = !proto.Equal(curCA, updCA)

	admins = changedAdmins(currentConfig.GetAdmins(), updatedConfig.GetAdmins())

	curLedger := currentConfig.GetLedgerConfig()
	updLedger := updatedConfig.GetLedgerConfig()
	ledger = !proto.Equal(curLedger, updLedger)

	return nodes, consensus, ca, admins, ledger
}

func changedAdmins(curAdmins []*types.Admin, updAdmins []*types.Admin) bool {
	if len(curAdmins) != len(updAdmins) {
		return true
	}

	for _, curA := range curAdmins {
		found := false
		for _, updA := range updAdmins {
			if proto.Equal(curA, updA) {
				found = true
				break
			}
		}
		if !found {
			return true
		}
	}

	return false
}

func changedNodes(curNodes []*types.NodeConfig, updNodes []*types.NodeConfig) bool {
	if len(curNodes) != len(updNodes) {
		return true
	}

	for _, curN := range curNodes {
		found := false
		for _, updN := range updNodes {
			if proto.Equal(curN, updN) {
				found = true
				break
			}
		}
		if !found {
			return true
		}
	}

	return false
}
