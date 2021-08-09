// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package replication

import (
	"fmt"
	"hash/crc64"

	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
	"github.com/golang/protobuf/proto"
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

// ClassifyClusterReConfig detects the kind of changes that happened in the ClusterConfig.
func ClassifyClusterReConfig(currentConfig, updatedConfig *types.ClusterConfig) (nodes bool, consensus bool, ca bool, admins bool) {
	nodes = changedNodes(currentConfig.GetNodes(), updatedConfig.GetNodes())

	curConsensus := currentConfig.GetConsensusConfig()
	updConsensus := updatedConfig.GetConsensusConfig()
	consensus = !proto.Equal(curConsensus, updConsensus)

	curCA := currentConfig.GetCertAuthConfig()
	updCA := updatedConfig.GetCertAuthConfig()
	ca = !proto.Equal(curCA, updCA)

	admins = changedAdmins(currentConfig.GetAdmins(), updatedConfig.GetAdmins())

	return nodes, consensus, ca, admins
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
