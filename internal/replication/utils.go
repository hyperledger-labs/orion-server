// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package replication

import (
	"fmt"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"hash/crc64"
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
