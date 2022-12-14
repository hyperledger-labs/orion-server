// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

// SharedConfiguration holds the initial configuration that will be converted into the ledger's genesis block and
// loaded into the database when the server starts with an empty ledger and database.
//
// This struct may also be used to bootstrap a new node into an existing cluster (not yet implemented).
//
// This part of the configuration is replicated and is common to all nodes.
// After the initial bootstrap, this part of the configuration can change only through configuration transactions.
type SharedConfiguration struct {
	// Nodes carry the identity, endpoint, and certificate of each database node that serves to clients.
	Nodes     []*NodeConf
	Consensus *ConsensusConf
	CAConfig  CAConfiguration
	Admin     AdminConf
	Ledger    LedgerConf
}

// NodeConf carry the identity, endpoint, and certificate of a database node that serves to clients.
// The NodeID correlates the node definition here with the peer definition in the SharedConfiguration.Consensus.
// The Host and Port are those that are accessible from clients.
// The certificate is the one used to authenticate with clients and validate the server;s signature on
// blocks and transaction/query responses.
type NodeConf struct {
	NodeID          string
	Host            string
	Port            uint32
	CertificatePath string
}

type ConsensusConf struct {
	// The consensus algorithm, currently only "raft" is supported.
	Algorithm string
	// Peers that take part in consensus.
	Members []*PeerConf
	// Peers that are allowed to connect and fetch the ledger from members, but do not take part in consensus.
	Observers []*PeerConf
	// Raft protocol parameters.
	RaftConfig *RaftConf
}

type RaftConf struct {
	// Time interval between two Node.Tick invocations. e.g. 100ms.
	TickInterval string
	// The number of Node.Tick invocations that must pass  between elections.
	// That is, if a follower does not receive any
	// message from the leader of current term before ElectionTick has
	// elapsed, it will become candidate and start an election.
	// electionTicks must be greater than heartbeatTicks.
	ElectionTicks uint32
	// The number of Node.Tick invocations that must
	// pass between heartbeats. That is, a leader sends heartbeat
	// messages to maintain its leadership every HeartbeatTick ticks.
	HeartbeatTicks uint32
	// Limits the max number of in-flight blocks (i.e. Raft messages).
	MaxInflightBlocks uint32
	// Take a snapshot when cumulative data since last snapshot exceeds a certain size in bytes.
	SnapshotIntervalSize uint64
}

// PeerConf defines a server that takes part in consensus, or an observer.
type PeerConf struct {
	// The node ID correlates the peer definition here with the NodeConfig.ID field.
	NodeId string
	// Raft ID must be >0 for members, or =0 for observers.
	RaftId uint64
	// The host name or IP address that is used by other peers to connect to this peer.
	PeerHost string
	// The port that is used by other peers to connect to this peer.
	PeerPort uint32
}

// AdminConf holds the credentials of the blockchain
// database cluster admin such as the ID and path to
// the x509 certificate
type AdminConf struct {
	ID              string
	CertificatePath string
}

// LedgerConf defines parameters on the distributed ledger capabilities and algorithms that must be defined uniformly across
// all servers.
type LedgerConf struct {
	// StateMerklePatriciaTrieDisabled disables the state Merkle-Patricia-Trie construction.
	// With MP-Trie construction disabled, the block's BlockHeader.StateMerkleTreeRootHash field will be nil.
	// This flag takes effect on deployment (bootstrap) only, from the first (genesis) block.
	// The value of this flag cannot be changed during run-time.
	StateMerklePatriciaTrieDisabled bool
}

// readSharedConfig reads the shared config from the file and returns it.
func readSharedConfig(sharedConfigFile string) (*SharedConfiguration, error) {
	if sharedConfigFile == "" {
		return nil, errors.New("path to the shared configuration file is empty")
	}

	v := viper.New()
	v.SetConfigFile(sharedConfigFile)

	if err := v.ReadInConfig(); err != nil {
		return nil, errors.Wrapf(err, "error reading shared config file: %s", sharedConfigFile)
	}

	sharedConf := &SharedConfiguration{}
	if err := v.UnmarshalExact(sharedConf); err != nil {
		return nil, errors.Wrapf(err, "unable to unmarshal shared config file: '%s' into struct", sharedConfigFile)
	}
	return sharedConf, nil
}
