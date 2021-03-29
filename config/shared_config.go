// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"time"
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
	Nodes     []NodeConf
	Consensus ConsensusConf
	CAConfig  CAConfiguration
	Admin     AdminConf
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

// ConsensusConf holds the employed consensus algorithm and its parameters.
type ConsensusConf struct {
	// The consensus algorithm.
	Algorithm string
	// Members contains the set of servers that take part in consensus.
	Members []PeerConf
	// Observers contains the set of servers that are allowed to communicate Members, and fetch their state.
	Observers []PeerConf
	// Raft specific parameters.
	Raft RaftConf
}

// PeerConf defines a server take part in consensus, or an observer.
// The NodeID correlates the peer definition here with the node definition in the SharedConfiguration.Nodes.
// The Host and Port are those that are accessible from other peers.
// RaftID must be >0 for members, or =0 for observers.
type PeerConf struct {
	NodeID   string
	RaftID   uint64
	PeerHost string
	PeerPort uint32
}

// RaftConf holds Raft specific parameters.
type RaftConf struct {
	TickInterval   time.Duration
	ElectionTicks  uint32
	HeartbeatTicks uint32
}

// AdminConf holds the credentials of the blockchain
// database cluster admin such as the ID and path to
// the x509 certificate
type AdminConf struct {
	ID              string
	CertificatePath string
}

// CAConfiguration holds the path to the x509 certificates of the certificate authorities who issues all certificates.
type CAConfiguration struct {
	RootCACertsPath         []string
	IntermediateCACertsPath []string
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
