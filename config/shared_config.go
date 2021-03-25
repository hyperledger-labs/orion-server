// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"time"
)

type SharedConfiguration struct {
	Nodes     []DBNodeConf
	Consensus ConsensusConf
	CAConfig  CAConfiguration
	Admin     AdminConf
}

type DBNodeConf struct {
	NodeID          string
	Host            string
	Port            uint32
	CertificatePath string
}

// ConsensusConf holds the employed consensus algorithm and its parameters
type ConsensusConf struct {
	Algorithm string
	Members   []PeerConf
	Observers []PeerConf
	Raft      RaftConf
}

type PeerConf struct {
	NodeID   string
	RaftID   uint64
	PeerHost string
	PeerPort uint32
}

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

// ReadSharedConfig reads the shared config from the file and returns it.
func ReadSharedConfig(sharedConfigFile string) (*SharedConfiguration, error) {
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
