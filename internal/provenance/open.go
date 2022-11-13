// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package provenance

import (
	"os"
	"path"
	"path/filepath"
	"sync"

	"github.com/cayleygraph/cayley"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/kv"
	db "github.com/cayleygraph/cayley/graph/kv/leveldb"
	"github.com/hidal-go/hidalgo/kv/flat/leveldb"
	"github.com/hyperledger-labs/orion-server/internal/fileops"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/pkg/errors"
)

func init() {
	kv.Register(leveldb.Name, kv.Registration{
		NewFunc:      db.Open,
		InitFunc:     db.Create,
		IsPersistent: true,
	})
}

var (
	// underCreationFlag is used to mark that the provenancestore
	// is being created. If a failure happens during the
	// creation, the retry logic will use this file to
	// detect the partially created store and do cleanup
	// before creating a new levelDB instance
	underCreationFlag = "undercreation"

	// disabled is used to mark that the provenance store is disabled.
	// Once disabled, the provenance store cannot be er-enabled.
	disabledFlag = "disabled"
)

// Store holds information about the provenance store, i.e., a
// graph database
type Store struct {
	rootDir     string
	cayleyGraph *cayley.Handle
	mutex       sync.RWMutex
	logger      *logger.SugarLogger
}

// Config holds the configuration parameter of the
// provenance store
type Config struct {
	StoreDir string
	Disabled bool
	Logger   *logger.SugarLogger
}

// Open opens a provenance store to maintain historical values of each state.
//
// If the provenance store is Config.Disabled is set, the disabled-flag file is created and nil is returned. If the
// disabled-flag file exists, the store cannot be re-enabled, and passing Config.Disabled=false results in an error.
func Open(conf *Config) (*Store, error) {
	exist, err := fileops.Exists(conf.StoreDir)
	if err != nil {
		return nil, err
	}
	if !exist {
		return openNewProvenanceStore(conf)
	}

	partialInstanceExist, err := isExistingProvenanceStoreCreatedPartially(conf.StoreDir)
	if err != nil {
		return nil, err
	}

	if partialInstanceExist {
		if err := fileops.RemoveAll(conf.StoreDir); err != nil {
			return nil, errors.Wrap(err, "error while removing the existing partially created provenance store")
		}
		return openNewProvenanceStore(conf)
	}

	if conf.Disabled {
		conf.Logger.Info("The provenance store is disabled on this node.")
		disabledFlagPath := filepath.Join(conf.StoreDir, disabledFlag)
		if _, err := os.Create(disabledFlagPath); err != nil {
			return nil, err
		}
		return nil, nil
	}

	return openExistingLevelDBInstance(conf)
}

func isExistingProvenanceStoreCreatedPartially(dbPath string) (bool, error) {
	empty, err := fileops.IsDirEmpty(dbPath)
	if err != nil {
		return false, err
	}

	if empty {
		return true, nil
	}

	return fileops.Exists(filepath.Join(dbPath, underCreationFlag))
}

func openNewProvenanceStore(c *Config) (*Store, error) {
	if err := fileops.CreateDir(c.StoreDir); err != nil {
		return nil, errors.WithMessagef(err, "failed to create director %s", c.StoreDir)
	}

	if c.Disabled {
		c.Logger.Info("The provenance store is disabled on this node.")
		disabledFlagPath := filepath.Join(c.StoreDir, disabledFlag)
		if err := fileops.CreateFile(disabledFlagPath); err != nil {
			return nil, err
		}
		return nil, nil
	}

	underCreationFlagPath := filepath.Join(c.StoreDir, underCreationFlag)
	if err := fileops.CreateFile(underCreationFlagPath); err != nil {
		return nil, err
	}

	if err := graph.InitQuadStore(leveldb.Name, c.StoreDir, nil); err != nil {
		return nil, err
	}

	cayleyGraph, err := cayley.NewGraph(leveldb.Name, c.StoreDir, nil)
	if err != nil {
		return nil, err
	}

	if err := fileops.Remove(underCreationFlagPath); err != nil {
		return nil, errors.WithMessagef(err, "error while removing the under creation flag [%s]", underCreationFlagPath)
	}

	return &Store{
		rootDir:     c.StoreDir,
		cayleyGraph: cayleyGraph,
		logger:      c.Logger,
	}, nil
}

func openExistingLevelDBInstance(c *Config) (*Store, error) {
	disabledFlagPath := path.Join(c.StoreDir, disabledFlag)
	exists, err := fileops.Exists(disabledFlagPath)
	if err != nil {
		return nil, errors.WithMessagef(err, "error while checking disabled flag: %s", disabledFlagPath)
	}
	if exists {
		return nil, errors.Errorf("provenance store was disabled and cannot be re-enabled: disabled flag exists: %s", disabledFlagPath)
	}

	cayleyGraph, err := cayley.NewGraph(leveldb.Name, c.StoreDir, nil)
	if err != nil {
		return nil, err
	}

	return &Store{
		rootDir:     c.StoreDir,
		cayleyGraph: cayleyGraph,
		logger:      c.Logger,
	}, nil
}

// Close closes the database instance by closing all leveldb databases
func (s *Store) Close() error {
	// when provenance is disabled, there is a nil pointer to it.
	if s == nil {
		return nil
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if err := s.cayleyGraph.Close(); err != nil {
		return errors.Wrap(err, "error closing provenance store")
	}

	return nil
}
