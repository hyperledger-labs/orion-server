// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"path/filepath"
	"sync"

	"github.com/hyperledger-labs/orion-server/internal/fileops"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

var (
	// trieDataDBName holds all Merkle-Particia Trie (State Trie)
	// data (both nodes and values) history
	trieDataDBName = "triedata"

	// underCreationFlag is used to mark that the store
	// is being created. If a failure happens during the
	// creation, the retry logic will use this file to
	// detect the partially created store and do cleanup
	// before creating a new store
	underCreationFlag = "undercreation"

	// Namespaces for different data types:
	// trie nodes
	trieNodesNs = []byte{0}
	// trie values
	trieValueNs = []byte{1}
	// last block stored
	lastBlockNs = []byte{2}
)

// Store maintains MPTrie nodes and values in backend store
type Store struct {
	disabled        bool
	trieDataDB      *leveldb.DB
	inMemoryNodes   map[string][]byte
	inMemoryValues  map[string][]byte
	nodesToPersist  map[string][]byte
	valuesToPersist map[string][]byte
	logger          *logger.SugarLogger
	mu              sync.RWMutex
}

// Config holds the configuration of a trie store
type Config struct {
	StoreDir string
	Disabled bool
	Logger   *logger.SugarLogger
}

type NodeBytesWithType struct {
	NodeBytes []byte `json:"NodeBytes,omitempty"`
	NodeType  string `json:"NodeType,omitempty"`
}

// Open opens the store to store MPTrie nodes and values
func Open(c *Config) (*Store, error) {
	exist, err := fileops.Exists(c.StoreDir)
	if err != nil {
		return nil, err
	}
	if !exist {
		return openNewStore(c)
	}

	partialStoreExist, err := isExistingStoreCreatedPartially(c.StoreDir)
	if err != nil {
		return nil, err
	}

	switch {
	case partialStoreExist:
		if err := fileops.RemoveAll(c.StoreDir); err != nil {
			return nil, errors.Wrap(err, "error while removing the existing partially created store")
		}

		return openNewStore(c)
	default:
		return openExistingStore(c)
	}
}

func isExistingStoreCreatedPartially(storeDir string) (bool, error) {
	empty, err := fileops.IsDirEmpty(storeDir)
	if err != nil || empty {
		return true, err
	}

	return fileops.Exists(filepath.Join(storeDir, underCreationFlag))
}

func openNewStore(c *Config) (*Store, error) {
	if c.Disabled {
		return &Store{disabled: c.Disabled}, nil
	}

	if err := fileops.CreateDir(c.StoreDir); err != nil {
		return nil, errors.WithMessagef(err, "error while creating directory [%s]", c.StoreDir)
	}

	underCreationFlagPath := filepath.Join(c.StoreDir, underCreationFlag)
	if err := fileops.CreateFile(underCreationFlagPath); err != nil {
		return nil, err
	}

	trieDataDBPath := filepath.Join(c.StoreDir, trieDataDBName)

	trieDataDB, err := leveldb.OpenFile(trieDataDBPath, &opt.Options{ErrorIfExist: true})
	if err != nil {
		return nil, errors.WithMessage(err, "error while creating an trie data database")
	}

	if err := fileops.Remove(underCreationFlagPath); err != nil {
		return nil, errors.WithMessagef(err, "error while removing the under creation flag [%s]", underCreationFlagPath)
	}

	return &Store{
		trieDataDB:      trieDataDB,
		inMemoryNodes:   make(map[string][]byte),
		inMemoryValues:  make(map[string][]byte),
		nodesToPersist:  make(map[string][]byte),
		valuesToPersist: make(map[string][]byte),
		logger:          c.Logger,
	}, nil
}

func openExistingStore(c *Config) (*Store, error) {
	if c.Disabled {
		return &Store{disabled: c.Disabled}, nil
	}

	trieDataDBPath := filepath.Join(c.StoreDir, trieDataDBName)

	trieDataDB, err := leveldb.OpenFile(trieDataDBPath, &opt.Options{ErrorIfMissing: true})
	if err != nil {
		return nil, errors.WithMessage(err, "error while opening the existing leveldb file for the trie data")
	}

	s := &Store{
		trieDataDB:      trieDataDB,
		inMemoryNodes:   make(map[string][]byte),
		inMemoryValues:  make(map[string][]byte),
		nodesToPersist:  make(map[string][]byte),
		valuesToPersist: make(map[string][]byte),
		logger:          c.Logger,
	}
	return s, nil
}

// Close closes the store
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.disabled {
		return nil
	}

	if err := s.trieDataDB.Close(); err != nil {
		return errors.WithMessage(err, "error while closing the trie data database")
	}
	return nil
}
