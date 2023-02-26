// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package leveldb

import (
	"path/filepath"
	"regexp"
	"sync"

	"github.com/hyperledger-labs/orion-server/internal/fileops"
	"github.com/hyperledger-labs/orion-server/internal/worldstate"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"go.uber.org/multierr"
)

var (
	// underCreationFlag is used to mark that the leveldb
	// instance is being created. If a failure happens during the
	// creation, the retry logic will use this file to
	// detect the partially created store and do cleanup
	// before creating a new levelDB instance
	underCreationFlag = "undercreation"
	// allowedCharsInDBName holds the regexp for allowed characters
	// in a database name
	allowedCharsInDBName = `^[0-9a-zA-Z_\-\.]+$`
)

// LevelDB holds information about all created database
type LevelDB struct {
	dbRootDir   string
	dbs         sync.Map
	logger      *logger.SugarLogger
	dbNameRegex *regexp.Regexp
	cache       *cache
}

// Db - a wrapper on an actual store
type Db struct {
	name      string
	file      *leveldb.DB
	readOpts  *opt.ReadOptions
	writeOpts *opt.WriteOptions
}

var (
	preCreateDBs = append(
		worldstate.SystemDBs(),
		worldstate.DefaultDBName,
	)
)

type Config struct {
	DBRootDir string
	Logger    *logger.SugarLogger
}

// Open opens a leveldb instance to maintain world state
func Open(conf *Config) (*LevelDB, error) {
	exist, err := fileops.Exists(conf.DBRootDir)
	if err != nil {
		return nil, err
	}
	if !exist {
		return openNewLevelDBInstance(conf)
	}

	partialInstanceExist, err := isExistingLevelDBInstanceCreatedPartially(conf.DBRootDir)
	if err != nil {
		return nil, err
	}

	switch {
	case partialInstanceExist:
		if err := fileops.RemoveAll(conf.DBRootDir); err != nil {
			return nil, errors.Wrap(err, "error while removing the existing partially created levelDB instance")
		}

		return openNewLevelDBInstance(conf)
	default:
		return openExistingLevelDBInstance(conf)
	}
}

func isExistingLevelDBInstanceCreatedPartially(dbPath string) (bool, error) {
	empty, err := fileops.IsDirEmpty(dbPath)
	if err != nil {
		return false, err
	}

	if empty {
		return true, nil
	}

	return fileops.Exists(filepath.Join(dbPath, underCreationFlag))
}

func openNewLevelDBInstance(c *Config) (*LevelDB, error) {
	if err := fileops.CreateDir(c.DBRootDir); err != nil {
		return nil, errors.WithMessagef(err, "failed to create director %s", c.DBRootDir)
	}

	underCreationFlagPath := filepath.Join(c.DBRootDir, underCreationFlag)
	if err := fileops.CreateFile(underCreationFlagPath); err != nil {
		return nil, err
	}

	l, err := openLevelDBInstance(c, preCreateDBs)
	if err != nil {
		return nil, err
	}

	if err := fileops.Remove(underCreationFlagPath); err != nil {
		return nil, errors.WithMessagef(err, "error while removing the under creation flag [%s]", underCreationFlagPath)
	}

	return l, nil
}

func openExistingLevelDBInstance(c *Config) (*LevelDB, error) {
	dbNames, err := fileops.ListSubdirs(c.DBRootDir)
	if err != nil {
		return nil, errors.WithMessagef(err, "failed to retrieve existing level dbs from %s", c.DBRootDir)
	}

	return openLevelDBInstance(c, dbNames)
}

func openLevelDBInstance(c *Config, dbNames []string) (*LevelDB, error) {
	l := &LevelDB{
		dbRootDir:   c.DBRootDir,
		logger:      c.Logger,
		dbNameRegex: regexp.MustCompile(allowedCharsInDBName),
		cache:       newCache(128),
	}

	for _, dbName := range dbNames {
		if err := l.create(dbName); err != nil {
			return nil, err
		}
	}

	return l, nil
}

// Close closes the database instance by closing all leveldb databases
func (l *LevelDB) Close() error {
	var aggErr []error
	l.dbs.Range(func(name, value interface{}) bool {
		db := value.(*Db)
		if err := db.file.Close(); err != nil {
			aggErr = append(aggErr, errors.Wrapf(err, "error while closing database %s", name))
		}
		return true
	})
	l.dbs = sync.Map{}

	if len(aggErr) > 0 {
		return multierr.Combine(aggErr...)
	}
	return nil
}

// ValidDBName returns true if the given dbName is valid
func (l *LevelDB) ValidDBName(dbName string) bool {
	return l.dbNameRegex.MatchString(dbName)
}

func (l *LevelDB) getDB(name string) (*Db, bool) {
	value, ok := l.dbs.Load(name)
	if !ok {
		return nil, ok
	}
	return value.(*Db), ok
}

func (l *LevelDB) getAndDelDB(name string) (*Db, bool) {
	value, loaded := l.dbs.LoadAndDelete(name)
	if !loaded {
		return nil, loaded
	}
	return value.(*Db), loaded
}

func (l *LevelDB) setDB(name string, value *Db) {
	l.dbs.Store(name, value)
}

// size is a costly operation. It is used only for testing.
func (l *LevelDB) size() int {
	sz := 0
	l.dbs.Range(func(key, value interface{}) bool {
		sz += 1
		return true
	})
	return sz
}
