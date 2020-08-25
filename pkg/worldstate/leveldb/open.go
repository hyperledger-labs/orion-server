package leveldb

import (
	"fmt"
	"path/filepath"
	"sync"

	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.ibm.com/blockchaindb/server/pkg/fileops"
	"github.ibm.com/blockchaindb/server/pkg/worldstate"
)

var (
	// underCreationFlag is used to mark that the leveldb
	// instance is being created. If a failure happens during the
	// creation, the retry logic will use this file to
	// detect the partially created store and do cleanup
	// before creating a new levelDB instance
	underCreationFlag = "undercreation"
)

// LevelDB holds information about all created database
type LevelDB struct {
	dbRootDir string
	dbs       map[string]*db
	dbsList   sync.RWMutex
}

// db - a wrapper on an actual store
type db struct {
	name      string
	file      *leveldb.DB
	mu        sync.RWMutex
	readOpts  *opt.ReadOptions
	writeOpts *opt.WriteOptions
}

var (
	systemDBs = []string{
		worldstate.UsersDBName,
		worldstate.DatabasesDBName,
		worldstate.ConfigDBName,
		worldstate.DefaultDBName,
	}
)

// Open opens a leveldb instance to maintain world state
func Open(dbRootDir string) (*LevelDB, error) {
	exist, err := fileops.Exists(dbRootDir)
	if err != nil {
		return nil, err
	}
	if !exist {
		return openNewLevelDBInstance(dbRootDir)
	}

	partialInstanceExist, err := isExistingLevelDBInstanceCreatedPartially(dbRootDir)
	if err != nil {
		return nil, err
	}

	switch {
	case partialInstanceExist:
		if err := fileops.RemoveAll(dbRootDir); err != nil {
			return nil, errors.Wrap(err, "error while removing the existing partially created levelDB instance")
		}

		return openNewLevelDBInstance(dbRootDir)
	default:
		return openExistingLevelDBInstance(dbRootDir)
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

func openNewLevelDBInstance(dbPath string) (*LevelDB, error) {
	if err := fileops.CreateDir(dbPath); err != nil {
		return nil, errors.WithMessagef(err, "failed to create director %s", dbPath)
	}

	underCreationFlagPath := filepath.Join(dbPath, underCreationFlag)
	if err := fileops.CreateFile(underCreationFlagPath); err != nil {
		return nil, err
	}

	l := &LevelDB{
		dbRootDir: dbPath,
		dbs:       make(map[string]*db),
	}

	for _, dbName := range systemDBs {
		if err := l.create(dbName); err != nil {
			return nil, err
		}
	}

	if err := fileops.Remove(underCreationFlagPath); err != nil {
		return nil, errors.WithMessagef(err, "error while removing the under creation flag [%s]", underCreationFlagPath)
	}

	return l, nil
}

func openExistingLevelDBInstance(dbPath string) (*LevelDB, error) {
	l := &LevelDB{
		dbRootDir: dbPath,
		dbs:       make(map[string]*db),
	}

	dbNames, err := fileops.ListSubdirs(dbPath)
	if err != nil {
		return nil, errors.WithMessagef(err, "failed to retrieve existing level dbs from %s", dbPath)
	}

	for _, dbName := range dbNames {
		file, err := leveldb.OpenFile(
			filepath.Join(l.dbRootDir, dbName),
			&opt.Options{ErrorIfMissing: false},
		)
		if err != nil {
			return nil, errors.WithMessagef(err, "failed to open leveldb file for database %s", dbName)
		}

		l.dbs[dbName] = &db{
			name:      dbName,
			file:      file,
			readOpts:  &opt.ReadOptions{},
			writeOpts: &opt.WriteOptions{Sync: true},
		}
	}

	return l, nil
}

// Close closes the database instance by closing all leveldb databases
func (l *LevelDB) Close() error {
	l.dbsList.Lock()
	defer l.dbsList.Unlock()

	for name, db := range l.dbs {
		db.mu.Lock()
		defer db.mu.Unlock()

		if err := db.file.Close(); err != nil {
			return fmt.Errorf("error while closing database %s, %v", name, err)
		}

		delete(l.dbs, db.name)
	}

	return nil
}
