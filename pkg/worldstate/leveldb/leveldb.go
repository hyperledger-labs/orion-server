package leveldb

import (
	"fmt"
	"path/filepath"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.ibm.com/blockchaindb/server/api"
	"github.ibm.com/blockchaindb/server/pkg/worldstate"
)

// LevelDB holds information about all created database
type LevelDB struct {
	dirPath string
	dbs     map[string]*db
	mu      sync.RWMutex
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
	systemDBs = []string{worldstate.UsersDBName, "test"}
)

// NewLevelDB creates a new leveldb instance
func NewLevelDB(dirPath string) (*LevelDB, error) {
	l := &LevelDB{
		dirPath: dirPath,
		dbs:     make(map[string]*db),
	}

	exists, err := fileExists(dirPath)
	if err != nil {
		return nil, err
	}
	if !exists {
		if err := createDir(dirPath); err != nil {
			return nil, errors.WithMessagef(err, "failed to create director %s", dirPath)
		}
		if err := l.createDBsIfNotExist(systemDBs); err != nil {
			return nil, err
		}
		return l, nil
	}

	dbNames, err := listSubdirs(dirPath)
	if err != nil {
		return nil, errors.WithMessagef(err, "failed to retrieve existing level dbs from %s", dirPath)
	}
	for _, dbName := range dbNames {
		file, err := leveldb.OpenFile(
			filepath.Join(l.dirPath, dbName),
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

	if err := l.createDBsIfNotExist(systemDBs); err != nil {
		return nil, err
	}
	return l, nil
}

func (l *LevelDB) createDBsIfNotExist(dbNames []string) error {
	for _, dbName := range dbNames {
		if db, _ := l.getDB(dbName); db == nil {
			if err := l.Create(dbName); err != nil {
				return err
			}
		}
	}
	return nil
}

// Create creates a new database. It returns an error if database already exists.
func (l *LevelDB) Create(dbName string) error {
	if db, _ := l.getDB(dbName); db != nil {
		return fmt.Errorf("database %s already exists", dbName)
	}

	file, err := leveldb.OpenFile(filepath.Join(l.dirPath, dbName), &opt.Options{})
	if err != nil {
		return errors.WithMessagef(err, "failed to open leveldb file for database %s", dbName)
	}

	l.dbs[dbName] = &db{
		name:      dbName,
		file:      file,
		readOpts:  &opt.ReadOptions{},
		writeOpts: &opt.WriteOptions{Sync: true},
	}

	return nil
}

// Open opens an existing database. It returns an error if the database does not exist.
func (l *LevelDB) Open(dbName string) error {
	_, err := l.getDB(dbName)
	return err
}

// Get returns the value of the key present in the database.
func (l *LevelDB) Get(dbName string, key string) (*api.Value, error) {
	db, err := l.getDB(dbName)
	if err != nil {
		return nil, err
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	dbval, err := db.file.Get([]byte(key), db.readOpts)
	if err == leveldb.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, errors.WithMessagef(err, "failed to retrieve leveldb key [%s] from database %s", key, dbName)
	}

	value := &api.Value{}
	if err := proto.Unmarshal(dbval, value); err != nil {
		return nil, err
	}

	return value, nil
}

// GetVersion returns the version of the key present in the database
func (l *LevelDB) GetVersion(dbName string, key string) (*api.Version, error) {
	dbval, err := l.Get(dbName, key)
	if err != nil {
		return nil, err
	}
	if dbval == nil {
		return nil, nil
	}

	return dbval.Metadata.Version, nil
}

// Commit commits the updates to the database
func (l *LevelDB) Commit(dbsUpdates []*worldstate.DBUpdates) error {
	for _, updates := range dbsUpdates {
		db, err := l.getDB(updates.DBName)
		if err != nil {
			return err
		}

		batch := &leveldb.Batch{}

		db.mu.Lock()
		for _, kv := range updates.Writes {
			dbval, err := proto.Marshal(kv.Value)
			if err != nil {
				return errors.WithMessagef(err, "failed to marshal the constructed dbValue [%v]", kv.Value)
			}
			batch.Put([]byte(kv.Key), dbval)
		}
		for _, key := range updates.Deletes {
			batch.Delete([]byte(key))
		}
		if err := db.file.Write(batch, db.writeOpts); err != nil {
			return err
		}
		db.mu.Unlock()
	}

	return nil
}

func (l *LevelDB) getDB(dbName string) (*db, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	db, ok := l.dbs[dbName]
	if !ok {
		return nil, &DBNotFoundErr{dbName: dbName}
	}
	return db, nil
}

// DBNotFoundErr denotes that the given dbName is not present in the database
type DBNotFoundErr struct {
	dbName string
}

func (e *DBNotFoundErr) Error() string {
	return fmt.Sprintf("database %s does not exist", e.dbName)
}
