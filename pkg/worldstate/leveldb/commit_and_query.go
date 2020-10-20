package leveldb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/pkg/worldstate"
)

var (
	lastCommittedBlockNumberKey = []byte("lastCommittedBlockNumber")
)

// Exist returns true if the given database exist. Otherwise, it returns false.
func (l *LevelDB) Exist(dbName string) bool {
	l.dbsList.RLock()
	defer l.dbsList.RUnlock()

	_, ok := l.dbs[dbName]
	return ok
}

// ListDBs list all user databases
func (l *LevelDB) ListDBs() []string {
	l.dbsList.RLock()
	defer l.dbsList.RUnlock()

	dbsToExclude := make(map[string]struct{})
	for _, name := range preCreateDBs {
		dbsToExclude[name] = struct{}{}
	}

	var dbNames []string
	for name := range l.dbs {
		if _, ok := dbsToExclude[name]; ok {
			continue
		}
		dbNames = append(dbNames, name)
	}

	return dbNames
}

// Height returns the block height of the state database. In other words, it
// returns the last committed block number
func (l *LevelDB) Height() (uint64, error) {
	l.dbsList.RLock()
	defer l.dbsList.RUnlock()

	db, ok := l.dbs[worldstate.MetadataDBName]
	if !ok {
		return 0, errors.Errorf("unable to retrieve the state database height due to missing metadataDB")
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	blockNumberEnc, err := db.file.Get(lastCommittedBlockNumberKey, &opt.ReadOptions{})
	if err != nil && err != leveldb.ErrNotFound {
		return 0, errors.Wrap(err, "error while retrieving the state database height")
	}

	if err == leveldb.ErrNotFound {
		return 0, nil
	}

	blockNumberDec, err := binary.ReadUvarint(bytes.NewBuffer(blockNumberEnc))
	if err != nil {
		return 0, errors.Wrap(err, "error while decoding the stored height")
	}

	return blockNumberDec, nil
}

// Get returns the value of the key present in the database.
func (l *LevelDB) Get(dbName string, key string) ([]byte, *types.Metadata, error) {
	l.dbsList.RLock()
	defer l.dbsList.RUnlock()

	db, ok := l.dbs[dbName]
	if !ok {
		return nil, nil, &DBNotFoundErr{
			dbName: dbName,
		}
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	dbval, err := db.file.Get([]byte(key), db.readOpts)
	if err == leveldb.ErrNotFound {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, errors.WithMessagef(err, "failed to retrieve leveldb key [%s] from database %s", key, dbName)
	}

	persisted := &ValueAndMetadata{}
	if err := proto.Unmarshal(dbval, persisted); err != nil {
		return nil, nil, err
	}

	return persisted.Value, persisted.Metadata, nil
}

// GetVersion returns the version of the key present in the database
func (l *LevelDB) GetVersion(dbName string, key string) (*types.Version, error) {
	_, metadata, err := l.Get(dbName, key)
	if err != nil {
		return nil, err
	}

	return metadata.GetVersion(), nil
}

// GetACL returns the access control rule for the given key present in the database
func (l *LevelDB) GetACL(dbName, key string) (*types.AccessControl, error) {
	_, metadata, err := l.Get(dbName, key)
	if err != nil {
		return nil, err
	}

	return metadata.GetAccessControl(), nil
}

// Has returns true if the key exist in the database
func (l *LevelDB) Has(dbName, key string) (bool, error) {
	l.dbsList.RLock()
	db := l.dbs[dbName]
	l.dbsList.RUnlock()

	return db.file.Has([]byte(key), nil)
}

// GetConfig returns the cluster configuration
func (l *LevelDB) GetConfig() (*types.ClusterConfig, *types.Metadata, error) {
	configSerialized, metadata, err := l.Get(worldstate.ConfigDBName, worldstate.ConfigKey)
	if err != nil {
		return nil, nil, err
	}

	config := &types.ClusterConfig{}
	if err := proto.Unmarshal(configSerialized, config); err != nil {
		return nil, nil, errors.Wrap(err, "error while unmarshaling committed cluster configuration")
	}

	return config, metadata, nil
}

// Commit commits the updates to the database
func (l *LevelDB) Commit(dbsUpdates []*worldstate.DBUpdates, blockNumber uint64) error {
	for _, updates := range dbsUpdates {
		l.dbsList.RLock()
		db := l.dbs[updates.DBName]
		l.dbsList.RUnlock()

		if db == nil {
			return errors.Errorf("database %s does not exist", updates.DBName)
		}

		if err := l.commitToDB(db, updates); err != nil {
			return err
		}
	}

	db, _ := l.dbs[worldstate.MetadataDBName]
	db.mu.Lock()
	defer db.mu.Unlock()

	b := make([]byte, binary.MaxVarintLen64)
	binary.PutUvarint(b, blockNumber)
	if err := db.file.Put(lastCommittedBlockNumberKey, b, &opt.WriteOptions{}); err != nil {
		return errors.Wrapf(err, "error while storing the last committed block number [%d] to the metadataDB", blockNumber)
	}

	return nil
}

func (l *LevelDB) commitToDB(db *db, updates *worldstate.DBUpdates) error {
	batch := &leveldb.Batch{}

	for _, kv := range updates.Writes {
		dbval, err := proto.Marshal(
			&ValueAndMetadata{
				Value:    kv.Value,
				Metadata: kv.Metadata,
			},
		)
		if err != nil {
			return errors.WithMessagef(err, "failed to marshal the constructed dbValue [%v]", kv.Value)
		}

		batch.Put([]byte(kv.Key), dbval)
	}

	for _, key := range updates.Deletes {
		batch.Delete([]byte(key))
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.file.Write(batch, db.writeOpts); err != nil {
		return errors.Wrapf(err, "error while writing an update batch to database [%s]", db.name)
	}

	if updates.DBName != worldstate.DatabasesDBName {
		return nil
	}

	// if node fails during the creation or deletion of
	// databases, during the recovery, these operations
	// will be repeated again. Given that create() and
	// delete() are a no-op when the db exist and not-exist,
	// respectively, we don't need anything special to
	// handle failures

	// we also assume the union of dbNames in create
	// and delete list to be unique which is to be ensured
	// by the validator.

	for _, kv := range updates.Writes {
		dbName := kv.Key
		if err := l.create(dbName); err != nil {
			return err
		}
	}

	for _, dbName := range updates.Deletes {
		if err := l.delete(dbName); err != nil {
			return err
		}
	}

	return nil
}

// create creates a database. It does not return an error when the database already exist.
func (l *LevelDB) create(dbName string) error {
	l.dbsList.Lock()
	defer l.dbsList.Unlock()

	if _, ok := l.dbs[dbName]; ok {
		return nil
	}

	file, err := leveldb.OpenFile(filepath.Join(l.dbRootDir, dbName), &opt.Options{})
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

// delete deletes a database. It does not return an error when the database does not exist.
// delete would be called only by the Commit() when processing delete entries associated with
// the _db
func (l *LevelDB) delete(dbName string) error {
	l.dbsList.Lock()
	defer l.dbsList.Unlock()

	db, ok := l.dbs[dbName]
	if !ok {
		return nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.file.Close(); err != nil {
		return errors.Wrapf(err, "error while closing the database [%s] before delete", dbName)
	}

	delete(l.dbs, dbName)

	if err := os.RemoveAll(filepath.Join(l.dbRootDir, dbName)); err != nil {
		return errors.Wrapf(err, "error while deleting database [%s]", dbName)
	}

	return nil
}

// DBNotFoundErr denotes that the given dbName is not present in the database
type DBNotFoundErr struct {
	dbName string
}

func (e *DBNotFoundErr) Error() string {
	return fmt.Sprintf("database %s does not exist", e.dbName)
}
