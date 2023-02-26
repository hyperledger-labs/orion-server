// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package leveldb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger-labs/orion-server/internal/worldstate"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

var (
	lastCommittedBlockNumberKey = []byte("lastCommittedBlockNumber")
)

// Exist returns true if the given database exist. Otherwise, it returns false.
func (l *LevelDB) Exist(dbName string) bool {
	_, ok := l.getDB(dbName)
	return ok
}

// ListDBs list all user databases. Used only for testing.
func (l *LevelDB) ListDBs() []string {
	dbsToExclude := make(map[string]struct{})
	for _, name := range preCreateDBs {
		dbsToExclude[name] = struct{}{}
	}

	var dbNames []string
	l.dbs.Range(func(key, value interface{}) bool {
		name := key.(string)
		if _, ok := dbsToExclude[name]; !ok {
			dbNames = append(dbNames, name)
		}
		return true
	})

	return dbNames
}

// Height returns the block height of the state database. In other words, it
// returns the last committed block number
func (l *LevelDB) Height() (uint64, error) {
	db, ok := l.getDB(worldstate.MetadataDBName)
	if !ok || db == nil {
		return 0, errors.Errorf("unable to retrieve the state database height due to missing metadataDB")
	}

	blockNumberEnc, err := db.file.Get(lastCommittedBlockNumberKey, db.readOpts)
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

func convertClosedErr(err error, dbName string) error {
	if err == leveldb.ErrClosed || err == leveldb.ErrSnapshotReleased {
		return &DBNotFoundErr{dbName: dbName}
	}
	return err
}

// Get returns the value of the key present in the database.
func (l *LevelDB) Get(dbName string, key string) ([]byte, *types.Metadata, error) {
	db, ok := l.getDB(dbName)
	if !ok || db == nil {
		return nil, nil, &DBNotFoundErr{dbName: dbName}
	}

	dbVal, inCache := l.cache.getState(dbName, key)
	if !inCache {
		var err error
		dbVal, err = db.file.Get([]byte(key), db.readOpts)
		switch err {
		case leveldb.ErrNotFound:
			if err = l.cache.putState(dbName, key, nil); err != nil {
				return nil, nil, err
			}
			return nil, nil, nil
		case leveldb.ErrClosed, leveldb.ErrSnapshotReleased:
			return nil, nil, &DBNotFoundErr{dbName: dbName}
		case nil:
			// ignore
		default: // != nil
			return nil, nil, errors.WithMessagef(err, "failed to retrieve leveldb key [%s] from database %s", key, dbName)
		}

		if err = l.cache.putState(dbName, key, dbVal); err != nil {
			return nil, nil, err
		}
	}

	value := &types.ValueWithMetadata{}
	if err := proto.Unmarshal(dbVal, value); err != nil {
		return nil, nil, err
	}

	return value.Value, value.Metadata, nil
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
	db, ok := l.getDB(dbName)
	if !ok || db == nil {
		return false, &DBNotFoundErr{dbName: dbName}
	}
	has, err := db.file.Has([]byte(key), db.readOpts)
	return has, convertClosedErr(err, dbName)
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

// GetIndexDefinition returns the index definition of a given database
func (l *LevelDB) GetIndexDefinition(dbName string) ([]byte, *types.Metadata, error) {
	return l.Get(worldstate.DatabasesDBName, dbName)
}

// GetIterator returns an iterator to fetch values associated with a range of keys
// startKey is inclusive while the endKey is exclusive. An empty startKey (i.e., "") denotes that
// the caller wants from the first key in the database (lexicographic order). An empty
// endKey (i.e., "") denotes that the caller wants till the last key in the database (lexicographic order).
func (l *LevelDB) GetIterator(dbName string, startKey, endKey string) (worldstate.Iterator, error) {
	db, ok := l.getDB(dbName)
	if !ok || db == nil {
		return nil, &DBNotFoundErr{dbName: dbName}
	}

	r := &util.Range{}
	if startKey == "" {
		r.Start = nil
	} else {
		r.Start = []byte(startKey)
	}

	if endKey == "" {
		r.Limit = nil
	} else {
		r.Limit = []byte(endKey)
	}

	it := db.file.NewIterator(r, &opt.ReadOptions{})

	// Iterator contains errors internally, but we want to fail early if there is an issue with the DB.
	if err := convertClosedErr(it.Error(), dbName); err != nil {
		return nil, err
	} else {
		return it, nil
	}
}

// Commit commits the updates to the database
func (l *LevelDB) Commit(dbsUpdates map[string]*worldstate.DBUpdates, blockNumber uint64) error {
	for dbName, updates := range dbsUpdates {
		db, ok := l.getDB(dbName)
		if !ok || db == nil {
			l.logger.Errorf("database %s does not exist", dbName)
			return errors.Errorf("database %s does not exist", dbName)
		}

		start := time.Now()
		if err := l.commitToDB(db, updates); err != nil {
			return err
		}
		l.logger.Debugf("changes committed to the database %s, took %d ms, available dbs are [%s]", dbName, time.Since(start).Milliseconds(), "")
	}

	db, ok := l.getDB(worldstate.MetadataDBName)
	if !ok {
		l.logger.Errorf("metadata database does not exist, available dbs are [%+v]", "")
		return errors.Errorf("metadata database does not exist")
	}

	b := make([]byte, binary.MaxVarintLen64)
	binary.PutUvarint(b, blockNumber)
	if err := db.file.Put(lastCommittedBlockNumberKey, b, db.writeOpts); err != nil {
		return errors.Wrapf(err, "error while storing the last committed block number [%d] to the metadataDB", blockNumber)
	}

	return nil
}

func (l *LevelDB) commitToDB(db *Db, updates *worldstate.DBUpdates) error {
	batch := &leveldb.Batch{}

	for _, kv := range updates.Writes {
		dbVal, err := proto.Marshal(
			&types.ValueWithMetadata{
				Value:    kv.Value,
				Metadata: kv.Metadata,
			},
		)
		if err != nil {
			return errors.WithMessagef(err, "failed to marshal the constructed dbValue [%v]", kv.Value)
		}

		batch.Put([]byte(kv.Key), dbVal)
		l.cache.putStateIfExist(db.name, kv.Key, dbVal)
	}

	for _, key := range updates.Deletes {
		batch.Delete([]byte(key))
		l.cache.delState(db.name, key)
	}

	if err := db.file.Write(batch, db.writeOpts); err != nil {
		return errors.Wrapf(err, "error while writing an update batch to database [%s]", db.name)
	}

	if db.name != worldstate.DatabasesDBName {
		return nil
	}

	// if node fails during the creation or deletion of
	// databases, during the recovery, these operations
	// will be repeated. Given that create() and
	// delete() are a no-op when the db exist and not-exist,
	// respectively, we don't need anything special to
	// handle failures

	// we also assume the union of dbNames in create
	// and delete list to be unique which is to be ensured
	// by the validator.

	for _, writeKV := range updates.Writes {
		if err := l.create(writeKV.Key); err != nil {
			return err
		}
	}

	for _, delDbName := range updates.Deletes {
		if err := l.delete(delDbName); err != nil {
			return err
		}
	}

	return nil
}

// create creates a database. It does not return an error when the database already exist.
func (l *LevelDB) create(dbName string) error {
	// The map is only updated in the methods create and delete, which are called on instance creation or commit.
	// Both these operations are single threaded. So it is safe to check if db-name exists in the map before inserting.
	// Note that this check is mandatory because trying to open an already opened DB will result in error.
	if _, ok := l.getDB(dbName); ok {
		l.logger.Debugf("Skipping %s cause database already exists", dbName)
		return nil
	}

	// By default, this won't issue an error if the DB is missing nor if it exists
	file, err := leveldb.OpenFile(filepath.Join(l.dbRootDir, dbName), &opt.Options{})
	if err != nil {
		return errors.WithMessagef(err, "failed to open leveldb file for database %s", dbName)
	}

	// We assume no concurrent updates to the map due to the above, so we can use a blind store.
	db := &Db{
		name:      dbName,
		file:      file,
		readOpts:  &opt.ReadOptions{},
		writeOpts: &opt.WriteOptions{Sync: true},
	}

	l.setDB(dbName, db)

	return nil
}

// delete deletes a database. It does not return an error when the database does not exist.
// delete would be called only by the Commit() when processing delete entries associated with
// the _db
func (l *LevelDB) delete(dbName string) error {
	db, loaded := l.getAndDelDB(dbName)
	if !loaded {
		return nil
	}

	if err := db.file.Close(); err != nil {
		return errors.Wrapf(err, "error while closing the database [%s] before delete", dbName)
	}

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
