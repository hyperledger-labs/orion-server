package leveldb

import (
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger-labs/orion-server/internal/worldstate"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type Snapshots struct {
	dbSnap map[string]*leveldb.Snapshot
	sync.RWMutex
}

func (l *LevelDB) GetDBsSnapshot(dbNames []string) (worldstate.DBsSnapshot, error) {
	snap := &Snapshots{
		dbSnap: make(map[string]*leveldb.Snapshot),
	}

	for _, dbName := range dbNames {
		db, ok := l.getDB(dbName)
		if !ok {
			return nil, &DBNotFoundErr{
				dbName: dbName,
			}
		}

		// We avoid using the existing snapshots as they might be released before the query will finish
		s, err := db.file.GetSnapshot()
		if err != nil {
			return nil, err
		}

		snap.dbSnap[dbName] = s
	}

	return snap, nil
}

func (s *Snapshots) Get(dbName, key string) ([]byte, *types.Metadata, error) {
	s.RLock()
	defer s.RUnlock()

	lSnap, ok := s.dbSnap[dbName]
	if !ok {
		return nil, nil, errors.New(dbName + " is needed to fetch the index definiton and is not snapshotted")
	}

	dbval, err := lSnap.Get([]byte(key), &opt.ReadOptions{})
	if err == leveldb.ErrNotFound {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, errors.WithMessagef(err, "failed to retrieve leveldb key [%s] from the snapshot of database [%s]", key, dbName)
	}

	persisted := &types.ValueWithMetadata{}
	if err := proto.Unmarshal(dbval, persisted); err != nil {
		return nil, nil, err
	}

	return persisted.Value, persisted.Metadata, nil
}

func (s *Snapshots) GetIndexDefinition(dbName string) ([]byte, *types.Metadata, error) {
	return s.Get(worldstate.DatabasesDBName, dbName)
}

func (s *Snapshots) GetIterator(dbName string, startKey, endKey string) (worldstate.Iterator, error) {
	s.RLock()
	defer s.RUnlock()

	lSnap, ok := s.dbSnap[dbName]
	if !ok {
		return nil, errors.New(dbName + " database is not snapshotted")
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

	return lSnap.NewIterator(r, &opt.ReadOptions{}), nil
}

func (s *Snapshots) Release() {
	s.Lock()
	defer s.Unlock()

	for _, lSnap := range s.dbSnap {
		lSnap.Release()
	}

	s.dbSnap = nil
}
