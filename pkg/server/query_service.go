package server

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.ibm.com/blockchaindb/server/api"
	"github.ibm.com/blockchaindb/server/config"
	"github.ibm.com/blockchaindb/server/pkg/worldstate"
	"github.ibm.com/blockchaindb/server/pkg/worldstate/leveldb"
)

type queryServer struct {
	api.UnimplementedQueryServer
	db worldstate.DB
}

func newQueryServer() (*queryServer, error) {
	qs := &queryServer{}
	dbConf := config.Database()
	if dbConf.Name != "leveldb" {
		return nil, errors.New("only leveldb is supported as the state database")
	}
	var err error
	if qs.db, err = leveldb.NewLevelDB(dbConf.LedgerDirectory); err != nil {
		return nil, errors.WithMessagef(err, "failed to create a new leveldb instance for the peer")
	}
	return qs, nil
}

func (qs *queryServer) GetStatus(ctx context.Context, req *api.DB) (*api.DBStatus, error) {
	if err := validateDB(req); err != nil {
		return nil, err
	}
	status := &api.DBStatus{
		Exist: false,
	}
	if err := qs.db.Open(req.Name); err != nil {
		return status, nil
	}
	status.Exist = true
	return status, nil
}

func (qs *queryServer) GetState(ctx context.Context, req *api.DataQuery) (*api.Value, error) {
	if err := validateDataQuery(req); err != nil {
		return nil, err
	}
	return qs.db.Get(req.Header.DBName, req.Key)
}

func validateDataQuery(req *api.DataQuery) error {
	if req == nil {
		return fmt.Errorf("dataQuery request is nil")
	}
	if req.Header == nil {
		return fmt.Errorf("header in DataQuery is nil [%v]", req)
	}
	return nil
}

func validateDB(req *api.DB) error {
	if req == nil {
		return fmt.Errorf("db request is nil")
	}
	return nil
}
