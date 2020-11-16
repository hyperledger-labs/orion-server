package backend

import (
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/pkg/blockstore"
	"github.ibm.com/blockchaindb/server/pkg/common/logger"
	"github.ibm.com/blockchaindb/server/pkg/cryptoservice"
	"github.ibm.com/blockchaindb/server/pkg/identity"
	"github.ibm.com/blockchaindb/server/pkg/worldstate"
)

type queryProcessor struct {
	nodeID          string
	db              worldstate.DB
	blockStore      *blockstore.Store
	identityQuerier *identity.Querier
	logger          *logger.SugarLogger
}

type queryProcessorConfig struct {
	nodeID          string
	db              worldstate.DB
	blockStore      *blockstore.Store
	identityQuerier *identity.Querier
	logger          *logger.SugarLogger
}

func newQueryProcessor(conf *queryProcessorConfig) *queryProcessor {
	return &queryProcessor{
		nodeID:          conf.nodeID,
		db:              conf.db,
		blockStore:      conf.blockStore,
		identityQuerier: conf.identityQuerier,
		logger:          conf.logger,
	}
}

func (q *queryProcessor) isDBExists(name string) bool {
	return q.db.Exist(name)
}

// getDBStatus returns the status about a database, i.e., whether a database exist or not
func (q *queryProcessor) getDBStatus(dbName string) (*types.GetDBStatusResponseEnvelope, error) {
	// ACL is meaningless here as this call is to check whether a DB exist. Even with ACL,
	// the user can infer the information.
	status := &types.GetDBStatusResponseEnvelope{
		Payload: &types.GetDBStatusResponse{
			Header: &types.ResponseHeader{
				NodeID: q.nodeID,
			},
			Exist: q.isDBExists(dbName),
		},
		Signature: nil,
	}

	var err error
	if status.Signature, err = cryptoservice.Sign(status.Payload); err != nil {
		return nil, err
	}
	return status, nil
}

// getState return the state associated with a given key
func (q *queryProcessor) getData(dbName, querierUserID, key string) (*types.GetDataResponseEnvelope, error) {
	if worldstate.IsSystemDB(dbName) {
		return nil, &PermissionErr{
			ErrMsg: "no user can directly read from a system database [" + dbName + "]. " +
				"To read from a system database, use /config, /user, /db rest endpoints instead of /data",
		}
	}

	hasPerm, err := q.identityQuerier.HasReadAccessOnDataDB(querierUserID, dbName)
	if err != nil {
		return nil, err
	}
	if !hasPerm {
		return nil, &PermissionErr{
			ErrMsg: "the user [" + querierUserID + "] has no permission to read from database [" + dbName + "]",
		}
	}

	value, metadata, err := q.db.Get(dbName, key)
	if err != nil {
		return nil, err
	}

	acl := metadata.GetAccessControl()
	if acl != nil {
		if !acl.ReadUsers[querierUserID] && !acl.ReadWriteUsers[querierUserID] {
			return nil, &PermissionErr{
				ErrMsg: "the user [" + querierUserID + "] has no permission to read key [" + key + "] from database [" + dbName + "]",
			}
		}
	}

	state := &types.GetDataResponseEnvelope{
		Payload: &types.GetDataResponse{
			Header: &types.ResponseHeader{
				NodeID: q.nodeID,
			},
			Value:    value,
			Metadata: metadata,
		},
		Signature: nil,
	}

	if state.Signature, err = cryptoservice.Sign(state.Payload); err != nil {
		return nil, err
	}

	return state, nil
}

func (q *queryProcessor) getUser(querierUserID, targetUserID string) (*types.GetUserResponseEnvelope, error) {
	user, metadata, err := q.identityQuerier.GetUser(targetUserID)
	if err != nil {
		if _, ok := err.(*identity.UserNotFoundErr); !ok {
			return nil, err
		}
	}

	acl := metadata.GetAccessControl()
	if acl != nil {
		if !acl.ReadUsers[querierUserID] && !acl.ReadWriteUsers[querierUserID] {
			return nil, &PermissionErr{
				ErrMsg: "the user [" + querierUserID + "] has no permission to read info of user [" + targetUserID + "]",
			}
		}
	}

	u := &types.GetUserResponseEnvelope{
		Payload: &types.GetUserResponse{
			Header: &types.ResponseHeader{
				NodeID: q.nodeID,
			},
			User:     user,
			Metadata: metadata,
		},
		Signature: nil,
	}

	if u.Signature, err = cryptoservice.Sign(u.Payload); err != nil {
		return nil, err
	}

	return u, nil
}

func (q *queryProcessor) getConfig() (*types.GetConfigResponseEnvelope, error) {
	// ACL may not be needed for the read as it would be useful to fetch IPs of
	// all nodes even without cluster admin privilege. We can add it later if needed
	config, metadata, err := q.db.GetConfig()
	if err != nil {
		return nil, err
	}

	c := &types.GetConfigResponseEnvelope{
		Payload: &types.GetConfigResponse{
			Header: &types.ResponseHeader{
				NodeID: q.nodeID,
			},
			Config:   config,
			Metadata: metadata,
		},
		Signature: nil,
	}

	if c.Signature, err = cryptoservice.Sign(c.Payload); err != nil {
		return nil, err
	}

	return c, nil
}

func (q *queryProcessor) close() error {
	if err := q.db.Close(); err != nil {
		return err
	}

	return q.blockStore.Close()
}

type PermissionErr struct {
	ErrMsg string
}

func (e *PermissionErr) Error() string {
	return e.ErrMsg
}
