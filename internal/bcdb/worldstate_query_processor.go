package bcdb

import (
	"strings"

	"github.ibm.com/blockchaindb/server/internal/blockstore"
	"github.ibm.com/blockchaindb/server/internal/errors"
	"github.ibm.com/blockchaindb/server/internal/identity"
	"github.ibm.com/blockchaindb/server/internal/worldstate"
	"github.ibm.com/blockchaindb/server/pkg/logger"
	"github.ibm.com/blockchaindb/server/pkg/types"
)

type worldstateQueryProcessor struct {
	nodeID          string
	db              worldstate.DB
	blockStore      *blockstore.Store
	identityQuerier *identity.Querier
	logger          *logger.SugarLogger
}

type worldstateQueryProcessorConfig struct {
	nodeID          string
	db              worldstate.DB
	blockStore      *blockstore.Store
	identityQuerier *identity.Querier
	logger          *logger.SugarLogger
}

func newWorldstateQueryProcessor(conf *worldstateQueryProcessorConfig) *worldstateQueryProcessor {
	return &worldstateQueryProcessor{
		nodeID:          conf.nodeID,
		db:              conf.db,
		blockStore:      conf.blockStore,
		identityQuerier: conf.identityQuerier,
		logger:          conf.logger,
	}
}

func (q *worldstateQueryProcessor) isDBExists(name string) bool {
	return q.db.Exist(name)
}

// getDBStatus returns the status about a database, i.e., whether a database exist or not
func (q *worldstateQueryProcessor) getDBStatus(dbName string) (*types.GetDBStatusResponse, error) {
	// ACL is meaningless here as this call is to check whether a DB exist. Even with ACL,
	// the user can infer the information.
	return &types.GetDBStatusResponse{
		Exist: q.isDBExists(dbName),
	}, nil
}

// getState return the state associated with a given key
func (q *worldstateQueryProcessor) getData(dbName, querierUserID, key string) (*types.GetDataResponse, error) {
	if worldstate.IsSystemDB(dbName) {
		return nil, &errors.PermissionErr{
			ErrMsg: "no user can directly read from a system database [" + dbName + "]. " +
				"To read from a system database, use /config, /user, /db rest endpoints instead of /data",
		}
	}

	hasPerm, err := q.identityQuerier.HasReadAccessOnDataDB(querierUserID, dbName)
	if err != nil {
		return nil, err
	}
	if !hasPerm {
		return nil, &errors.PermissionErr{
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
			return nil, &errors.PermissionErr{
				ErrMsg: "the user [" + querierUserID + "] has no permission to read key [" + key + "] from database [" + dbName + "]",
			}
		}
	}

	return &types.GetDataResponse{
		Value:    value,
		Metadata: metadata,
	}, nil
}

func (q *worldstateQueryProcessor) getUser(querierUserID, targetUserID string) (*types.GetUserResponse, error) {
	user, metadata, err := q.identityQuerier.GetUser(targetUserID)
	if err != nil {
		if _, ok := err.(*identity.UserNotFoundErr); !ok {
			return nil, err
		}
	}

	acl := metadata.GetAccessControl()
	if acl != nil {
		if !acl.ReadUsers[querierUserID] && !acl.ReadWriteUsers[querierUserID] {
			return nil, &errors.PermissionErr{
				ErrMsg: "the user [" + querierUserID + "] has no permission to read info of user [" + targetUserID + "]",
			}
		}
	}

	return &types.GetUserResponse{
		User:     user,
		Metadata: metadata,
	}, nil
}

func (q *worldstateQueryProcessor) getConfig() (*types.GetConfigResponse, error) {
	// ACL may not be needed for the read as it would be useful to fetch IPs of
	// all nodes even without cluster admin privilege. We can add it later if needed
	config, metadata, err := q.db.GetConfig()
	if err != nil {
		return nil, err
	}

	return &types.GetConfigResponse{
		Config:   config,
		Metadata: metadata,
	}, nil
}

func (q *worldstateQueryProcessor) getNodeConfig(nodeID string) (*types.GetNodeConfigResponse, error) {
	config, _, err := q.db.GetConfig()
	if err != nil {
		return nil, err
	}

	c := &types.GetNodeConfigResponse{}

	for _, node := range config.Nodes {
		if strings.Compare(node.ID, nodeID) == 0 {
			c.NodeConfig = node
		}
	}

	return c, nil
}
