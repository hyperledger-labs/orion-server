// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package bcdb

import (
	"context"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/hyperledger-labs/orion-server/config"
	"github.com/hyperledger-labs/orion-server/internal/blockstore"
	"github.com/hyperledger-labs/orion-server/internal/errors"
	ierrors "github.com/hyperledger-labs/orion-server/internal/errors"
	"github.com/hyperledger-labs/orion-server/internal/identity"
	"github.com/hyperledger-labs/orion-server/internal/queryexecutor"
	"github.com/hyperledger-labs/orion-server/internal/stateindex"
	"github.com/hyperledger-labs/orion-server/internal/worldstate"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/types"
)

type worldstateQueryProcessor struct {
	nodeID              string
	db                  worldstate.DB
	queryProcessingConf *config.QueryProcessingConf
	blockStore          *blockstore.Store
	identityQuerier     *identity.Querier
	logger              *logger.SugarLogger
}

type worldstateQueryProcessorConfig struct {
	nodeID              string
	db                  worldstate.DB
	queryProcessingConf *config.QueryProcessingConf
	blockStore          *blockstore.Store
	identityQuerier     *identity.Querier
	logger              *logger.SugarLogger
}

func newWorldstateQueryProcessor(conf *worldstateQueryProcessorConfig) *worldstateQueryProcessor {
	return &worldstateQueryProcessor{
		nodeID:              conf.nodeID,
		db:                  conf.db,
		queryProcessingConf: conf.queryProcessingConf,
		blockStore:          conf.blockStore,
		identityQuerier:     conf.identityQuerier,
		logger:              conf.logger,
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

// getDBIndex returns the index definition associated with the given database.
func (q *worldstateQueryProcessor) getDBIndex(dbName, querierUserID string) (*types.GetDBIndexResponse, error) {
	if worldstate.IsSystemDB(dbName) {
		return nil, &errors.PermissionErr{
			ErrMsg: "no index for the system database [" + dbName + "]",
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

	index, _, err := q.db.GetIndexDefinition(dbName)
	if err != nil {
		return nil, err
	}

	return &types.GetDBIndexResponse{
		Header: &types.ResponseHeader{},
		Index:  string(index),
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

// getDataRange return the state associated with a given key
func (q *worldstateQueryProcessor) getDataRange(dbName, querierUserID, startKey, endKey string, limit uint64) (*types.GetDataRangeResponse, error) {
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

	var kvs []*types.KVWithMetadata
	var resultCount uint64
	var size uint64
	var pendingResult bool
	var nextStartKey string

	itr, err := q.db.GetIterator(dbName, startKey, endKey)
	defer itr.Release()
	if err != nil {
		return nil, err
	}

	for itr.Next() {
		k := string(itr.Key())
		v := &types.ValueWithMetadata{}
		if err := proto.Unmarshal(itr.Value(), v); err != nil {
			return nil, err
		}

		acl := v.GetMetadata().GetAccessControl()
		if acl != nil {
			if !acl.ReadUsers[querierUserID] && !acl.ReadWriteUsers[querierUserID] {
				continue
			}
		}

		if limit > 0 {
			resultCount++
			if resultCount > limit {
				pendingResult = true
				nextStartKey = k
				break
			}
		}

		size += uint64(len(k) + len(itr.Value()))
		if size > q.queryProcessingConf.ResponseSizeLimitInBytes {
			pendingResult = true
			nextStartKey = k
			if len(kvs) != 0 {
				break
			}

			return nil, &errors.ServerRestrictionError{
				ErrMsg: fmt.Sprintf("response size limit for queries is configured as %d bytes but a single record size itself is %d bytes. Increase the query response size limit at the server", q.queryProcessingConf.ResponseSizeLimitInBytes, size),
			}
		}

		kvs = append(kvs, &types.KVWithMetadata{
			Key:      k,
			Value:    v.GetValue(),
			Metadata: v.GetMetadata(),
		})
	}

	return &types.GetDataRangeResponse{
		KVs:           kvs,
		PendingResult: pendingResult,
		NextStartKey:  nextStartKey,
	}, nil
}

func (q *worldstateQueryProcessor) getUser(querierUserID, targetUserID string) (*types.GetUserResponse, error) {
	user, metadata, err := q.identityQuerier.GetUser(targetUserID)
	if err != nil {
		if _, ok := err.(*identity.NotFoundErr); !ok {
			return nil, err
		} else {
			return &types.GetUserResponse{
				User:     nil,
				Metadata: nil,
			}, nil
		}
	}

	isAdmin, err := q.identityQuerier.HasAdministrationPrivilege(querierUserID)
	if err != nil {
		return nil, err
	}

	if !isAdmin {
		acl := metadata.GetAccessControl()
		if (acl != nil && !acl.ReadUsers[querierUserID]) || acl == nil {
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

func (q *worldstateQueryProcessor) getConfig(querierUserID string) (*types.GetConfigResponse, error) {
	// Limited access to admins only. Regular users can use the `GetNodeConfig` or `GetClusterStatus` APIs to discover
	// and fetch the details of nodes that are needed for external cluster access.
	isAdmin, err := q.identityQuerier.HasAdministrationPrivilege(querierUserID)
	if err != nil {
		return nil, err
	}
	if !isAdmin {
		return nil, &errors.PermissionErr{
			ErrMsg: "the user [" + querierUserID + "] has no permission to read a config object",
		}
	}

	config, metadata, err := q.db.GetConfig()
	if err != nil {
		return nil, err
	}

	return &types.GetConfigResponse{
		Config:   config,
		Metadata: metadata,
	}, nil
}

func (q *worldstateQueryProcessor) getNodeConfigAndMetadata() ([]*types.NodeConfig, *types.Metadata, error) {
	config, metadata, err := q.db.GetConfig()
	if err != nil {
		return nil, nil, err
	}

	return config.Nodes, metadata, nil
}

func (q *worldstateQueryProcessor) getNodeConfig(nodeID string) (*types.GetNodeConfigResponse, error) {
	nodeConfig, _, err := q.identityQuerier.GetNode(nodeID)
	if err != nil {
		if _, ok := err.(*identity.NotFoundErr); !ok {
			return nil, err
		}
	}

	c := &types.GetNodeConfigResponse{
		NodeConfig: nodeConfig,
	}

	return c, nil
}

func (q *worldstateQueryProcessor) getConfigBlock(querierUserID string, blockNumber uint64) (*types.GetConfigBlockResponse, error) {
	isAdmin, err := q.identityQuerier.HasAdministrationPrivilege(querierUserID)
	if err != nil {
		return nil, err
	}
	if !isAdmin {
		return nil, &errors.PermissionErr{
			ErrMsg: "the user [" + querierUserID + "] has no permission to read a config block",
		}
	}

	if blockNumber == 0 {
		_, metadata, err := q.db.GetConfig()
		if err != nil {
			return nil, err
		}
		blockNumber = metadata.GetVersion().GetBlockNum()
	}
	block, err := q.blockStore.Get(blockNumber)
	if err != nil {
		return nil, err
	}
	if isConfig := block.GetConfigTxEnvelope(); isConfig == nil {
		return nil, &ierrors.NotFoundErr{Message: fmt.Sprintf("block [%d] is not a config block", blockNumber)}
	}

	blockBytes, err := proto.Marshal(block)
	if err != nil {
		return nil, err
	}
	return &types.GetConfigBlockResponse{
		Block: blockBytes,
	}, nil
}

func (q *worldstateQueryProcessor) executeJSONQuery(ctx context.Context, dbName, querierUserID string, query []byte) (*types.DataQueryResponse, error) {
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

	snapshots, err := q.db.GetDBsSnapshot(
		[]string{
			worldstate.DatabasesDBName,
			dbName,
			stateindex.IndexDB(dbName),
		},
	)
	if err != nil {
		return nil, err
	}
	defer func() {
		snapshots.Release()
	}()

	jsonQueryExecutor := queryexecutor.NewWorldStateJSONQueryExecutor(snapshots, q.logger)
	keys, err := jsonQueryExecutor.ExecuteQuery(ctx, dbName, query)
	select {
	case <-ctx.Done():
		return nil, nil
	default:
		if err != nil {
			return nil, err
		}
	}

	var results []*types.KVWithMetadata

	for k := range keys {
		select {
		case <-ctx.Done():
			return nil, nil
		default:
			value, metadata, err := snapshots.Get(dbName, k)
			if err != nil {
				return nil, err
			}

			// TODO: we can store the ACL as value in the indexEntry. With that, we can avoid reading the whole value
			// to perform the access control - issue #152
			acl := metadata.GetAccessControl()
			if acl != nil {
				if !acl.ReadUsers[querierUserID] && !acl.ReadWriteUsers[querierUserID] {
					continue
				}
			}

			results = append(
				results,
				&types.KVWithMetadata{
					Key:      k,
					Value:    value,
					Metadata: metadata,
				},
			)
		}
	}

	return &types.DataQueryResponse{
		KVs: results,
	}, nil
}
