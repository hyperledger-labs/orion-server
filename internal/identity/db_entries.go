// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package identity

import (
	"strings"

	"github.com/hyperledger-labs/orion-server/internal/provenance"
	"github.com/hyperledger-labs/orion-server/internal/worldstate"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
)

var (
	// UserNamespace holds the user identity information in the user db
	UserNamespace = []byte{0}
	// NodeNamespace holds the node identity information in the config db
	NodeNamespace = []byte{0}
)

// ConstructDBEntriesForUserAdminTx constructs database entries for the transaction that manipulates
// user information
func ConstructDBEntriesForUserAdminTx(tx *types.UserAdministrationTx, version *types.Version) (*worldstate.DBUpdates, error) {
	var userWrites []*worldstate.KVWithMetadata
	var userDeletes []string

	for _, w := range tx.UserWrites {
		userSerialized, err := proto.Marshal(w.User)
		if err != nil {
			return nil, errors.Wrap(err, "error while marshaling user")
		}

		kv := &worldstate.KVWithMetadata{
			Key:   string(UserNamespace) + w.User.Id,
			Value: userSerialized,
			Metadata: &types.Metadata{
				Version:       version,
				AccessControl: w.Acl,
			},
		}
		userWrites = append(userWrites, kv)
	}

	for _, d := range tx.UserDeletes {
		userDeletes = append(userDeletes, string(UserNamespace)+d.UserId)
	}

	return &worldstate.DBUpdates{
		Writes:  userWrites,
		Deletes: userDeletes,
	}, nil
}

// ConstructProvenanceEntriesForUserAdminTx constructs provenance entries for the transaction that manipulates
func ConstructProvenanceEntriesForUserAdminTx(
	tx *types.UserAdministrationTx,
	version *types.Version,
	db worldstate.DB,
) (*provenance.TxDataForProvenance, error) {
	identityQuerier := NewQuerier(db)
	txData := &provenance.TxDataForProvenance{
		IsValid:            true,
		DBName:             worldstate.UsersDBName,
		UserID:             tx.UserId,
		TxID:               tx.TxId,
		Deletes:            make(map[string]*types.Version),
		OldVersionOfWrites: make(map[string]*types.Version),
	}

	for _, read := range tx.UserReads {
		k := &provenance.KeyWithVersion{
			Key:     read.UserId,
			Version: read.Version,
		}
		txData.Reads = append(txData.Reads, k)
	}

	for _, write := range tx.UserWrites {
		userSerialized, err := proto.Marshal(write.User)
		if err != nil {
			return nil, errors.Wrap(err, "error while marshaling user")
		}

		kv := &types.KVWithMetadata{
			Key:   write.User.Id,
			Value: userSerialized,
			Metadata: &types.Metadata{
				Version:       version,
				AccessControl: write.Acl,
			},
		}
		txData.Writes = append(txData.Writes, kv)

		v, err := identityQuerier.GetUserVersion(write.User.Id)
		if err != nil {
			if _, ok := err.(*NotFoundErr); ok {
				continue
			}

			return nil, err
		}

		txData.OldVersionOfWrites[write.User.Id] = v
	}

	for _, d := range tx.UserDeletes {
		v, err := identityQuerier.GetUserVersion(d.UserId)
		if err != nil {
			return nil, err
		}

		// for a delete to be valid, the value must exist and hence, the version will
		// never be nil
		txData.Deletes[d.UserId] = v
	}

	return txData, nil
}

// ConstructDBEntriesForClusterAdmins constructs database entries for the cluster admins
func ConstructDBEntriesForClusterAdmins(oldAdmins, newAdmins []*types.Admin, version *types.Version) (*worldstate.DBUpdates, error) {
	var kvWrites []*worldstate.KVWithMetadata
	var deletes []string

	newAdms := make(map[string]*types.Admin)
	for _, newAdm := range newAdmins {
		newAdms[newAdm.Id] = newAdm
	}

	for _, oldAdm := range oldAdmins {
		if _, ok := newAdms[oldAdm.Id]; ok {
			if proto.Equal(oldAdm, newAdms[oldAdm.Id]) {
				delete(newAdms, oldAdm.Id)
			}
			continue
		}

		deletes = append(deletes, string(UserNamespace)+oldAdm.Id)
	}

	for _, admin := range newAdms {
		u := &types.User{
			Id:          admin.Id,
			Certificate: admin.Certificate,
			Privilege: &types.Privilege{
				Admin: true,
			},
		}

		value, err := proto.Marshal(u)
		if err != nil {
			return nil, errors.New("error marshaling admin user")
		}

		kvWrites = append(
			kvWrites,
			&worldstate.KVWithMetadata{
				Key:   string(UserNamespace) + admin.Id,
				Value: value,
				Metadata: &types.Metadata{
					Version: version,
				},
			},
		)
	}

	if len(kvWrites) == 0 && len(deletes) == 0 {
		return nil, nil
	}

	return &worldstate.DBUpdates{
		Writes:  kvWrites,
		Deletes: deletes,
	}, nil
}

// ConstructProvenanceEntriesForClusterAdmins constructs provenance entries for the transaction that manipulates
// cluster admins
func ConstructProvenanceEntriesForClusterAdmins(
	userID, txID string,
	adminUpdates *worldstate.DBUpdates,
	db worldstate.DB,
) (*provenance.TxDataForProvenance, error) {
	identityQuerier := NewQuerier(db)
	txData := &provenance.TxDataForProvenance{
		IsValid:            true,
		DBName:             worldstate.UsersDBName,
		UserID:             userID,
		TxID:               txID,
		Deletes:            make(map[string]*types.Version),
		OldVersionOfWrites: make(map[string]*types.Version),
	}

	if adminUpdates == nil {
		return txData, nil
	}

	for _, w := range adminUpdates.Writes {
		adminID := getUserIDFromCompositeUserKey(w.Key)
		txData.Writes = append(
			txData.Writes,
			&types.KVWithMetadata{
				Key:      adminID,
				Value:    w.Value,
				Metadata: w.Metadata,
			},
		)

		version, err := identityQuerier.GetUserVersion(adminID)
		if err != nil {
			if _, ok := err.(*NotFoundErr); ok {
				continue
			}

			return nil, err
		}
		txData.OldVersionOfWrites[adminID] = version
	}

	for _, d := range adminUpdates.Deletes {
		adminID := getUserIDFromCompositeUserKey(d)
		version, err := identityQuerier.GetUserVersion(adminID)
		if err != nil {
			// admin to be deleted must exist
			return nil, err
		}
		txData.Deletes[adminID] = version
	}

	return txData, nil
}

// ConstructDBEntriesForNodes constructs database entries for the nodes present in the clusterr
func ConstructDBEntriesForNodes(oldNodes, newNodes []*types.NodeConfig, version *types.Version) (*worldstate.DBUpdates, error) {
	var kvWrites []*worldstate.KVWithMetadata
	var deletes []string

	nodes := make(map[string]*types.NodeConfig)
	for _, newNode := range newNodes {
		nodes[newNode.Id] = newNode
	}

	for _, oldNode := range oldNodes {
		if _, ok := nodes[oldNode.Id]; ok {
			if proto.Equal(oldNode, nodes[oldNode.Id]) {
				delete(nodes, oldNode.Id)
			}
			continue
		}

		deletes = append(deletes, string(NodeNamespace)+oldNode.Id)
	}

	for _, n := range nodes {
		value, err := proto.Marshal(n)
		if err != nil {
			return nil, err
		}

		kvWrites = append(
			kvWrites,
			&worldstate.KVWithMetadata{
				Key:   string(NodeNamespace) + n.Id,
				Value: value,
				Metadata: &types.Metadata{
					Version: version,
				},
			},
		)
	}

	if len(kvWrites) == 0 && len(deletes) == 0 {
		return nil, nil
	}

	return &worldstate.DBUpdates{
		Writes:  kvWrites,
		Deletes: deletes,
	}, nil
}

// ConstructProvenanceEntriesForNodes constructs provenance entries for the transaction that manipulates
// nodes present in the cluster configuration
func ConstructProvenanceEntriesForNodes(
	userID, txID string,
	nodeUpdates *worldstate.DBUpdates,
	db worldstate.DB,
) (*provenance.TxDataForProvenance, error) {
	identityQuerier := NewQuerier(db)
	txData := &provenance.TxDataForProvenance{
		IsValid:            true,
		DBName:             worldstate.ConfigDBName,
		UserID:             userID,
		TxID:               txID,
		Deletes:            make(map[string]*types.Version),
		OldVersionOfWrites: make(map[string]*types.Version),
	}

	if nodeUpdates == nil {
		return txData, nil
	}

	for _, w := range nodeUpdates.Writes {
		nodeID := getNodeIDFromCompositeUserKey(w.Key)
		txData.Writes = append(
			txData.Writes,
			&types.KVWithMetadata{
				Key:      nodeID,
				Value:    w.Value,
				Metadata: w.Metadata,
			},
		)

		version, err := identityQuerier.GetNodeVersion(nodeID)
		if err != nil {
			if _, ok := err.(*NotFoundErr); ok {
				continue
			}

			return nil, errors.Wrap(err, "error while fetching a node version")
		}
		txData.OldVersionOfWrites[nodeID] = version
	}

	for _, d := range nodeUpdates.Deletes {
		nodeID := getNodeIDFromCompositeUserKey(d)
		version, err := identityQuerier.GetNodeVersion(nodeID)
		if err != nil {
			// node to be deleted must exist
			return nil, errors.Wrap(err, "error while fetching a node version")
		}
		txData.Deletes[nodeID] = version
	}

	return txData, nil
}

func getUserIDFromCompositeUserKey(ckey string) string {
	strs := strings.Split(ckey, string(UserNamespace))
	return strs[1]
}

func getNodeIDFromCompositeUserKey(ckey string) string {
	strs := strings.Split(ckey, string(NodeNamespace))
	return strs[1]
}
