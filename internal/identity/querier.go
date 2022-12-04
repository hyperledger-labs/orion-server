// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package identity

import (
	"crypto/x509"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger-labs/orion-server/internal/worldstate"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/pkg/errors"
)

// Querier provides method to query both user and
// admin information
type Querier struct {
	db worldstate.DB
	// TODO: cache to reduce the number of DB access
	// a listener to invalidate committed entries
}

// NewQuerier returns a querier to fetch identity
// and related credentials
func NewQuerier(db worldstate.DB) *Querier {
	return &Querier{
		db: db,
	}
}

// DoesUserExist returns true if the given user exist. Otherwise, it
// return false
func (q *Querier) DoesUserExist(userID string) (bool, error) {
	exist, err := q.db.Has(worldstate.UsersDBName, string(UserNamespace)+userID)
	if err != nil {
		return false, errors.Wrapf(err, "error while checking the existance of the userID [%s]", userID)
	}

	return exist, nil
}

// GetUser returns the credentials associated with the given
// non-admin userID
func (q *Querier) GetUser(userID string) (*types.User, *types.Metadata, error) {
	val, meta, err := q.db.Get(worldstate.UsersDBName, string(UserNamespace)+userID)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "error while fetching userID [%s]", userID)
	}

	if val == nil {
		return nil, nil, &NotFoundErr{
			id: userID,
		}
	}

	user := &types.User{}
	if err := proto.Unmarshal(val, user); err != nil {
		return nil, nil, errors.Wrapf(err, "error while unmarshaling persisted value of userID [%s]", userID)
	}

	return user, meta, nil
}

// GetAccessControl returns the ACL defined on the userID
func (q *Querier) GetAccessControl(userID string) (*types.AccessControl, error) {
	_, metadata, err := q.GetUser(userID)
	if err != nil {
		return nil, err
	}

	return metadata.GetAccessControl(), nil
}

//TODO keep a cache of user and parsed certificates to avoid going to the DB and parsing the certificate
// on every TX. Provide a mechanism to invalidate the cache when the user database changes.

// GetCertificate returns the current certificate associated with a given userID
func (q *Querier) GetCertificate(userID string) (*x509.Certificate, error) {
	user, _, err := q.GetUser(userID)
	if err != nil {
		return nil, err
	}

	cert, err := x509.ParseCertificate(user.Certificate)
	if err != nil {
		return nil, err
	}

	return cert, nil
}

// GetUserVersion returns the current version of a given userID
func (q *Querier) GetUserVersion(userID string) (*types.Version, error) {
	_, metadata, err := q.GetUser(userID)
	if err != nil {
		return nil, err
	}

	return metadata.Version, nil
}

// HasReadAccessOnDataDB returns true if the given userID has read access on the given
// dbName. Otherwise, it returns false
func (q *Querier) HasReadAccessOnDataDB(userID, dbName string) (bool, error) {
	return q.hasPrivilege(userID, dbName, types.Privilege_Read)
}

// HasReadWriteAccess returns true if the given userID has read-write access on the given
// dbName. Otherwise, it returns false
func (q *Querier) HasReadWriteAccess(userID, dbName string) (bool, error) {
	return q.hasPrivilege(userID, dbName, types.Privilege_ReadWrite)
}

// HasAdministrationPrivilege returns true if the given userID has privilege to perform
// administrative tasks
func (q *Querier) HasAdministrationPrivilege(userID string) (bool, error) {
	user, _, err := q.GetUser(userID)
	if err != nil {
		return false, err
	}

	return user.GetPrivilege().GetAdmin(), nil
}

// HasLedgerAccess check is user has access to ledger data
// For now, all users has this access, so only user existence validated
func (q *Querier) HasLedgerAccess(userID string) (bool, error) {
	return q.DoesUserExist(userID)
}

// GetNode returns the credentials associated with the given
// node ID
func (q *Querier) GetNode(nodeID string) (*types.NodeConfig, *types.Metadata, error) {
	val, meta, err := q.db.Get(worldstate.ConfigDBName, string(NodeNamespace)+nodeID)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "error while fetching nodeID [%s]", nodeID)
	}

	if val == nil {
		return nil, nil, &NotFoundErr{
			id: nodeID,
		}
	}

	node := &types.NodeConfig{}
	if err := proto.Unmarshal(val, node); err != nil {
		return nil, nil, errors.Wrapf(err, "error while unmarshaling persisted value of nodeID [%s]", nodeID)
	}

	return node, meta, nil
}

// GetNodeVersion returns the current version of a given nodeID
func (q *Querier) GetNodeVersion(nodeID string) (*types.Version, error) {
	_, metadata, err := q.GetNode(nodeID)
	if err != nil {
		return nil, err
	}

	return metadata.Version, nil
}

func (q *Querier) hasPrivilege(userID, dbName string, privilege types.Privilege_Access) (bool, error) {
	user, _, err := q.GetUser(userID)
	if err != nil {
		return false, err
	}

	if user.GetPrivilege() != nil && user.Privilege.Admin {
		return true, nil
	}

	dbPermission := user.GetPrivilege().GetDbPermission()
	if dbPermission == nil {
		return false, err
	}

	p, ok := dbPermission[dbName]
	if !ok {
		return false, nil
	}

	return p >= privilege, nil
}

// NotFoundErr denotes that the id does not exist in the worldstate
type NotFoundErr struct {
	id string
}

func (e *NotFoundErr) Error() string {
	return fmt.Sprintf("the user [%s] does not exist", e.id)
}
