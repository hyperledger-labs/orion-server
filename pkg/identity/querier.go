package identity

import (
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/pkg/worldstate"
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

// GetUser returns the credentials associated with the given
// non-admin userID
func (q *Querier) GetUser(userID string) (*types.User, *types.Metadata, error) {
	val, meta, err := q.db.Get(worldstate.UsersDBName, string(userNamespace)+userID)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "error while fetching userID [%s]", userID)
	}

	if val == nil {
		return nil, nil, nil
	}

	user := &types.User{}
	if err := proto.Unmarshal(val, user); err != nil {
		return nil, nil, errors.Wrapf(err, "error while unmarshaling persisted value of userID [%s]", userID)
	}

	return user, meta, nil
}

// HasReadAccess returns true if the given userID has read access on the given
// dbName. Otherwise, it returns false
func (q *Querier) HasReadAccess(userID, dbName string) (bool, error) {
	user, _, err := q.GetUser(userID)
	if err != nil {
		return false, err
	}

	dbPermission := user.GetPrivilege().GetDBPermission()
	if dbPermission == nil {
		return false, err
	}

	_, ok := dbPermission[dbName]
	return ok, nil
}

// HasReadWriteAccess returns true if the given userID has read-write access on the given
// dbName. Otherwise, it returns false
func (q *Querier) HasReadWriteAccess(userID, dbName string) (bool, error) {
	user, _, err := q.GetUser(userID)
	if err != nil {
		return false, err
	}

	dbPermission := user.GetPrivilege().GetDBPermission()
	if dbPermission == nil {
		return false, err
	}

	access, ok := dbPermission[dbName]
	if !ok {
		return false, nil
	}

	return access == types.Privilege_ReadWrite, nil
}

// HasDBAdministrationPrivilege returns true if the given userID has privilege to perform
// database administrative tasks such as creation and deletion of databases
func (q *Querier) HasDBAdministrationPrivilege(userID string) (bool, error) {
	user, _, err := q.GetUser(userID)
	if err != nil {
		return false, err
	}

	return user.GetPrivilege().GetDBAdministration(), nil
}

// HasUserAdministrationPrivilege returns true if the given userID has privilege to perform
// user administrative tasks such as creation, updation, and deletion of users
func (q *Querier) HasUserAdministrationPrivilege(userID string) (bool, error) {
	user, _, err := q.GetUser(userID)
	if err != nil {
		return false, err
	}

	return user.GetPrivilege().GetUserAdministration(), nil
}

// HasClusterAdministrationPrivilege returns true if the given userID has privilege to perform
// cluster administrative tasks such as addition, removal, and updation of node or cluster
// configuration
func (q *Querier) HasClusterAdministrationPrivilege(userID string) (bool, error) {
	user, _, err := q.GetUser(userID)
	if err != nil {
		return false, err
	}

	return user.GetPrivilege().GetClusterAdministration(), nil
}
