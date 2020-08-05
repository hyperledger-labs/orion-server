package identity

import (
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/pkg/worldstate"
)

// Querier provides method to query both user and
// admin information
type Querier struct {
	db worldstate.DB
}

// GetAdmin returns the credentials associated with the given
// admin ID
func (q *Querier) GetAdmin(ID string) (*types.Admin, error) {
	return nil, nil
}

// GetUser returns the credentials associated with the given
// user ID
func (q *Querier) GetUser(ID string) (*types.User, error) {
	return nil, nil
}

// GetNode returns the credentials associated with the given
// node ID
func (q *Querier) GetNode(ID string) (*types.Node, error) {
	return nil, nil
}
