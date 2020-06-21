package committer

import (
	"github.ibm.com/blockchaindb/server/api"
	"github.ibm.com/blockchaindb/server/pkg/worldstate"
)

type Committer struct {
	db worldstate.DB
	// TODO
	// 1. Block Store
	// 2. Provenance Store
	// 3. Proof Store
}

func NewCommitter(db worldstate.DB) *Committer {
	return &Committer{
		db: db,
	}
}

func (c *Committer) Commit(block *api.Block, validationInfo []*api.ValidationInfo) error {
	return nil
}
