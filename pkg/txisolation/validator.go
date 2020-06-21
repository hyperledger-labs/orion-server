package txisolation

import (
	"github.ibm.com/blockchaindb/server/api"
	"github.ibm.com/blockchaindb/server/pkg/worldstate"
)

type Validator struct {
	db worldstate.DB
}

func NewValidator(db worldstate.DB) *Validator {
	return &Validator{
		db: db,
	}

}

func (v *Validator) Validate(block *api.Block) ([]*api.ValidationInfo, error) {
	return nil, nil
}
