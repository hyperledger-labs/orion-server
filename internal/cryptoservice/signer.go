package cryptoservice

import (
	"encoding/json"

	"github.com/pkg/errors"
	"github.ibm.com/blockchaindb/server/pkg/crypto"
	"github.ibm.com/blockchaindb/server/pkg/types"
)

func SignQuery(querySigner *crypto.Signer, query interface{}) ([]byte, error) {
	switch v := query.(type) {
	case *types.GetConfigQuery:
	case *types.GetDataQuery:
	case *types.GetDBStatusQuery:
	case *types.GetUserQuery:
	case *types.GetBlockQuery:
	case *types.GetLedgerPathQuery:

	default:
		return nil, errors.Errorf("unknown query type: %T", v)
	}

	queryBytes, err := json.Marshal(query)
	if err != nil {
		return nil, err
	}
	sig, err := querySigner.Sign(queryBytes)
	if err != nil {
		return nil, err
	}
	return sig, nil
}

func SignTx(txSigner *crypto.Signer, tx interface{}) ([]byte, error) {
	switch v := tx.(type) {
	case *types.ConfigTx:
	case *types.DataTx:
	case *types.UserAdministrationTx:
	case *types.DBAdministrationTx:

	default:
		return nil, errors.Errorf("unknown transaction type: %T", v)
	}

	txBytes, err := json.Marshal(tx)
	if err != nil {
		return nil, err
	}
	sig, err := txSigner.Sign(txBytes)
	if err != nil {
		return nil, err
	}
	return sig, nil
}
