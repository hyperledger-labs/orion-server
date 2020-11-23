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

	return signPayload(querySigner, query)
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

	return signPayload(txSigner, tx)
}

func signPayload(signer *crypto.Signer, payload interface{}) ([]byte, error) {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	sig, err := signer.Sign(payloadBytes)
	if err != nil {
		return nil, err
	}
	return sig, nil
}

func SignQueryResponse(responseSigner *crypto.Signer, queryResp interface{}) ([]byte, error) {
	switch v := queryResp.(type) {
	case *types.GetUserResponse:
	case *types.GetDataResponse:
	case *types.GetDBStatusResponse:
	case *types.GetConfigResponse:
	case *types.GetLedgerPathResponse:
	case *types.GetBlockResponse:

	default:
		return nil, errors.Errorf("unknown query response type: %T", v)
	}

	return signPayload(responseSigner, queryResp)
}
