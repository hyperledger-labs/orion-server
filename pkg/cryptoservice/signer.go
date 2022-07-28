// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package cryptoservice

import (
	"github.com/hyperledger-labs/orion-server/pkg/crypto"
	"github.com/hyperledger-labs/orion-server/pkg/marshal"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

func SignQuery(querySigner crypto.Signer, query interface{}) ([]byte, error) {
	switch v := query.(type) {
	case *types.GetConfigQuery:
	case *types.GetConfigBlockQuery:
	case *types.GetClusterStatusQuery:
	case *types.GetDataQuery:
	case *types.GetDataRangeQuery:
	case *types.GetDBStatusQuery:
	case *types.GetDBIndexQuery:
	case *types.GetUserQuery:
	case *types.GetBlockQuery:
	case *types.GetLastBlockQuery:
	case *types.GetLedgerPathQuery:
	case *types.GetNodeConfigQuery:
	case *types.GetTxProofQuery:
	case *types.GetTxReceiptQuery:
	case *types.GetHistoricalDataQuery:
	case *types.GetDataReadersQuery:
	case *types.GetDataWritersQuery:
	case *types.GetDataReadByQuery:
	case *types.GetDataWrittenByQuery:
	case *types.GetDataDeletedByQuery:
	case *types.GetTxIDsSubmittedByQuery:
	case *types.GetMostRecentUserOrNodeQuery:
	case *types.GetDataProofQuery:
	case *types.DataJSONQuery:

	default:
		return nil, errors.Errorf("unknown query type: %T", v)
	}

	return SignPayload(querySigner, query)
}

func SignTx(txSigner crypto.Signer, tx interface{}) ([]byte, error) {
	switch v := tx.(type) {
	case *types.ConfigTx:
	case *types.DataTx:
	case *types.UserAdministrationTx:
	case *types.DBAdministrationTx:

	default:
		return nil, errors.Errorf("unknown transaction type: %T", v)
	}

	return SignPayload(txSigner, tx)
}

func SignPayload(signer crypto.Signer, payload interface{}) ([]byte, error) {
	payloadBytes, err := marshal.DefaultMarshaler().Marshal(payload.(proto.Message))
	if err != nil {
		return nil, err
	}

	sig, err := signer.Sign(payloadBytes)
	if err != nil {
		return nil, err
	}
	return sig, nil
}
