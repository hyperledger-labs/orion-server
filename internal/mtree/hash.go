// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package mtree

import (
	"encoding/json"

	"github.com/hyperledger-labs/orion-server/pkg/crypto"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
)

func calculateBlockTxHashes(block *types.Block) ([][]byte, error) {
	hashes := make([][]byte, 0)

	switch block.Payload.(type) {
	case *types.Block_DataTxEnvelopes:
		for i, tx := range block.GetDataTxEnvelopes().GetEnvelopes() {
			h, err := calculateTxHash(tx, block.GetHeader().GetValidationInfo()[i])
			if err != nil {
				return nil, errors.Wrapf(err, "can't calculate msg hash %v", tx.GetPayload())
			}
			hashes = append(hashes, h)
		}
		return hashes, nil
	case *types.Block_UserAdministrationTxEnvelope:
		userTx := block.GetUserAdministrationTxEnvelope()
		h, err := calculateTxHash(userTx, block.GetHeader().GetValidationInfo()[0])
		if err != nil {
			return nil, errors.Wrapf(err, "can't calculate msg hash %v", userTx.GetPayload())
		}
		return [][]byte{h}, nil
	case *types.Block_DbAdministrationTxEnvelope:
		dbTx := block.GetDbAdministrationTxEnvelope()
		h, err := calculateTxHash(dbTx, block.GetHeader().GetValidationInfo()[0])
		if err != nil {
			return nil, errors.Wrapf(err, "can't calculate msg hash %v", dbTx.GetPayload())
		}
		return [][]byte{h}, nil
	case *types.Block_ConfigTxEnvelope:
		configTx := block.GetConfigTxEnvelope()
		h, err := calculateTxHash(configTx, block.GetHeader().GetValidationInfo()[0])
		if err != nil {
			return nil, errors.Wrapf(err, "can't calculate msg hash %v", configTx.GetPayload())
		}
		return [][]byte{h}, nil
	default:
		return nil, errors.Errorf("unexpected transaction envelope in the block")
	}

}

func calculateTxHash(msg proto.Message, valInfo proto.Message) ([]byte, error) {
	payloadBytes, err := json.Marshal(msg)
	if err != nil {
		return nil, errors.Wrapf(err, "can't serialize msg to json %v", msg)
	}
	valBytes, err := json.Marshal(valInfo)
	if err != nil {
		return nil, errors.Wrapf(err, "can't validationInfo msg to json %v", msg)
	}
	finalBytes := append(payloadBytes, valBytes...)
	return crypto.ComputeSHA256Hash(finalBytes)
}
