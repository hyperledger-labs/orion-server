// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"encoding/json"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/pkg/errors"
)

func BlockPayloadToTxIDs(blockPayload interface{}) ([]string, error) {
	var txIDs []string

	switch env := blockPayload.(type) {
	case *types.Block_DataTxEnvelopes:
		for i, dEnv := range env.DataTxEnvelopes.GetEnvelopes() {
			p := dEnv.GetPayload()
			if p == nil {
				return nil, errors.Errorf("empty payload in index [%d]: %+v", i, env)
			}
			id := p.GetTxId()
			if id == "" {
				return nil, errors.Errorf("missing TxId in index [%d]: %+v", i, env.DataTxEnvelopes.GetEnvelopes()[i])
			}
			txIDs = append(txIDs, id)
		}

		if len(txIDs) == 0 {
			return nil, errors.Errorf("empty payload in: %+v", blockPayload)
		}

	case *types.Block_UserAdministrationTxEnvelope:
		p := env.UserAdministrationTxEnvelope.GetPayload()
		if p == nil {
			return nil, errors.Errorf("empty payload in: %+v", blockPayload)
		}
		id := p.GetTxId()
		if id == "" {
			return nil, errors.Errorf("missing TxId in: %+v", blockPayload)
		}
		txIDs = append(txIDs, id)

	case *types.Block_ConfigTxEnvelope:
		p := env.ConfigTxEnvelope.GetPayload()
		if p == nil {
			return nil, errors.Errorf("empty payload in: %+v", blockPayload)
		}
		id := p.GetTxId()
		if id == "" {
			return nil, errors.Errorf("missing TxId in: %+v", blockPayload)
		}
		txIDs = append(txIDs, id)

	case *types.Block_DbAdministrationTxEnvelope:
		p := env.DbAdministrationTxEnvelope.GetPayload()
		if p == nil {
			return nil, errors.Errorf("empty payload in: %+v", blockPayload)
		}
		id := p.GetTxId()
		if id == "" {
			return nil, errors.Errorf("missing TxId in: %+v", blockPayload)
		}
		txIDs = append(txIDs, id)

	default:
		return nil, errors.Errorf("unexpected envelope type: %v", env)
	}

	return txIDs, nil
}

func IsConfigBlock(block *types.Block) bool {
	switch block.GetPayload().(type) {
	case *types.Block_ConfigTxEnvelope:
		return true
	default:
		return false
	}
}

func MarshalOrPanic(m proto.Message) []byte {
	bytes, err := proto.Marshal(m)
	if err != nil {
		panic(err)
	}

	return bytes
}

func MarshalJsonOrPanic(o interface{}) []byte {
	bytes, err := json.Marshal(o)
	if err != nil {
		panic(err)
	}

	return bytes
}
