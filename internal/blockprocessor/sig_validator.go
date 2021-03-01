// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package blockprocessor

import (
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
	"github.ibm.com/blockchaindb/server/pkg/cryptoservice"
	"github.ibm.com/blockchaindb/server/pkg/logger"
	"github.ibm.com/blockchaindb/server/pkg/types"
)

type txSigValidator struct {
	sigVerifier *cryptoservice.SignatureVerifier
	logger      *logger.SugarLogger
}

func (s *txSigValidator) validate(
	user string,
	signature []byte,
	txPayload interface{},
) (*types.ValidationInfo, error) {
	requestBytes, err := json.Marshal(txPayload)
	if err != nil {
		s.logger.Errorf("Error during json.Marshal Tx: %s, error: %s", txPayload, err)
		return nil, errors.Wrapf(err, "failed to json.Marshal Tx: %s", txPayload)
	}

	err = s.sigVerifier.Verify(user, signature, requestBytes)
	if err != nil {
		s.logger.Debugf("Failed to verify Tx (Flag_INVALID_UNAUTHORISED): user: %s, sig: %x, payload: %s, error: %s",
			user, signature, txPayload, err)
		return &types.ValidationInfo{
			Flag:            types.Flag_INVALID_UNAUTHORISED,
			ReasonIfInvalid: fmt.Sprintf("signature verification failed: %s", err.Error()),
		}, nil
	}

	return &types.ValidationInfo{Flag: types.Flag_VALID}, nil
}
