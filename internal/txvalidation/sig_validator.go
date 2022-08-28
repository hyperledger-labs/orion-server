// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package txvalidation

import (
	"fmt"

	"github.com/hyperledger-labs/orion-server/pkg/cryptoservice"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/marshal"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
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
	requestBytes, err := marshal.DefaultMarshaler().Marshal(txPayload.(proto.Message))
	if err != nil {
		s.logger.Errorf("Error during Marshal Tx: %s, error: %s", txPayload, err)
		return nil, errors.Wrapf(err, "failed to Marshal Tx: %s", txPayload)
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
