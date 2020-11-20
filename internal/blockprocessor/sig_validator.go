package blockprocessor

import (
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.ibm.com/blockchaindb/server/internal/cryptoservice"
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
		return nil, errors.Wrapf(err, "failed to json.Marshal Tx: %s", txPayload)
	}

	err = s.sigVerifier.Verify(user, signature, requestBytes)
	if err != nil {
		return &types.ValidationInfo{
			Flag:            types.Flag_INVALID_UNAUTHORISED,
			ReasonIfInvalid: fmt.Sprintf("signature verification failed: %s", err.Error()),
		}, nil
	}

	return &types.ValidationInfo{Flag: types.Flag_VALID}, nil
}
