package server

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/pkg/server/mocks"
)

func TestSendHTTPResponse(t *testing.T) {
	w := &mocks.ResponseWriter{}
	dbStatus := &types.GetDBStatusResponseEnvelope{
		Payload: &types.GetDBStatusResponse{
			Header: &types.ResponseHeader{
				NodeID: "testID",
			},
		},
	}
	w.On("Header").Return(http.Header(map[string][]string{}))
	w.On("WriteHeader", mock.Anything).Run(func(args mock.Arguments) {
		httpCode := args.Get(0).(int)
		require.Equal(t, http.StatusOK, httpCode)
	})
	w.On("Write", mock.Anything).Return(0, nil).Run(func(args mock.Arguments) {
		b := args.Get(0).([]byte)
		mB, err := json.Marshal(dbStatus)
		require.NoError(t, err)
		require.Equal(t, b, mB)
	})
	SendHTTPResponse(w, http.StatusOK, dbStatus)
}
