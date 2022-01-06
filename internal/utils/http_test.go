// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package utils

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
)

func TestSendHTTPResponse(t *testing.T) {
	t.Parallel()

	t.Run("ok status", func(t *testing.T) {
		t.Parallel()

		w := httptest.NewRecorder()
		dbStatus := &types.GetDBStatusResponseEnvelope{
			Response: &types.GetDBStatusResponse{
				Header: &types.ResponseHeader{
					NodeId: "testID",
				},
				Exist: false,
			},
		}
		SendHTTPResponse(w, http.StatusOK, dbStatus)

		require.Equal(t, http.StatusOK, w.Code)
		actualDBStatus := &types.GetDBStatusResponseEnvelope{}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), actualDBStatus))
		require.True(t, proto.Equal(dbStatus, actualDBStatus))
	})

	t.Run("forbidden status", func(t *testing.T) {
		t.Parallel()

		w := httptest.NewRecorder()
		err := &types.HttpResponseErr{
			ErrMsg: "user does not have a read permission",
		}
		SendHTTPResponse(w, http.StatusForbidden, err)

		require.Equal(t, http.StatusForbidden, w.Code)
		actualErr := &types.HttpResponseErr{}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), actualErr))
		require.Equal(t, err, actualErr)
	})
}

func TestSendHTTPRedirectServer(t *testing.T) {
	w := httptest.NewRecorder()

	reqUrl := &url.URL{
		Scheme: "http",
		Host:   "11.11.11.11:6091",
		Path:   "some/path",
	}
	r, err := http.NewRequest(http.MethodPost, reqUrl.String(), bytes.NewReader([]byte("body")))
	require.NoError(t, err)

	hostPort := "10.10.10.10:6090"
	SendHTTPRedirectServer(w, r, hostPort)

	require.Equal(t, http.StatusTemporaryRedirect, w.Code)
	locationUrl := w.Header().Get("Location")
	require.Equal(t, "http://10.10.10.10:6090/some/path", locationUrl)
}
