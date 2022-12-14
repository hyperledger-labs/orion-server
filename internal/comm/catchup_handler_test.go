// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package comm_test

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger-labs/orion-server/internal/comm"
	"github.com/hyperledger-labs/orion-server/internal/comm/mocks"
	"github.com/hyperledger-labs/orion-server/internal/utils"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewCatchupHandler(t *testing.T) {
	lg, err := logger.New(&logger.Config{
		Level:         "debug",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
	})
	require.NoError(t, err)

	h := comm.NewCatchupHandler(lg, nil, 0)
	require.NotNil(t, h)
}

func TestCatchupHandler_ServeHTTP_Height(t *testing.T) {
	lg, err := logger.New(&logger.Config{
		Level:         "debug",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
	})
	require.NoError(t, err)

	t.Run("height ok", func(t *testing.T) {
		ledgerReader := &mocks.LedgerReader{}
		h := comm.NewCatchupHandler(lg, ledgerReader, 0)
		require.NotNil(t, h)

		resp := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, comm.GetHeightPath, nil)
		req.Header.Set("Accept", "application/json")

		ledgerReader.HeightReturns(5, nil)

		h.ServeHTTP(resp, req)
		assert.Equal(t, http.StatusOK, resp.Result().StatusCode)

		decoder := json.NewDecoder(resp.Result().Body)
		hResp := &comm.HeightResponse{}
		err = decoder.Decode(hResp)
		require.NoError(t, err, "body: %s", resp.Result().Body)
		assert.Equal(t, &comm.HeightResponse{Height: 5}, hResp)
	})

	t.Run("height error", func(t *testing.T) {
		ledgerReader := &mocks.LedgerReader{}
		h := comm.NewCatchupHandler(lg, ledgerReader, 0)
		require.NotNil(t, h)

		resp := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, comm.GetHeightPath, nil)
		req.Header.Set("Accept", "application/json")

		ledgerReader.HeightReturns(0, errors.New("oops"))

		h.ServeHTTP(resp, req)
		assert.Equal(t, http.StatusInternalServerError, resp.Result().StatusCode)

		decoder := json.NewDecoder(resp.Result().Body)
		errResp := &types.HttpResponseErr{}
		err = decoder.Decode(errResp)
		require.NoError(t, err, "body: %s", resp.Result().Body)
		assert.Equal(t, &types.HttpResponseErr{ErrMsg: "oops"}, errResp)
	})
}

func TestCatchupHandler_ServeHTTP_Blocks(t *testing.T) {
	lg, err := logger.New(&logger.Config{
		Level:         "debug",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
	})
	require.NoError(t, err)

	ledger1 := &memLedger{}
	for n := uint64(1); n < 6; n++ {
		ledger1.Append(&types.Block{Header: &types.BlockHeader{BaseHeader: &types.BlockHeaderBase{Number: n}}})
	}

	h := comm.NewCatchupHandler(lg, ledger1, 0)
	require.NotNil(t, h)

	t.Run("bad: no parameters", func(t *testing.T) {
		resp := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, comm.GetBlocksPath, nil)
		req.Header.Set("Accept", "application/json")

		h.ServeHTTP(resp, req)
		require.Equal(t, http.StatusNotFound, resp.Result().StatusCode)
		bodyBytes, err := ioutil.ReadAll(resp.Body)
		require.NoError(t, err)
		assert.Equal(t, "404 page not found\n", string(bodyBytes))
	})

	t.Run("bad: start=0", func(t *testing.T) {
		resp := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, comm.GetBlocksPath, nil)
		q := req.URL.Query()
		q.Add("start", "0")
		q.Add("end", "4")
		req.URL.RawQuery = q.Encode()
		req.Header.Set("Accept", utils.MultiPartFormData)
		t.Logf("url: %s", req.URL.String())

		h.ServeHTTP(resp, req)
		require.Equal(t, http.StatusBadRequest, resp.Result().StatusCode)
		decoder := json.NewDecoder(resp.Result().Body)
		errResp := &types.HttpResponseErr{}
		err = decoder.Decode(errResp)
		require.NoError(t, err)
		assert.Equal(t, &types.HttpResponseErr{ErrMsg: "requested startId [0] must be greater than 0"}, errResp)
	})

	t.Run("bad: start>height", func(t *testing.T) {
		resp := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, comm.GetBlocksPath, nil)
		q := req.URL.Query()
		q.Add("start", "10")
		q.Add("end", "14")
		req.URL.RawQuery = q.Encode()
		req.Header.Set("Accept", utils.MultiPartFormData)
		t.Logf("url: %s", req.URL.String())

		h.ServeHTTP(resp, req)
		require.Equal(t, http.StatusBadRequest, resp.Result().StatusCode)
		decoder := json.NewDecoder(resp.Result().Body)
		errResp := &types.HttpResponseErr{}
		err = decoder.Decode(errResp)
		require.NoError(t, err)
		assert.Equal(t, &types.HttpResponseErr{ErrMsg: "requested startId [10] is out of range, height is [5]"}, errResp)
	})

	t.Run("bad: start>end", func(t *testing.T) {
		resp := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, comm.GetBlocksPath, nil)
		q := req.URL.Query()
		q.Add("start", "4")
		q.Add("end", "2")
		req.URL.RawQuery = q.Encode()
		req.Header.Set("Accept", utils.MultiPartFormData)
		t.Logf("url: %s", req.URL.String())

		h.ServeHTTP(resp, req)
		require.Equal(t, http.StatusBadRequest, resp.Result().StatusCode)
		decoder := json.NewDecoder(resp.Result().Body)
		errResp := &types.HttpResponseErr{}
		err = decoder.Decode(errResp)
		require.NoError(t, err)
		assert.Equal(t, &types.HttpResponseErr{ErrMsg: "query error: startId=4 > endId=2"}, errResp)
	})

	t.Run("valid", func(t *testing.T) {
		resp := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodGet, comm.GetBlocksPath, nil)
		q := req.URL.Query()
		q.Add("start", "2")
		q.Add("end", "4")
		req.URL.RawQuery = q.Encode()
		req.Header.Set("Accept", utils.MultiPartFormData)
		t.Logf("url: %s", req.URL.String())

		h.ServeHTTP(resp, req)
		require.Equal(t, http.StatusOK, resp.Result().StatusCode)

		_, params, err := mime.ParseMediaType(resp.Result().Header.Get("Content-Type"))
		require.NoError(t, err)
		boundary, ok := params["boundary"]
		require.True(t, ok)
		mr := multipart.NewReader(resp.Result().Body, boundary)
		bNum := uint64(2)
		for part, errP := mr.NextPart(); errP == nil; part, errP = mr.NextPart() {
			assert.Equal(t, fmt.Sprintf("block-%d", bNum-2), part.FormName())
			assert.Equal(t, fmt.Sprintf("num-%d", bNum), part.FileName())

			blockBytes, err := ioutil.ReadAll(part)
			require.NoError(t, err)
			require.NotNil(t, blockBytes)

			block := &types.Block{}
			err = proto.Unmarshal(blockBytes, block)
			require.NoError(t, err)
			assert.Equal(t, bNum, block.Header.BaseHeader.Number)
			bNum++
		}
		require.Equal(t, uint64(5), bNum)
	})

	t.Run("valid: end > height", func(t *testing.T) {
		resp := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodGet, comm.GetBlocksPath, nil)
		q := req.URL.Query()
		q.Add("start", "2")
		q.Add("end", "10")
		req.URL.RawQuery = q.Encode()
		req.Header.Set("Accept", utils.MultiPartFormData)
		t.Logf("url: %s", req.URL.String())

		h.ServeHTTP(resp, req)
		require.Equal(t, http.StatusOK, resp.Result().StatusCode)

		_, params, err := mime.ParseMediaType(resp.Result().Header.Get("Content-Type"))
		require.NoError(t, err)
		boundary, ok := params["boundary"]
		require.True(t, ok)
		mr := multipart.NewReader(resp.Result().Body, boundary)
		bNum := uint64(2)
		for part, errP := mr.NextPart(); errP == nil; part, errP = mr.NextPart() {
			assert.Equal(t, fmt.Sprintf("block-%d", bNum-2), part.FormName())
			assert.Equal(t, fmt.Sprintf("num-%d", bNum), part.FileName())

			blockBytes, err := ioutil.ReadAll(part)
			require.NoError(t, err)
			require.NotNil(t, blockBytes)

			block := &types.Block{}
			err = proto.Unmarshal(blockBytes, block)
			require.NoError(t, err)
			assert.Equal(t, bNum, block.Header.BaseHeader.Number)
			bNum++
		}
		require.Equal(t, uint64(6), bNum)
	})
}

func TestCatchupHandler_ServeHTTP_LargeResponse(t *testing.T) {
	lg, err := logger.New(&logger.Config{
		Level:         "debug",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
	})
	require.NoError(t, err)

	b1Size := 0
	b5Size := 0

	ledger1 := &memLedger{}
	for n := uint64(1); n < 11; n++ {
		block := &types.Block{
			Header: &types.BlockHeader{
				BaseHeader:           &types.BlockHeaderBase{Number: n},
				TxMerkleTreeRootHash: make([]byte, 1024), //just for size
			},
		}
		ledger1.Append(block)

		if n == 1 {
			b1Size = len(utils.MarshalOrPanic(block))
		}
		if n < 6 {
			b5Size += len(utils.MarshalOrPanic(block))
		}
	}

	t.Run("too many blocks in request", func(t *testing.T) {
		h := comm.NewCatchupHandler(lg, ledger1, b5Size) // 5 blocks in response
		require.NotNil(t, h)

		resp := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodGet, comm.GetBlocksPath, nil)
		q := req.URL.Query()
		q.Add("start", "2")
		q.Add("end", "9") // too many
		req.URL.RawQuery = q.Encode()
		req.Header.Set("Accept", utils.MultiPartFormData)

		h.ServeHTTP(resp, req)
		require.Equal(t, http.StatusOK, resp.Result().StatusCode)

		_, params, err := mime.ParseMediaType(resp.Result().Header.Get("Content-Type"))
		require.NoError(t, err)
		boundary, ok := params["boundary"]
		require.True(t, ok)
		mr := multipart.NewReader(resp.Result().Body, boundary)
		bNum := uint64(2)
		var part *multipart.Part
		var errP error
		for part, errP = mr.NextPart(); errP == nil; part, errP = mr.NextPart() {
			assert.Equal(t, fmt.Sprintf("block-%d", bNum-2), part.FormName())
			assert.Equal(t, fmt.Sprintf("num-%d", bNum), part.FileName())

			blockBytes, err := ioutil.ReadAll(part)
			require.NoError(t, err)
			require.NotNil(t, blockBytes)

			block := &types.Block{}
			err = proto.Unmarshal(blockBytes, block)
			require.NoError(t, err)
			assert.Equal(t, bNum, block.Header.BaseHeader.Number)
			bNum++
		}
		require.EqualError(t, errP, io.EOF.Error())
		require.Equal(t, uint64(7), bNum) // blocks 2-6 in response
	})

	t.Run("blocks are bigger than max-response-size", func(t *testing.T) {
		h := comm.NewCatchupHandler(lg, ledger1, b1Size/2) // 1 block in response
		require.NotNil(t, h)

		resp := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodGet, comm.GetBlocksPath, nil)
		q := req.URL.Query()
		q.Add("start", "2")
		q.Add("end", "8") // too many
		req.URL.RawQuery = q.Encode()
		req.Header.Set("Accept", utils.MultiPartFormData)

		h.ServeHTTP(resp, req)
		require.Equal(t, http.StatusOK, resp.Result().StatusCode)

		_, params, err := mime.ParseMediaType(resp.Result().Header.Get("Content-Type"))
		require.NoError(t, err)
		boundary, ok := params["boundary"]
		require.True(t, ok)
		mr := multipart.NewReader(resp.Result().Body, boundary)
		bNum := uint64(2)
		var part *multipart.Part
		var errP error
		for part, errP = mr.NextPart(); errP == nil; part, errP = mr.NextPart() {
			assert.Equal(t, fmt.Sprintf("block-%d", bNum-2), part.FormName())
			assert.Equal(t, fmt.Sprintf("num-%d", bNum), part.FileName())

			blockBytes, err := ioutil.ReadAll(part)
			require.NoError(t, err)
			require.NotNil(t, blockBytes)

			block := &types.Block{}
			err = proto.Unmarshal(blockBytes, block)
			require.NoError(t, err)
			assert.Equal(t, bNum, block.Header.BaseHeader.Number)
			bNum++
		}
		require.EqualError(t, errP, io.EOF.Error())
		require.Equal(t, uint64(3), bNum) // block 2 in response
	})
}
