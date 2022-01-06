// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package comm

import (
	"fmt"
	"mime/multipart"
	"net/http"

	"github.com/golang/protobuf/proto"
	"github.com/gorilla/mux"
	"github.com/hyperledger-labs/orion-server/internal/utils"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/types"
)

const (
	BCDBPeerEndpoint = "/bcdb-peer/"
	GetBlocksPath    = BCDBPeerEndpoint + "blocks"
	GetHeightPath    = BCDBPeerEndpoint + "height"

	maxResponseBytesDefault = 100 * 1024 * 1024 // protects the server against huge requests from a client
)

//go:generate counterfeiter -o mocks/ledger_reader.go --fake-name LedgerReader . LedgerReader

type LedgerReader interface {
	Height() (uint64, error)
	Get(blockNumber uint64) (*types.Block, error)
}

type catchupHandler struct {
	router           *mux.Router
	lg               *logger.SugarLogger
	ledgerReader     LedgerReader
	maxResponseBytes int
}

func NewCatchupHandler(lg *logger.SugarLogger, ledgerReader LedgerReader, maxResponseBytes int) *catchupHandler {
	h := &catchupHandler{
		router:           mux.NewRouter(),
		lg:               lg,
		ledgerReader:     ledgerReader,
		maxResponseBytes: maxResponseBytesDefault,
	}

	if maxResponseBytes > 0 {
		h.maxResponseBytes = maxResponseBytes
	}

	h.router.HandleFunc(GetBlocksPath, h.blocksRequest).Methods(http.MethodGet).Headers("Accept", "multipart/form-data").Queries("start", "{startId:[0-9]+}", "end", "{endId:[0-9]+}")
	h.router.HandleFunc(GetHeightPath, h.heightRequest).Methods(http.MethodGet)

	return h
}

func (h *catchupHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.lg.Debugf("request: %s", r.URL)
	h.router.ServeHTTP(w, r)
}

func (h *catchupHandler) blocksRequest(response http.ResponseWriter, request *http.Request) {
	params := mux.Vars(request)
	startBlockNum, endBlockNum, err := utils.GetStartAndEndBlockNum(params)
	if err != nil {
		utils.SendHTTPResponse(response, http.StatusBadRequest, &types.HttpResponseErr{ErrMsg: err.Error()})
		return
	}

	height, err := h.ledgerReader.Height()
	if err != nil {
		utils.SendHTTPResponse(response, http.StatusInternalServerError, &types.HttpResponseErr{ErrMsg: err.Error()})
		return
	}

	if startBlockNum < 1 {
		utils.SendHTTPResponse(response, http.StatusBadRequest, &types.HttpResponseErr{fmt.Sprintf("requested startId [%d] must be greater than 0", startBlockNum)})
		return
	}

	if startBlockNum > height {
		utils.SendHTTPResponse(response, http.StatusBadRequest, &types.HttpResponseErr{fmt.Sprintf("requested startId [%d] is out of range, height is [%d]", startBlockNum, height)})
		return
	}

	var blocks [][]byte
	var blocksSize int
	for i := startBlockNum; i <= endBlockNum; i++ {
		if i > height {
			break
		}
		block, err := h.ledgerReader.Get(i)
		if err != nil {
			utils.SendHTTPResponse(response, http.StatusInternalServerError, &types.HttpResponseErr{ErrMsg: err.Error()})
			return

		}
		blockBytes, err := proto.Marshal(block)
		if err != nil {
			utils.SendHTTPResponse(response, http.StatusInternalServerError, &types.HttpResponseErr{ErrMsg: err.Error()})
			return

		}

		blocksSize += len(blockBytes)
		if blocksSize > h.maxResponseBytes {
			// no more than maxResponseBytes but at least one block
			if i == startBlockNum {
				blocks = append(blocks, blockBytes)
			}
			break
		}
		blocks = append(blocks, blockBytes)
	}

	sendHTTPMultiPartResponse(response, blocks, startBlockNum)
}

func sendHTTPMultiPartResponse(w http.ResponseWriter, blocks [][]byte, startBlockNum uint64) {
	mw := multipart.NewWriter(w)
	w.Header().Set("Content-Type", mw.FormDataContentType())
	for i, blockBytes := range blocks {
		fw, err := mw.CreateFormFile(
			fmt.Sprintf("block-%d", i),
			fmt.Sprintf("num-%d", startBlockNum+uint64(i)))
		if err != nil {
			utils.SendHTTPResponse(w, http.StatusInternalServerError, &types.HttpResponseErr{ErrMsg: err.Error()})
			return
		}
		if _, err := fw.Write(blockBytes); err != nil {
			utils.SendHTTPResponse(w, http.StatusInternalServerError, &types.HttpResponseErr{ErrMsg: err.Error()})
			return
		}
	}
	if err := mw.Close(); err != nil {
		utils.SendHTTPResponse(w, http.StatusInternalServerError, &types.HttpResponseErr{ErrMsg: err.Error()})
		return
	}
}

type HeightResponse struct {
	Height uint64
}

func (h *catchupHandler) heightRequest(w http.ResponseWriter, r *http.Request) {
	h.lg.Debugf("height request: %s", r.URL)
	height, err := h.ledgerReader.Height()
	if err != nil {
		utils.SendHTTPResponse(w, http.StatusInternalServerError, &types.HttpResponseErr{ErrMsg: err.Error()})
		return
	}

	utils.SendHTTPResponse(w, http.StatusOK, HeightResponse{Height: height})
}
