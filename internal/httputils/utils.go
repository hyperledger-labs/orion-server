// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package httputils

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"

	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
	"github.com/golang/protobuf/proto"
)

const MultiPartFormData = "multipart/form-data"

func MarshalOrPanic(m proto.Message) []byte {
	bytes, err := proto.Marshal(m)
	if err != nil {
		panic(err)
	}

	return bytes
}

// SendHTTPResponse writes HTTP response back including HTTP code number and encode payload
func SendHTTPResponse(w http.ResponseWriter, code int, payload interface{}) {
	response, _ := json.Marshal(payload)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if _, err := w.Write(response); err != nil {
		log.Printf("Warning: failed to write response [%v] to the response writer\n", w)
	}
}

func GetStartAndEndBlockNum(params map[string]string) (uint64, uint64, error) {
	startBlockNum, err := GetUintParam("startId", params)
	if err != nil {
		return 0, 0, err
	}

	endBlockNum, err := GetUintParam("endId", params)
	if err != nil {
		return 0, 0, err
	}

	if endBlockNum < startBlockNum {
		return 0, 0, &types.HttpResponseErr{
			ErrMsg: fmt.Sprintf("query error: startId=%d > endId=%d", startBlockNum, endBlockNum),
		}
	}

	return startBlockNum, endBlockNum, nil
}

func GetUintParam(key string, params map[string]string) (uint64, *types.HttpResponseErr) {
	valStr, ok := params[key]
	if !ok {
		return 0, &types.HttpResponseErr{
			ErrMsg: "query error - bad or missing literal: " + key,
		}
	}
	val, err := strconv.ParseUint(valStr, 10, 64)
	if err != nil {
		return 0, &types.HttpResponseErr{
			ErrMsg: "query error - bad or missing literal: " + key + " " + err.Error(),
		}
	}
	return val, nil
}

func GetBlockNumAndTxIndex(params map[string]string) (uint64, uint64, error) {
	blockNum, err := GetUintParam("blockId", params)
	if err != nil {
		return 0, 0, err
	}

	txIndex, err := GetUintParam("idx", params)
	if err != nil {
		return 0, 0, err
	}

	return blockNum, txIndex, nil
}

func GetBlockNum(params map[string]string) (uint64, error) {
	blockNum, err := GetUintParam("blockId", params)
	if err != nil {
		return 0, err
	}

	return blockNum, nil
}

func GetVersion(params map[string]string) (*types.Version, error) {
	if _, ok := params["blknum"]; !ok {
		return nil, nil
	}

	blockNum, err := GetUintParam("blknum", params)
	if err != nil {
		return nil, err
	}

	txNum, err := GetUintParam("txnum", params)
	if err != nil {
		return nil, err
	}

	return &types.Version{
		BlockNum: blockNum,
		TxNum:    txNum,
	}, nil
}

