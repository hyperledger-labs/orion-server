// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package mock

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/hyperledger-labs/orion-server/pkg/constants"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/pkg/errors"
)

type Client struct {
	RawURL     string
	BaseURL    *url.URL
	UserAgent  string
	httpClient *http.Client
}

type ResponseErr struct {
	Error string `json:"error,omitempty"`
}

func NewRESTClient(rawurl string, checkRedirect func(req *http.Request, via []*http.Request) error, tlsConfig *tls.Config) (*Client, error) {
	res := new(Client)
	var err error
	res.RawURL = rawurl
	res.BaseURL, err = url.Parse(rawurl)
	if err != nil {
		return nil, errors.Wrapf(err, "parsing url %s", rawurl)
	}

	httpClient := &http.Client{
		Transport: &http.Transport{
			DisableKeepAlives: true,
			TLSClientConfig:   tlsConfig,
		},

		CheckRedirect: checkRedirect,
	}

	res.httpClient = httpClient

	return res, nil
}

func (c *Client) GetDBStatus(e *types.GetDBStatusQueryEnvelope) (*types.GetDBStatusResponseEnvelope, error) {
	resp, err := c.handleGetRequest(
		constants.URLForGetDBStatus(e.Payload.DbName),
		e.Payload.UserId,
		e.Signature,
	)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	res := &types.GetDBStatusResponseEnvelope{}
	err = json.NewDecoder(resp.Body).Decode(res)
	return res, err
}

func (c *Client) GetDBIndex(e *types.GetDBIndexQueryEnvelope) (*types.GetDBIndexResponseEnvelope, error) {
	resp, err := c.handleGetRequest(
		constants.URLForGetDBIndex(e.Payload.DbName),
		e.Payload.UserId,
		e.Signature,
	)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	res := &types.GetDBIndexResponseEnvelope{}
	err = json.NewDecoder(resp.Body).Decode(res)
	return res, err
}

func (c *Client) GetData(e *types.GetDataQueryEnvelope) (*types.GetDataResponseEnvelope, error) {
	resp, err := c.handleGetRequest(
		constants.URLForGetData(e.Payload.DbName, e.Payload.Key),
		e.Payload.UserId,
		e.Signature,
	)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	res := &types.GetDataResponseEnvelope{}
	err = json.NewDecoder(resp.Body).Decode(res)
	return res, err
}

func (c *Client) GetUser(e *types.GetUserQueryEnvelope) (*types.GetUserResponseEnvelope, error) {
	resp, err := c.handleGetRequest(
		constants.URLForGetUser(e.Payload.TargetUserId),
		e.Payload.UserId,
		e.Signature,
	)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	res := &types.GetUserResponseEnvelope{}
	err = json.NewDecoder(resp.Body).Decode(res)
	return res, err
}

func (c *Client) GetTxProof(e *types.GetTxProofQueryEnvelope) (*types.GetTxProofResponseEnvelope, error) {
	path := constants.URLTxProof(e.Payload.BlockNumber, e.Payload.TxIndex)
	resp, err := c.handleGetRequest(
		path,
		e.Payload.UserId,
		e.Signature,
	)
	if err != nil {
		return nil, errors.Wrapf(err, "error while issuing %s", path)
	}

	defer resp.Body.Close()

	res := &types.GetTxProofResponseEnvelope{}
	err = json.NewDecoder(resp.Body).Decode(res)
	return res, err
}

func (c *Client) GetDataRange(e *types.GetDataQueryEnvelope, startKey, endKey string, limit uint64) (*types.GetDataRangeResponseEnvelope, error) {
	path := constants.URLForGetDataRange(e.Payload.DbName, startKey, endKey, limit)
	resp, err := c.handleGetRequest(
		path,
		e.Payload.UserId,
		e.Signature,
	)
	if err != nil {
		return nil, errors.Wrapf(err, "error while issuing %s", path)
	}

	defer resp.Body.Close()

	res := &types.GetDataRangeResponseEnvelope{}
	err = json.NewDecoder(resp.Body).Decode(res)
	return res, err
}

func (c *Client) GetLastConfigBlockStatus(e *types.GeConfigBlockQueryEnvelope) (*types.GetConfigBlockResponseEnvelope, error) {
	resp, err := c.handleGetRequest(
		constants.GetLastConfigBlock,
		e.Payload.UserId,
		e.Signature,
	)
	if err != nil {
		return nil, errors.Wrap(err, "error while issuing "+constants.GetLastConfigBlock)
	}

	defer resp.Body.Close()

	res := &types.GetConfigBlockResponseEnvelope{}
	err = json.NewDecoder(resp.Body).Decode(res)
	return res, err
}

func (c *Client) GetLastBlock(e *types.GetLastBlockQueryEnvelope) (*types.GetBlockResponseEnvelope, error) {
	resp, err := c.handleGetRequest(
		constants.GetLastBlockHeader,
		e.Payload.UserId,
		e.Signature,
	)
	if err != nil {
		return nil, errors.Wrap(err, "error while issuing "+constants.GetLastBlockHeader)
	}

	defer resp.Body.Close()

	res := &types.GetBlockResponseEnvelope{}
	err = json.NewDecoder(resp.Body).Decode(res)
	return res, err
}

func (c *Client) GetLedgerPath(e *types.GetLedgerPathQueryEnvelope) (*types.GetLedgerPathResponseEnvelope, error) {
	path := constants.URLForLedgerPath(e.Payload.StartBlockNumber, e.Payload.EndBlockNumber)

	resp, err := c.handleGetRequest(
		path,
		e.Payload.UserId,
		e.Signature,
	)
	if err != nil {
		return nil, errors.Wrap(err, "error while issuing "+path)
	}

	defer resp.Body.Close()

	res := &types.GetLedgerPathResponseEnvelope{}
	err = json.NewDecoder(resp.Body).Decode(res)
	return res, err
}

func (c *Client) GetTxReceipt(e *types.GetTxReceiptQueryEnvelope) (*types.TxReceiptResponseEnvelope, error) {
	path := constants.URLForGetTransactionReceipt(e.Payload.TxId)

	resp, err := c.handleGetRequest(
		path,
		e.Payload.UserId,
		e.Signature,
	)
	if err != nil {
		return nil, errors.Wrap(err, "error while issuing "+path)
	}

	defer resp.Body.Close()

	res := &types.TxReceiptResponseEnvelope{}
	err = json.NewDecoder(resp.Body).Decode(res)
	return res, err
}

func (c *Client) GetBlockHeader(e *types.GetBlockQueryEnvelope, forceParam bool) (*types.GetBlockResponseEnvelope, error) {
	path := constants.LedgerEndpoint + fmt.Sprintf("block/%d", e.Payload.BlockNumber)
	if forceParam {
		path = constants.LedgerEndpoint + fmt.Sprintf("block/%d?augmented=%t", e.Payload.BlockNumber, false)
	}

	resp, err := c.handleGetRequest(
		path,
		e.Payload.UserId,
		e.Signature,
	)
	if err != nil {
		return nil, errors.Wrap(err, "error while issuing "+path)
	}

	defer resp.Body.Close()

	res := &types.GetBlockResponseEnvelope{}
	err = json.NewDecoder(resp.Body).Decode(res)
	return res, err
}

func (c *Client) GetAugmentedBlockHeader(e *types.GetBlockQueryEnvelope) (*types.GetAugmentedBlockHeaderResponseEnvelope, error) {
	path := constants.URLForLedgerBlock(e.Payload.BlockNumber, true)
	resp, err := c.handleGetRequest(
		path,
		e.Payload.UserId,
		e.Signature,
	)
	if err != nil {
		return nil, errors.Wrap(err, "error while issuing "+path)
	}

	defer resp.Body.Close()

	res := &types.GetAugmentedBlockHeaderResponseEnvelope{}
	err = json.NewDecoder(resp.Body).Decode(res)
	return res, err
}

func (c *Client) GetClusterStatus(e *types.GetClusterStatusQueryEnvelope) (*types.GetClusterStatusResponseEnvelope, error) {
	resp, err := c.handleGetRequest(
		constants.GetClusterStatus,
		e.Payload.UserId,
		e.Signature,
	)
	if err != nil {
		return nil, errors.Wrap(err, "error while issuing "+constants.GetClusterStatus)
	}

	defer resp.Body.Close()

	res := &types.GetClusterStatusResponseEnvelope{}
	err = json.NewDecoder(resp.Body).Decode(res)
	return res, err
}

func (c *Client) GetConfig(e *types.GetConfigQueryEnvelope) (*types.GetConfigResponseEnvelope, error) {
	resp, err := c.handleGetRequest(
		constants.URLForGetConfig(),
		e.Payload.UserId,
		e.Signature,
	)
	if err != nil {
		return nil, errors.Wrap(err, "error while issuing "+constants.URLForGetConfig())
	}

	defer resp.Body.Close()

	res := &types.GetConfigResponseEnvelope{}
	err = json.NewDecoder(resp.Body).Decode(res)
	return res, err
}

func (c *Client) GetNodeConfig(e *types.GetNodeConfigQueryEnvelope) (*types.GetNodeConfigResponseEnvelope, error) {
	resp, err := c.handleGetRequest(
		constants.URLForNodeConfigPath(e.Payload.NodeId),
		e.Payload.UserId,
		e.Signature,
	)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	res := &types.GetNodeConfigResponseEnvelope{}
	err = json.NewDecoder(resp.Body).Decode(res)
	return res, err
}

func (c *Client) GetHistoricalData(urlPath string, e *types.GetHistoricalDataQueryEnvelope) (*types.GetHistoricalDataResponseEnvelope, error) {
	resp, err := c.handleGetRequest(
		urlPath,
		e.Payload.UserId,
		e.Signature,
	)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	res := &types.GetHistoricalDataResponseEnvelope{}
	err = json.NewDecoder(resp.Body).Decode(res)
	return res, err
}

func (c *Client) GetDataReadByUser(urlPath string, e *types.GetDataReadByQueryEnvelope) (*types.GetDataProvenanceResponseEnvelope, error) {
	resp, err := c.handleGetRequest(
		urlPath,
		e.Payload.UserId,
		e.Signature,
	)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	res := &types.GetDataProvenanceResponseEnvelope{}
	err = json.NewDecoder(resp.Body).Decode(res)
	return res, err
}

func (c *Client) GetDataWrittenByUser(urlPath string, e *types.GetDataWrittenByQueryEnvelope) (*types.GetDataProvenanceResponseEnvelope, error) {
	resp, err := c.handleGetRequest(
		urlPath,
		e.Payload.UserId,
		e.Signature,
	)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	res := &types.GetDataProvenanceResponseEnvelope{}
	err = json.NewDecoder(resp.Body).Decode(res)
	return res, err
}

func (c *Client) GetDataDeletedByUser(urlPath string, e *types.GetDataDeletedByQueryEnvelope) (*types.GetDataProvenanceResponseEnvelope, error) {
	resp, err := c.handleGetRequest(
		urlPath,
		e.Payload.UserId,
		e.Signature,
	)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	res := &types.GetDataProvenanceResponseEnvelope{}
	err = json.NewDecoder(resp.Body).Decode(res)
	return res, err
}

func (c *Client) GetDataReaders(urlPath string, e *types.GetDataReadersQueryEnvelope) (*types.GetDataReadersResponseEnvelope, error) {
	resp, err := c.handleGetRequest(
		urlPath,
		e.Payload.UserId,
		e.Signature,
	)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	res := &types.GetDataReadersResponseEnvelope{}
	err = json.NewDecoder(resp.Body).Decode(res)
	return res, err
}

func (c *Client) GetDataWriters(urlPath string, e *types.GetDataWritersQueryEnvelope) (*types.GetDataWritersResponseEnvelope, error) {
	resp, err := c.handleGetRequest(
		urlPath,
		e.Payload.UserId,
		e.Signature,
	)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	res := &types.GetDataWritersResponseEnvelope{}
	err = json.NewDecoder(resp.Body).Decode(res)
	return res, err
}

func (c *Client) GetTxIDsSubmitedBy(urlPath string, e *types.GetTxIDsSubmittedByQueryEnvelope) (*types.GetTxIDsSubmittedByResponseEnvelope, error) {
	resp, err := c.handleGetRequest(
		urlPath,
		e.Payload.UserId,
		e.Signature,
	)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	res := &types.GetTxIDsSubmittedByResponseEnvelope{}
	err = json.NewDecoder(resp.Body).Decode(res)
	return res, err
}

func (c *Client) ExecuteJSONQuery(urlPath string, e *types.DataJSONQuery, signature []byte) (*types.DataQueryResponseEnvelope, error) {
	marshaledJSONQuery, err := json.Marshal(e.Query)
	if err != nil {
		return nil, errors.WithMessage(err, "check whether the query string passed is in JSON format")
	}

	resp, err := c.handlePostRequest(
		urlPath,
		e.UserId,
		marshaledJSONQuery,
		signature,
	)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	res := &types.DataQueryResponseEnvelope{}
	err = json.NewDecoder(resp.Body).Decode(res)
	return res, err
}

func (c *Client) handlePostRequest(urlPath string, userID string, postData, signature []byte) (*http.Response, error) {
	parsedURL, err := url.Parse(urlPath)
	if err != nil {
		return nil, err
	}

	u := c.BaseURL.ResolveReference(parsedURL)
	req, err := http.NewRequest(http.MethodPost, u.String(), bytes.NewReader(postData))
	if err != nil {
		return nil, err
	}

	return c.handleGetPostRequest(req, userID, signature)
}

func (c *Client) handleGetRequest(urlPath, userID string, signature []byte) (*http.Response, error) {
	parsedURL, err := url.Parse(urlPath)
	if err != nil {
		return nil, err
	}

	u := c.BaseURL.ResolveReference(parsedURL)
	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}

	return c.handleGetPostRequest(req, userID, signature)

}

func (c *Client) handleGetPostRequest(req *http.Request, userID string, signature []byte) (*http.Response, error) {
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", c.UserAgent)
	req.Header.Set(constants.UserHeader, userID)
	req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(signature))

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		ct := resp.Header.Get("Content-Type")
		if ct != "application/json" {
			bodyBytes, err := io.ReadAll(resp.Body)
			if err != nil {
				return nil, errors.Wrapf(err, "cannot read response; status: %d", resp.StatusCode)
			}
			bodyString := string(bodyBytes)
			return nil, errors.Errorf("status: %d; body: %s", resp.StatusCode, bodyString)
		}
		errorRes := new(ResponseErr)
		err = json.NewDecoder(resp.Body).Decode(errorRes)
		if err != nil {
			return nil, err
		}
		return nil, errors.New(errorRes.Error)
	}

	return resp, nil
}

// SubmitTransaction to the server.
// If the returned error is nil, the response body must be closed after consuming it.
func (c *Client) SubmitTransaction(urlPath string, tx interface{}, serverTimeout time.Duration) (*http.Response, error) {
	u := c.BaseURL.ResolveReference(
		&url.URL{
			Path: urlPath,
		},
	)

	buf := &bytes.Buffer{}
	if err := json.NewEncoder(buf).Encode(tx); err != nil {
		return nil, err
	}

	ctx := context.Background()
	if serverTimeout > 0 {
		contextTimeout := serverTimeout + time.Second
		var cancelFnc context.CancelFunc
		ctx, cancelFnc = context.WithTimeout(context.Background(), contextTimeout)
		defer cancelFnc()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), buf)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", c.UserAgent)
	if serverTimeout > 0 {
		req.Header.Set(constants.TimeoutHeader, serverTimeout.String())
	}

	resp, err := c.httpClient.Do(req)
	if _, ok := err.(net.Error); ok {
		if err.(net.Error).Timeout() {
			err = errors.WithMessage(err, "timeout error")
		}
	}

	return resp, err
}
