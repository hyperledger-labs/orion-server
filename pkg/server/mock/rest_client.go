// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package mock

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
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

func (c *Client) GetLastBlockStatus(e *types.GetBlockQueryEnvelope) (*types.GetBlockResponseEnvelope, error) {
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

func (c *Client) GetHistoricalData(e *types.GetHistoricalDataQueryEnvelope) (*types.GetHistoricalDataResponseEnvelope, error) {
	resp, err := c.handleGetRequest(
		constants.URLForGetHistoricalData(e.Payload.DbName, e.Payload.Key),
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

func (c *Client) handleGetRequest(urlPath, userID string, signature []byte) (*http.Response, error) {
	u := c.BaseURL.ResolveReference(
		&url.URL{
			Path: urlPath,
		},
	)
	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}

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
