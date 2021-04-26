// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package mock

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"github.com/pkg/errors"
	"github.ibm.com/blockchaindb/server/pkg/constants"
	"github.ibm.com/blockchaindb/server/pkg/types"
	"net/http"
	"net/url"
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

func NewRESTClient(rawurl string) (*Client, error) {
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
		},
	}
	res.httpClient = httpClient

	//TODO using the default client which allows keep-alives exposes a bug in the server. The when stopped, the server
	// does not close (or shutdown) the http server, and keeps the old connections. The client sends requests that need
	// to get to the new server on the old connections, and they get processed by the old http server, in which the
	// handlers lead nowhere. See: https://github.ibm.com/blockchaindb/server/issues/429
	//res.httpClient = http.DefaultClient

	return res, nil
}

func (c *Client) GetDBStatus(e *types.GetDBStatusQueryEnvelope) (*types.ResponseEnvelope, error) {
	resp, err := c.handleGetRequest(
		constants.URLForGetDBStatus(e.Payload.DBName),
		e.Payload.UserID,
		e.Signature,
	)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	res := &types.ResponseEnvelope{}
	err = json.NewDecoder(resp.Body).Decode(res)
	return res, err
}

func (c *Client) GetData(e *types.GetDataQueryEnvelope) (*types.ResponseEnvelope, error) {
	resp, err := c.handleGetRequest(
		constants.URLForGetData(e.Payload.DBName, e.Payload.Key),
		e.Payload.UserID,
		e.Signature,
	)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	res := &types.ResponseEnvelope{}
	err = json.NewDecoder(resp.Body).Decode(res)
	return res, err
}

func (c *Client) GetUser(e *types.GetUserQueryEnvelope) (*types.ResponseEnvelope, error) {
	resp, err := c.handleGetRequest(
		constants.URLForGetUser(e.Payload.TargetUserID),
		e.Payload.UserID,
		e.Signature,
	)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	res := &types.ResponseEnvelope{}
	err = json.NewDecoder(resp.Body).Decode(res)
	return res, err
}

func (c *Client) GetConfig(e *types.GetConfigQueryEnvelope) (*types.ResponseEnvelope, error) {
	resp, err := c.handleGetRequest(
		constants.URLForGetConfig(),
		e.Payload.UserID,
		e.Signature,
	)
	if err != nil {
		return nil, errors.Wrap(err, "error while issuing "+constants.URLForGetConfig())
	}

	defer resp.Body.Close()

	res := &types.ResponseEnvelope{}
	err = json.NewDecoder(resp.Body).Decode(res)
	return res, err
}

func (c *Client) GetNodeConfig(e *types.GetNodeConfigQueryEnvelope) (*types.ResponseEnvelope, error) {
	resp, err := c.handleGetRequest(
		constants.URLForNodeConfigPath(e.Payload.NodeID),
		e.Payload.UserID,
		e.Signature,
	)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	res := &types.ResponseEnvelope{}
	err = json.NewDecoder(resp.Body).Decode(res)
	return res, err
}

func (c *Client) GetHistoricalData(e *types.GetHistoricalDataQueryEnvelope) (*types.ResponseEnvelope, error) {
	resp, err := c.handleGetRequest(
		constants.URLForGetHistoricalData(e.Payload.DBName, e.Payload.Key),
		e.Payload.UserID,
		e.Signature,
	)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	res := &types.ResponseEnvelope{}
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

func (c *Client) SubmitTransaction(urlPath string, tx interface{}) (*http.Response, error) {
	u := c.BaseURL.ResolveReference(
		&url.URL{
			Path: urlPath,
		},
	)

	buf := &bytes.Buffer{}
	if err := json.NewEncoder(buf).Encode(tx); err != nil {
		return nil, err
	}

	req, err := http.NewRequest(http.MethodPost, u.String(), buf)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", c.UserAgent)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		errorRes := &ResponseErr{}
		if err = json.NewDecoder(resp.Body).Decode(errorRes); err != nil {
			return nil, err
		}

		return nil, errors.New(errorRes.Error)
	}

	return resp, err
}
