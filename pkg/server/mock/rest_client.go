package mock

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/pkg/errors"
	"github.ibm.com/blockchaindb/server/api"
)

const UserHeader = "X-BLockchain-DB-User-ID"
const SignatureHeader = "X-BLockchain-DB-Signature"

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
	res.httpClient = http.DefaultClient
	return res, nil
}

func (c *Client) GetStatus(ctx context.Context, in *api.GetStatusQueryEnvelope) (*api.GetStatusResponseEnvelope, error) {
	rel := &url.URL{Path: fmt.Sprintf("/db/%s", in.Payload.DBName)}
	u := c.BaseURL.ResolveReference(rel)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", c.UserAgent)
	req.Header.Set(UserHeader, in.Payload.UserID)
	req.Header.Set(SignatureHeader, base64.StdEncoding.EncodeToString(in.Signature))

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		errorRes := new(ResponseErr)
		err = json.NewDecoder(resp.Body).Decode(errorRes)
		if err != nil {
			return nil, err
		}
		return nil, errors.New(errorRes.Error)
	}
	res := &api.GetStatusResponseEnvelope{}
	err = json.NewDecoder(resp.Body).Decode(res)
	return res, err
}

func (c *Client) GetState(ctx context.Context, in *api.GetStateQueryEnvelope) (*api.GetStateResponseEnvelope, error) {
	rel := &url.URL{Path: fmt.Sprintf("/db/%s/state/%s", in.Payload.DBName, in.Payload.Key)}
	u := c.BaseURL.ResolveReference(rel)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", c.UserAgent)
	req.Header.Set(UserHeader, in.Payload.UserID)
	req.Header.Set(SignatureHeader, base64.StdEncoding.EncodeToString(in.Signature))

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		errorRes := new(ResponseErr)
		err = json.NewDecoder(resp.Body).Decode(errorRes)
		if err != nil {
			return nil, err
		}
		return nil, errors.New(errorRes.Error)
	}
	res := &api.GetStateResponseEnvelope{}
	err = json.NewDecoder(resp.Body).Decode(res)
	return res, err
}

func (c *Client) SubmitTransaction(ctx context.Context, in *api.TransactionEnvelope) (*api.ResponseEnvelope, error) {
	rel := &url.URL{Path: "/tx"}
	u := c.BaseURL.ResolveReference(rel)

	var buf io.ReadWriter
	if in != nil {
		buf = new(bytes.Buffer)
		err := json.NewEncoder(buf).Encode(in)
		if err != nil {
			return nil, err
		}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), buf)
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
		errorRes := new(ResponseErr)
		err = json.NewDecoder(resp.Body).Decode(errorRes)
		if err != nil {
			return nil, err
		}
		return nil, errors.New(errorRes.Error)
	}
	res := new(api.ResponseEnvelope)
	err = json.NewDecoder(resp.Body).Decode(res)
	if res.Data == nil {
		return nil, nil
	}
	return res, err
}
