// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package comm

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"mime"
	"mime/multipart"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger-labs/orion-server/internal/utils"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/pkg/errors"
)

var RetryIntervalMin = 10 * time.Millisecond
var RetryIntervalMax = 10 * time.Second

type catchUpClient struct {
	httpClient *http.Client
	logger     *logger.SugarLogger
	tlsConfig  *tls.Config

	mutex   sync.Mutex
	members map[uint64]*url.URL
}

func NewCatchUpClient(lg *logger.SugarLogger, tlsConfig *tls.Config) *catchUpClient {
	c := &catchUpClient{
		httpClient: newHTTPClient(tlsConfig),
		tlsConfig:  tlsConfig,
		logger:     lg,
		members:    make(map[uint64]*url.URL),
	}
	return c
}

// UpdateMembers updates the peer member list, must not include the self RaftID.
func (c *catchUpClient) UpdateMembers(memberList []*types.PeerConfig) error {
	members := make(map[uint64]*url.URL)

	scheme := "http"
	if c.tlsConfig != nil {
		scheme = "https"
	}

	for _, m := range memberList {
		rawURL := fmt.Sprintf("%s://%s:%d", scheme, m.PeerHost, m.PeerPort)
		baseURL, err := url.Parse(rawURL)
		if err != nil {
			return errors.Wrapf(err, "failed to convert PeerConfig [%+v] to url", m)
		}
		if m.RaftId == 0 {
			return errors.Errorf("raft ID cannot be 0, PeerConfig: [%+v]", m)
		}
		members[m.RaftId] = baseURL
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.members = members

	return nil
}

func (c *catchUpClient) PullBlocks(ctx context.Context, start, end uint64, leaderHint uint64) ([]*types.Block, error) {
	curRetryInterval := RetryIntervalMin

	var rounds uint64
	for {
		var memberIDs []uint64
		if leaderHint != 0 {
			memberIDs = append(memberIDs, leaderHint)
		}
		memberIDs = append(memberIDs, c.memberIDs()...)
		c.logger.Debugf("going to try getting blocks [%d,%d] from members: %v, in that order", start, end, memberIDs)

		for _, id := range memberIDs {
			select {
			case <-ctx.Done():
				c.logger.Infof("PulledBlocks canceled: %s", ctx.Err())
				return nil, errors.WithMessage(ctx.Err(), "PullBlocks canceled")
			default:
				blocks, err := c.GetBlocks(ctx, id, start, end)
				if err != nil {
					c.logger.Debugf("failed to get blocks from member [%d], error: %s", id, err)
					continue
				}

				last := blocks[len(blocks)-1].Header.BaseHeader.Number
				c.logger.Infof("Pulled blocks [%d,%d] from member [%d]", start, last, id)
				return blocks, nil
			}
		}

		rounds++
		c.logger.Debugf("Round %d failed to get blocks [%d,%d] from members, will try again in %s", rounds, start, end, curRetryInterval)
		if leaderHint != 0 {
			c.logger.Debugf("Hinted leader [%d] is not responsive, hint will not be used again", leaderHint)
			leaderHint = 0
		}

		select {
		case <-ctx.Done():
			return nil, errors.WithMessage(ctx.Err(), "PullBlocks canceled")
		case <-time.After(curRetryInterval):
			// double the retry interval up to a max, to implement exponential back-off
			curRetryInterval = 2 * curRetryInterval
			if curRetryInterval > RetryIntervalMax {
				curRetryInterval = RetryIntervalMax
				c.logger.Debugf("Retry interval max reached: %v", curRetryInterval)
			}
		}
	}
}

func (c *catchUpClient) memberIDs() []uint64 {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	var ids []uint64
	for k, _ := range c.members {
		ids = append(ids, k)
	}

	rand.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })

	return ids
}

func (c *catchUpClient) getMemberURL(target uint64) *url.URL {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.members[target]
}

func (c *catchUpClient) GetBlocks(ctx context.Context, targetID, start, end uint64) ([]*types.Block, error) {
	baseURL := c.getMemberURL(targetID)
	if baseURL == nil {
		return nil, errors.Errorf("target ID [%d] not found", targetID)
	}

	q := make(url.Values)
	q.Add("start", strconv.FormatUint(start, 10))
	q.Add("end", strconv.FormatUint(end, 10))
	url := baseURL.ResolveReference(
		&url.URL{
			Path:     GetBlocksPath,
			RawQuery: q.Encode(),
		},
	)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Accept", utils.MultiPartFormData)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		eRes := &types.HttpResponseErr{}
		if err = json.NewDecoder(resp.Body).Decode(eRes); err != nil {
			return nil, err
		}
		return nil, eRes
	}

	return multipartResponseToBlocks(c.logger, resp)
}

func (c *catchUpClient) GetHeight(ctx context.Context, targetID uint64) (uint64, error) {
	baseURL := c.getMemberURL(targetID)
	if baseURL == nil {
		return 0, errors.Errorf("target ID [%d] not found", targetID)
	}

	url := baseURL.ResolveReference(&url.URL{Path: GetHeightPath})
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url.String(), nil)
	if err != nil {
		return 0, err
	}
	req.Header.Add("Accept", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		eRes := &types.HttpResponseErr{}
		if err = json.NewDecoder(resp.Body).Decode(eRes); err != nil {
			return 0, err
		}
		return 0, eRes
	}

	hRes := &HeightResponse{}
	if err = json.NewDecoder(resp.Body).Decode(hRes); err != nil {
		return 0, err
	}

	return hRes.Height, nil
}

func newHTTPClient(tlsConfig *tls.Config) *http.Client {
	//TODO expose some transport parameters
	dialer := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	httpClient := &http.Client{
		Transport: &http.Transport{
			Proxy:                 http.ProxyFromEnvironment,
			DialContext:           dialer.DialContext,
			TLSClientConfig:       tlsConfig,
			TLSHandshakeTimeout:   10 * time.Second,
			MaxIdleConns:          100,
			MaxIdleConnsPerHost:   100,
			IdleConnTimeout:       90 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			ForceAttemptHTTP2:     true,
		},
	}
	return httpClient
}

func multipartResponseToBlocks(lg *logger.SugarLogger, resp *http.Response) ([]*types.Block, error) {
	mediaType, params, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse Content-Type header")
	}
	if mediaType != utils.MultiPartFormData {
		return nil, errors.Errorf("unexpected Content-Type: [%s], expected %s", mediaType, utils.MultiPartFormData)
	}
	boundary, ok := params["boundary"]
	if !ok {
		return nil, errors.Errorf("%s boundary not found", utils.MultiPartFormData)
	}

	mr := multipart.NewReader(resp.Body, boundary)
	var blocks []*types.Block
	var totalBytes int
	for part, errP := mr.NextPart(); errP == nil; part, errP = mr.NextPart() {
		lg.Debugf("reading part: %s, block: %s", part.FormName(), part.FileName())
		blockBytes, err := ioutil.ReadAll(part)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to read block, part name: %s, file name: %s", part.FormName(), part.FileName())
		}

		block := &types.Block{}
		if err := proto.Unmarshal(blockBytes, block); err != nil {
			return nil, errors.Wrapf(err, "failed to unmarshal block, part name: %s, file name: %s", part.FormName(), part.FileName())
		}
		blocks = append(blocks, block)
		totalBytes += len(blockBytes)
	}

	lg.Debugf("num blocks: %d, total-bytes: %d", len(blocks), totalBytes)
	if len(blocks) == 0 {
		return nil, errors.Errorf("empty %s, no blocks found", utils.MultiPartFormData)
	}

	return blocks, nil
}
