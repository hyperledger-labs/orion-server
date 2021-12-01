package comm_test

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"sync"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger-labs/orion-server/internal/comm"
	"github.com/hyperledger-labs/orion-server/internal/comm/mocks"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHTTPTransport_CatchupService(t *testing.T) {
	lg, err := logger.New(&logger.Config{
		Level:         "debug",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
	})
	require.NoError(t, err)

	localConfigs, sharedConfig := newTestSetup(t, 1)

	ledger1 := &memLedger{}
	for n := uint64(1); n < 6; n++ {
		ledger1.Append(&types.Block{Header: &types.BlockHeader{BaseHeader: &types.BlockHeaderBase{Number: n}}})
	}
	cl1 := &mocks.ConsensusListener{}
	tr1, _ := comm.NewHTTPTransport(&comm.Config{
		LocalConf:    localConfigs[0],
		Logger:       lg,
		LedgerReader: ledger1,
	})
	require.NotNil(t, tr1)
	err = tr1.SetConsensusListener(cl1)
	require.NoError(t, err)
	err = tr1.SetClusterConfig(sharedConfig)
	require.NoError(t, err)

	err = tr1.Start()
	require.NoError(t, err)
	defer tr1.Close()

	httpClient := &http.Client{
		Transport: &http.Transport{
			DisableKeepAlives: true,
		},
	}

	rawURL := fmt.Sprintf("http://%s:%d", localConfigs[0].Replication.Network.Address, localConfigs[0].Replication.Network.Port)
	baseURL, err := url.Parse(rawURL)
	require.NoError(t, err)

	// Scenario: ask for height
	t.Run("height", func(t *testing.T) {
		url := baseURL.ResolveReference(
			&url.URL{
				Path: comm.GetHeightPath,
			},
		)

		req, err := http.NewRequest(http.MethodGet, url.String(), nil)
		require.NoError(t, err)
		resp, err := httpClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusOK, resp.StatusCode)
		hRes := &comm.HeightResponse{}
		err = json.NewDecoder(resp.Body).Decode(hRes)
		require.NoError(t, err)
		require.Equal(t, &comm.HeightResponse{Height: 5}, hRes)
	})

	// Scenario: end is smaller than height
	// - Ask for blocks [2:4]
	// - Receive blocks [2:4]
	t.Run("blocks [2:4]", func(t *testing.T) {
		url := baseURL.ResolveReference(
			&url.URL{
				Path:     comm.GetBlocksPath,
				RawQuery: "start=2&end=4",
			},
		)

		req, err := http.NewRequest(http.MethodGet, url.String(), nil)
		req.Header.Set("Accept", "multipart/form-data")
		require.NoError(t, err)
		resp, err := httpClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusOK, resp.StatusCode)

		mediatype, params, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
		require.NoError(t, err)
		require.Equal(t, "multipart/form-data", mediatype)
		boundary, ok := params["boundary"]
		require.True(t, ok)

		mr := multipart.NewReader(resp.Body, boundary)
		bNum := uint64(2)
		count := 0
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
			count++
		}
		require.Equal(t, 3, count)
	})

	// Scenario: end is higher than height
	// - Ask for blocks [1:10]
	// - Receive blocks [1:5]
	t.Run("blocks [1:10]", func(t *testing.T) {
		url := baseURL.ResolveReference(
			&url.URL{
				Path:     comm.GetBlocksPath,
				RawQuery: "start=2&end=10",
			},
		)

		req, err := http.NewRequest(http.MethodGet, url.String(), nil)
		req.Header.Set("Accept", "multipart/form-data")
		require.NoError(t, err)
		resp, err := httpClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusOK, resp.StatusCode)

		mediatype, params, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
		require.NoError(t, err)
		require.Equal(t, "multipart/form-data", mediatype)
		boundary, ok := params["boundary"]
		require.True(t, ok)

		mr := multipart.NewReader(resp.Body, boundary)
		count := 0
		bNum := uint64(2)
		for part, errP := mr.NextPart(); errP == nil; part, errP = mr.NextPart() {
			assert.Equal(t, fmt.Sprintf("block-%d", count), part.FormName())
			assert.Equal(t, fmt.Sprintf("num-%d", bNum), part.FileName())

			blockBytes, err := ioutil.ReadAll(part)
			require.NoError(t, err)
			require.NotNil(t, blockBytes)

			block := &types.Block{}
			err = proto.Unmarshal(blockBytes, block)
			require.NoError(t, err)
			assert.Equal(t, bNum, block.Header.BaseHeader.Number)
			bNum++
			count++
		}
		require.Equal(t, 4, count)
	})

	// Scenario: end equal to start
	// - Ask for blocks [3:3]
	// - Receive blocks [3:3]
	t.Run("blocks [3:3]", func(t *testing.T) {
		url := baseURL.ResolveReference(
			&url.URL{
				Path:     comm.GetBlocksPath,
				RawQuery: "start=3&end=3",
			},
		)

		req, err := http.NewRequest(http.MethodGet, url.String(), nil)
		req.Header.Set("Accept", "multipart/form-data")
		require.NoError(t, err)
		resp, err := httpClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusOK, resp.StatusCode)

		mediatype, params, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
		require.NoError(t, err)
		require.Equal(t, "multipart/form-data", mediatype)
		boundary, ok := params["boundary"]
		require.True(t, ok)

		mr := multipart.NewReader(resp.Body, boundary)
		count := 0
		bNum := uint64(3)
		for part, errP := mr.NextPart(); errP == nil; part, errP = mr.NextPart() {
			assert.Equal(t, fmt.Sprintf("block-%d", count), part.FormName())
			assert.Equal(t, fmt.Sprintf("num-%d", bNum), part.FileName())

			blockBytes, err := ioutil.ReadAll(part)
			require.NoError(t, err)
			require.NotNil(t, blockBytes)

			block := &types.Block{}
			err = proto.Unmarshal(blockBytes, block)
			require.NoError(t, err)
			assert.Equal(t, bNum, block.Header.BaseHeader.Number)
			bNum++
			count++
		}
		require.Equal(t, 1, count)
	})

}

// memLedger mocks the block processor, which commits blocks and keeps them in the ledger.
type memLedger struct {
	mutex  sync.Mutex
	ledger []*types.Block
}

func (l *memLedger) Height() (uint64, error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	return uint64(len(l.ledger)), nil
}

func (l *memLedger) Append(block *types.Block) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if h := len(l.ledger); h > 0 {
		if l.ledger[h-1].GetHeader().GetBaseHeader().GetNumber()+1 != block.GetHeader().GetBaseHeader().Number {
			return errors.Errorf("block number [%d] out of sequence, expected [%d]",
				block.GetHeader().GetBaseHeader().Number, l.ledger[h-1].GetHeader().GetBaseHeader().GetNumber()+1)
		}
	} else if block.GetHeader().GetBaseHeader().Number != 1 {
		return errors.Errorf("first block number [%d] must be 1",
			block.GetHeader().GetBaseHeader().Number)
	}

	l.ledger = append(l.ledger, block)
	return nil
}

func (l *memLedger) Get(blockNum uint64) (*types.Block, error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if blockNum-1 >= uint64(len(l.ledger)) {
		return nil, errors.Errorf("block number out of bounds: %d, len: %d", blockNum, len(l.ledger))
	}
	return l.ledger[blockNum-1], nil
}
