package config

import (
	"fmt"
	"io/ioutil"
	"path"
	"testing"

	"github.com/hyperledger-labs/orion-server/internal/utils"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestJoinBlock(t *testing.T) {
	dir := t.TempDir()

	t.Run("empty-join-block-path", func(t *testing.T) {
		joinBlock, err := readJoinBlock("")
		require.EqualError(t, err, "path to the join block file is empty")
		require.Nil(t, joinBlock)
	})

	t.Run("missing-join-block", func(t *testing.T) {
		joinBlockPath := path.Join(dir, "bogus-cluster-join.block")
		joinBlock, err := readJoinBlock(joinBlockPath)
		expectedErr := fmt.Sprintf("error reading join block file: open %s: no such file or directory", joinBlockPath)
		require.EqualError(t, err, expectedErr)
		require.Nil(t, joinBlock)
	})

	t.Run("unmarshal-error", func(t *testing.T) {
		joinBlockPath := path.Join(dir, "bad-cluster-join.block")

		ioutil.WriteFile(joinBlockPath, []byte{1, 2, 3, 4}, 0666)
		joinBlock, err := readJoinBlock(joinBlockPath)
		expectedErr := fmt.Sprintf("error unmarshalling join block, file: %s: proto:", joinBlockPath)
		require.Contains(t, err.Error(), expectedErr)
		require.Nil(t, joinBlock)
	})

	t.Run("bad block: no header", func(t *testing.T) {
		joinBlockPath := path.Join(dir, "bad-cluster-join-1.block")
		blockBytes := utils.MarshalOrPanic(&types.Block{})
		ioutil.WriteFile(joinBlockPath, blockBytes, 0666)
		joinBlock, err := readJoinBlock(joinBlockPath)
		expectedErr := fmt.Sprintf("join block header is empty, file: %s", joinBlockPath)
		require.EqualError(t, err, expectedErr)
		require.Nil(t, joinBlock)
	})

	t.Run("bad block: no base header", func(t *testing.T) {
		joinBlockPath := path.Join(dir, "bad-cluster-join-2.block")
		blockBytes := utils.MarshalOrPanic(&types.Block{
			Header: &types.BlockHeader{},
		})
		ioutil.WriteFile(joinBlockPath, blockBytes, 0666)
		joinBlock, err := readJoinBlock(joinBlockPath)
		expectedErr := fmt.Sprintf("join block base header is empty, file: %s", joinBlockPath)
		require.EqualError(t, err, expectedErr)
		require.Nil(t, joinBlock)
	})

	t.Run("bad block: no number", func(t *testing.T) {
		joinBlockPath := path.Join(dir, "bad-cluster-join-3.block")
		blockBytes := utils.MarshalOrPanic(&types.Block{
			Header: &types.BlockHeader{
				BaseHeader: &types.BlockHeaderBase{},
			},
		})
		ioutil.WriteFile(joinBlockPath, blockBytes, 0666)
		joinBlock, err := readJoinBlock(joinBlockPath)
		expectedErr := fmt.Sprintf("join block number is 0, file: %s", joinBlockPath)
		require.EqualError(t, err, expectedErr)
		require.Nil(t, joinBlock)
	})

	t.Run("bad block: not a config block", func(t *testing.T) {
		joinBlockPath := path.Join(dir, "bad-cluster-join-4.block")
		blockBytes := utils.MarshalOrPanic(&types.Block{
			Header: &types.BlockHeader{
				BaseHeader: &types.BlockHeaderBase{Number: 10},
			},
		})
		ioutil.WriteFile(joinBlockPath, blockBytes, 0666)
		joinBlock, err := readJoinBlock(joinBlockPath)
		expectedErr := fmt.Sprintf("join block is not a config block, file: %s", joinBlockPath)
		require.EqualError(t, err, expectedErr)
		require.Nil(t, joinBlock)
	})

	t.Run("bad block: no config-tx", func(t *testing.T) {
		joinBlockPath := path.Join(dir, "bad-cluster-join-5.block")
		block := &types.Block{
			Header: &types.BlockHeader{
				BaseHeader: &types.BlockHeaderBase{Number: 10},
			},
			Payload: &types.Block_ConfigTxEnvelope{},
		}
		blockBytes := utils.MarshalOrPanic(block)
		ioutil.WriteFile(joinBlockPath, blockBytes, 0666)
		joinBlock, err := readJoinBlock(joinBlockPath)
		expectedErr := fmt.Sprintf("join block config-tx envelope is empty or nil, file: %s", joinBlockPath)
		require.EqualError(t, err, expectedErr)
		require.Nil(t, joinBlock)

		block.Payload.(*types.Block_ConfigTxEnvelope).ConfigTxEnvelope = &types.ConfigTxEnvelope{}
		blockBytes = utils.MarshalOrPanic(block)
		ioutil.WriteFile(joinBlockPath, blockBytes, 0666)
		joinBlock, err = readJoinBlock(joinBlockPath)
		require.EqualError(t, err, expectedErr)
		require.Nil(t, joinBlock)
	})

	t.Run("bad block: no new-config", func(t *testing.T) {
		joinBlockPath := path.Join(dir, "bad-cluster-join-6.block")
		blockBytes := utils.MarshalOrPanic(&types.Block{
			Header: &types.BlockHeader{
				BaseHeader: &types.BlockHeaderBase{Number: 10},
			},
			Payload: &types.Block_ConfigTxEnvelope{
				ConfigTxEnvelope: &types.ConfigTxEnvelope{
					Payload: &types.ConfigTx{},
				}},
		})
		ioutil.WriteFile(joinBlockPath, blockBytes, 0666)
		joinBlock, err := readJoinBlock(joinBlockPath)
		expectedErr := fmt.Sprintf("join block new-config is nil, file: %s", joinBlockPath)
		require.EqualError(t, err, expectedErr)
		require.Nil(t, joinBlock)
	})

	t.Run("success", func(t *testing.T) {
		joinBlockPath := path.Join(dir, "cluster-join.block")
		blockBytes := utils.MarshalOrPanic(&types.Block{
			Header: &types.BlockHeader{
				BaseHeader: &types.BlockHeaderBase{Number: 10},
			},
			Payload: &types.Block_ConfigTxEnvelope{
				ConfigTxEnvelope: &types.ConfigTxEnvelope{
					Payload: &types.ConfigTx{
						NewConfig: &types.ClusterConfig{},
					},
				}},
		})
		ioutil.WriteFile(joinBlockPath, blockBytes, 0666)
		joinBlock, err := readJoinBlock(joinBlockPath)
		require.NoError(t, err)
		require.NotNil(t, joinBlock)
	})
}
