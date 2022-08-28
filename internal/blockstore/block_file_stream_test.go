// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package blockstore

import (
	"encoding/binary"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestBlockFileStream(t *testing.T) {
	commonSetup := func(s *Store, totalBlocks uint64) []*types.Block {
		var preBlockBaseHash, preBlockHash []byte
		var expectedBlocks []*types.Block
		var err error

		for blockNumber := uint64(1); blockNumber <= totalBlocks; blockNumber++ {
			b := createSampleUserTxBlock(blockNumber, preBlockBaseHash, preBlockHash)

			require.NoError(t, s.AddSkipListLinks(b))
			require.NoError(t, s.Commit(b))

			preBlockBaseHash, err = s.GetBaseHeaderHash(blockNumber)
			require.NoError(t, err)

			preBlockHash, err = s.GetHash(blockNumber)
			require.NoError(t, err)

			expectedBlocks = append(expectedBlocks, proto.Clone(b).(*types.Block))
		}

		return expectedBlocks
	}

	t.Run("happy path", func(t *testing.T) {
		env := newTestEnv(t)
		defer env.cleanup(true)

		totalBlocks := uint64(100)

		expectedBlocks := commonSetup(env.s, totalBlocks)

		tests := []struct {
			name       string
			startBlock uint64
			expected   []*types.Block
		}{
			{
				name:       "start from the 1st block",
				startBlock: 1,
				expected:   expectedBlocks,
			},
			{
				name:       "start from the last block",
				startBlock: 100,
				expected:   expectedBlocks[99:],
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				startLocation, err := env.s.getLocation(tt.startBlock)
				require.NoError(t, err)

				stream, err := newBlockfileStream(env.s.logger, env.s.fileChunksDirPath, startLocation)
				require.NoError(t, err)
				defer stream.close()

				i := 0
				for blockNum := tt.startBlock; blockNum <= totalBlocks; blockNum++ {
					blockWithLocation, err := stream.nextBlockWithLocation()
					require.NoError(t, err)

					location, err := env.s.getLocation(blockNum)
					require.NoError(t, err)

					expectedBlockWithLocation := &blockAndLocation{
						block:            tt.expected[i],
						fileChunkNum:     location.FileChunkNum,
						blockStartOffset: location.Offset,
						blockEndOffset:   location.Offset + location.Length,
					}

					require.True(t, proto.Equal(expectedBlockWithLocation.block, blockWithLocation.block))
					require.Equal(t, expectedBlockWithLocation.fileChunkNum, blockWithLocation.fileChunkNum)
					require.Equal(t, expectedBlockWithLocation.blockStartOffset, blockWithLocation.blockStartOffset)
					require.Equal(t, expectedBlockWithLocation.blockEndOffset, blockWithLocation.blockEndOffset)
					i++
				}
				blockWithLocation, err := stream.nextBlockWithLocation()
				require.NoError(t, err)
				require.Nil(t, blockWithLocation)
			})
		}
	})

	t.Run("unexpected EOF", func(t *testing.T) {
		tests := []struct {
			name              string
			setupForErrorCase func(s *Store, blockNum uint64) error
			expectedErr       string
		}{
			{
				name: "only the length of the block is written but not the block",
				setupForErrorCase: func(s *Store, blockNum uint64) error {
					buf := make([]byte, binary.MaxVarintLen64)
					n := binary.PutUvarint(buf, uint64(50))
					_, err := s.appendBlock(blockNum, buf[:n])
					return err
				},
				expectedErr: ErrUnexpectedEndOfBlockfile.Error(),
			},
			{
				name: "only partial length of the block is written",
				setupForErrorCase: func(s *Store, blockNum uint64) error {
					buf := make([]byte, binary.MaxVarintLen64)
					n := binary.PutUvarint(buf, uint64(555))
					_, err := s.appendBlock(blockNum, buf[:n-1])
					return err
				},
				expectedErr: ErrUnexpectedEndOfBlockfile.Error(),
			},
			{
				name: "the length of the block is written but only a partial block content is appended",
				setupForErrorCase: func(s *Store, blockNum uint64) error {
					buf := make([]byte, binary.MaxVarintLen64)
					n := binary.PutUvarint(buf, uint64(555))
					content := append(buf[:n], []byte("random block")...)
					_, err := s.appendBlock(blockNum, content)
					return err
				},
				expectedErr: ErrUnexpectedEndOfBlockfile.Error(),
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				env := newTestEnv(t)
				defer env.cleanup(true)

				totalBlocks := uint64(3)

				expectedBlocks := commonSetup(env.s, totalBlocks)
				require.NoError(t, tt.setupForErrorCase(env.s, totalBlocks+1))

				startLocation, err := env.s.getLocation(1)
				require.NoError(t, err)

				stream, err := newBlockfileStream(env.s.logger, env.s.fileChunksDirPath, startLocation)
				require.NoError(t, err)
				defer stream.close()

				for blockNum := uint64(1); blockNum <= totalBlocks; blockNum++ {
					blockWithLocation, err := stream.nextBlockWithLocation()
					require.NoError(t, err)

					location, err := env.s.getLocation(blockNum)
					require.NoError(t, err)

					expectedBlockWithLocation := &blockAndLocation{
						block:            expectedBlocks[blockNum-1],
						fileChunkNum:     location.FileChunkNum,
						blockStartOffset: location.Offset,
						blockEndOffset:   location.Offset + location.Length,
					}

					require.True(t, proto.Equal(expectedBlockWithLocation.block, blockWithLocation.block))
					require.Equal(t, expectedBlockWithLocation.fileChunkNum, blockWithLocation.fileChunkNum)
					require.Equal(t, expectedBlockWithLocation.blockStartOffset, blockWithLocation.blockStartOffset)
					require.Equal(t, expectedBlockWithLocation.blockEndOffset, blockWithLocation.blockEndOffset)
				}
				blockWithLocation, err := stream.nextBlockWithLocation()
				require.Nil(t, blockWithLocation)
				require.EqualError(t, err, ErrUnexpectedEndOfBlockfile.Error())
			})
		}
	})
}
