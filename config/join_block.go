package config

import (
	"io/ioutil"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger-labs/orion-server/internal/utils"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/pkg/errors"
)

func readJoinBlock(joinBlockFile string) (*types.Block, error) {
	if joinBlockFile == "" {
		return nil, errors.New("path to the join block file is empty")
	}

	blockBytes, err := ioutil.ReadFile(joinBlockFile)
	if err != nil {
		return nil, errors.Wrap(err, "error reading join block file")
	}

	block := &types.Block{}
	err = proto.Unmarshal(blockBytes, block)
	if err != nil {
		return nil, errors.Wrapf(err, "error unmarshalling join block, file: %s", joinBlockFile)
	}

	if block.GetHeader() == nil {
		return nil, errors.Errorf("join block header is empty, file: %s", joinBlockFile)
	}

	if block.GetHeader().GetBaseHeader() == nil {
		return nil, errors.Errorf("join block base header is empty, file: %s", joinBlockFile)
	}

	if block.GetHeader().GetBaseHeader().GetNumber() == 0 {
		return nil, errors.Errorf("join block number is 0, file: %s", joinBlockFile)
	}

	if !utils.IsConfigBlock(block) {
		return nil, errors.Errorf("join block is not a config block, file: %s", joinBlockFile)
	}

	configBlock := block.GetPayload().(*types.Block_ConfigTxEnvelope)

	configTx := configBlock.ConfigTxEnvelope.GetPayload()
	if configTx == nil {
		return nil, errors.Errorf("join block config-tx envelope is empty or nil, file: %s", joinBlockFile)
	}

	if configTx.NewConfig == nil {
		return nil, errors.Errorf("join block new-config is nil, file: %s", joinBlockFile)
	}

	return block, nil
}
