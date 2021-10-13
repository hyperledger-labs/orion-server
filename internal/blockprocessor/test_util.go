package blockprocessor

import (
	"github.com/hyperledger-labs/orion-server/internal/worldstate"
	"github.com/hyperledger-labs/orion-server/pkg/types"
)

func ConstructDBUpdatesForBlock(block *types.Block, bp *BlockProcessor) (map[string]*worldstate.DBUpdates, error) {
	dbsUpdates, _, err := bp.committer.constructDBAndProvenanceEntries(block)
	if err != nil {
		return nil, err
	}

	return dbsUpdates, nil
}
