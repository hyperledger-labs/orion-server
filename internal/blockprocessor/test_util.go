package blockprocessor

import (
	"github.com/IBM-Blockchain/bcdb-server/internal/worldstate"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
)

func ConstructDBUpdatesForBlock(block *types.Block, bp *BlockProcessor) (map[string]*worldstate.DBUpdates, error) {
	dbsUpdates, _, err := bp.committer.constructDBAndProvenanceEntries(block)
	if err != nil {
		return nil, err
	}

	return dbsUpdates, nil
}
