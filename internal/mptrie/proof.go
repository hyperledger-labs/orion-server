package mptrie

import (
	"github.com/hyperledger-labs/orion-server/pkg/state"
	"github.com/hyperledger-labs/orion-server/pkg/types"
)

// GetProof calculates proof (path) from node contains value to root node in trie
// for given key and delete flag, i.e. if value was deleted, but delete flag id false,
// no proof will be calculated
func (t *MPTrie) GetProof(key []byte, isDeleted bool) (*state.Proof, error) {
	hexKey := convertByteToHex(key)
	path, node, err := t.getPath(hexKey)

	if err != nil {
		return nil, err
	}

	if node == nil {
		return nil, nil
	}

	if node.isDeleted() != isDeleted {
		return nil, nil
	}

	resPath := make([]*types.MPTrieProofElement, 0)
	for i := len(path) - 1; i >= 0; i-- {
		node := path[i]
		resPath = append(resPath, &types.MPTrieProofElement{
			Hashes: node.bytes(),
		})
	}

	return state.NewProof(resPath), nil
}
