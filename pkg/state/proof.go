package state

import (
	"bytes"

	"github.com/hyperledger-labs/orion-server/pkg/crypto"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/pkg/errors"
)

var KeyDeleteMarkerBytes = []byte{1}

type Proof struct {
	// For each node in trie path, it contains bytes of all node fields and []byte{1} in case of deleted flag true
	// path is from rom node contains value to root node
	// Exactly same byte slices used to calculate node hash.
	path []*types.MPTrieProofElement
}

func (p *Proof) Verify(leafHash, rootHash []byte, isDeleted bool) (bool, error) {
	pathLen := len(p.path)

	if pathLen == 0 {
		return false, errors.New("proof can't be empty")
	}

	// In case deleted value, node that contains it should contain []byte{1} between its hashes/bytes
	if isDeleted {
		isDeleteFound := false
		for _, hash := range p.path[0].GetHashes() {
			if bytes.Equal(hash, KeyDeleteMarkerBytes) {
				isDeleteFound = true
				break
			}
		}
		if !isDeleteFound {
			return false, nil
		}
	}

	hashToFind := leafHash

	// Validation algorithm just checks is hashToFind (current node/value hash) is part of hashes/bytes
	// list in node above. We start from value hash (leafHash) and continue to root stored in block
	for i := 0; i < pathLen; i++ {
		isHashFound := false
		for _, hash := range p.path[i].GetHashes() {
			if bytes.Equal(hash, hashToFind) {
				isHashFound = true
				break
			}
		}
		if !isHashFound {
			return false, nil
		}

		var err error
		// hash here calculated same way as node hash calculated
		hashToFind, err = CalcHash(p.path[i].GetHashes())
		if err != nil {
			return false, err
		}
	}
	// Check if calculated root hash if equal to supplied (stored in block)
	return bytes.Equal(rootHash, hashToFind), nil
}

func (p *Proof) GetPath() []*types.MPTrieProofElement {
	return p.path
}

func NewProof(path []*types.MPTrieProofElement) *Proof {
	return &Proof{path: path}
}

func CalcHash(bytes [][]byte) ([]byte, error) {
	bytesToHash := make([]byte, 0)
	for _, b := range bytes {
		bytesToHash = append(bytesToHash, b...)
	}
	return crypto.ComputeSHA256Hash(bytesToHash)
}

func ConstructCompositeKey(dbName, key string) ([]byte, error) {
	bytesToHash := make([]byte, 0)
	if len(dbName) > 0 {
		dbNameHash, err := crypto.ComputeSHA256Hash([]byte(dbName))
		if err != nil {
			return nil, err
		}
		bytesToHash = append(bytesToHash, dbNameHash...)
	}
	if len(key) > 0 {
		keyHash, err := crypto.ComputeSHA256Hash([]byte(key))
		if err != nil {
			return nil, err
		}
		bytesToHash = append(bytesToHash, keyHash...)
	}
	return crypto.ComputeSHA256Hash(bytesToHash)
}

func CalculateKeyValueHash(key, value []byte) ([]byte, error) {
	bytesToHash := make([]byte, 0)
	if len(key) > 0 {
		bytesToHash = append(bytesToHash, key...)
	}
	if len(value) > 0 {
		valHash, err := crypto.ComputeSHA256Hash(value)
		if err != nil {
			return nil, err
		}
		bytesToHash = append(bytesToHash, valHash...)
	}
	return crypto.ComputeSHA256Hash(bytesToHash)
}
