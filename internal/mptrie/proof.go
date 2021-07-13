package mptrie

import (
	"bytes"

	"github.com/pkg/errors"
)

type NodeBytes [][]byte

type Proof struct {
	// For each node in trie path, it contains bytes of all node fields and []byte{1} in case of deleted flag true
	// Path is from rom node contains value to root node
	// Exactly same byte slices used to calculate node hash.
	path []NodeBytes
}

// GetProof calculates proof (path) from node contains value to root node in trie
// for given key and delete flag, i.e. if value was deleted, but delete flag id false,
// no proof will be calculated
func (t *MPTrie) GetProof(key []byte, isDeleted bool) (*Proof, error) {
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

	res := &Proof{
		path: make([]NodeBytes, 0),
	}
	for i := len(path) - 1; i >= 0; i-- {
		node := path[i]
		res.path = append(res.path, node.bytes())
	}

	return res, nil
}

func (p *Proof) Validate(leafHash, rootHash []byte, isDeleted bool) (bool, error) {
	pathLen := len(p.path)

	if pathLen == 0 {
		return false, errors.New("proof can't be empty")
	}

	// In case deleted value, node that contains it should contain []byte{1} between its hashes/bytes
	if isDeleted {
		isDeleteFound := false
		for _, hash := range p.path[0] {
			if bytes.Equal(hash, deleteBytes) {
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
		for _, hash := range p.path[i] {
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
		hashToFind, err = calcHash(p.path[i])
		if err != nil {
			return false, err
		}
	}
	// Check if calculated root hash if equal to supplied (stored in block)
	return bytes.Equal(rootHash, hashToFind), nil
}

func (p *Proof) GetPath() []NodeBytes {
	return p.path
}

func NewProof(path []NodeBytes) *Proof {
	return &Proof{path: path}
}
