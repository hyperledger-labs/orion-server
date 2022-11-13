package store

import (
	"encoding/base64"
	"encoding/binary"
	"errors"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger-labs/orion-server/internal/mptrie"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

const (
	Branch    = 0x1
	Extension = 0x2
	Value     = 0x3
)

func (s *Store) GetNode(nodePtr []byte) (mptrie.TrieNode, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	key := base64.StdEncoding.EncodeToString(nodePtr)
	storedNodeBytes, ok := s.inMemoryNodes[key]
	if !ok {
		storedNodeBytes, ok = s.nodesToPersist[key]
		if !ok {
			var err error
			storedNodeBytes, err = s.trieDataDB.Get(append(trieNodesNs, []byte(key)...), &opt.ReadOptions{})
			if err != nil {
				return nil, err
			}
		}
	}
	nodeTypePrefix := storedNodeBytes[0]
	switch nodeTypePrefix {
	case Branch:
		branchNode := &mptrie.BranchNode{
			Children: make([][]byte, 16),
		}
		if err := proto.Unmarshal(storedNodeBytes[1:], branchNode); err != nil {
			return nil, err
		}
		// In array of byte slices (Children), proto.Unmarshal may convert nil to empty byte slice.
		for i, c := range branchNode.Children {
			if c != nil && len(c) == 0 {
				branchNode.Children[i] = nil
			}
		}
		return branchNode, nil
	case Extension:
		extensionNode := &mptrie.ExtensionNode{}
		if err := proto.Unmarshal(storedNodeBytes[1:], extensionNode); err != nil {
			return nil, err
		}
		return extensionNode, nil
	case Value:
		valueNode := &mptrie.ValueNode{}
		if err := proto.Unmarshal(storedNodeBytes[1:], valueNode); err != nil {
			return nil, err
		}
		return valueNode, nil
	default:
		return nil, errors.New("unknown node type")
	}
}

func (s *Store) GetValue(valuePtr []byte) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	key := base64.StdEncoding.EncodeToString(valuePtr)
	valueBytes, ok := s.inMemoryValues[key]
	if !ok {
		valueBytes, ok = s.valuesToPersist[key]
		if !ok {
			var err error
			valueBytes, err = s.trieDataDB.Get(append(trieValueNs, []byte(key)...), &opt.ReadOptions{})
			if err != nil {
				return nil, err
			}
		}
	}
	return valueBytes, nil
}

func (s *Store) PutNode(nodePtr []byte, node mptrie.TrieNode) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := base64.StdEncoding.EncodeToString(nodePtr)
	nodeBytesWithType := make([]byte, 0)
	switch node.(type) {
	case *mptrie.BranchNode:
		nodeBytes, err := proto.Marshal(node.(*mptrie.BranchNode))
		if err != nil {
			return err
		}
		nodeBytesWithType = append([]byte{Branch}, nodeBytes...)
	case *mptrie.ExtensionNode:
		nodeBytes, err := proto.Marshal(node.(*mptrie.ExtensionNode))
		if err != nil {
			return err
		}
		nodeBytesWithType = append([]byte{Extension}, nodeBytes...)
	case *mptrie.ValueNode:
		nodeBytes, err := proto.Marshal(node.(*mptrie.ValueNode))
		if err != nil {
			return err
		}
		nodeBytesWithType = append([]byte{Value}, nodeBytes...)
	}
	s.inMemoryNodes[key] = nodeBytesWithType
	return nil
}

func (s *Store) PutValue(valuePtr, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := base64.StdEncoding.EncodeToString(valuePtr)
	s.inMemoryValues[key] = value
	return nil
}

func (s *Store) PersistNode(nodePtr []byte) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := base64.StdEncoding.EncodeToString(nodePtr)
	nodeBytesWithType, ok := s.inMemoryNodes[key]
	if ok {
		s.nodesToPersist[key] = nodeBytesWithType
		return true, nil
	}
	return false, nil
}

func (s *Store) PersistValue(valuePtr []byte) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := base64.StdEncoding.EncodeToString(valuePtr)
	valueBytes, ok := s.inMemoryValues[key]
	if ok {
		s.valuesToPersist[key] = valueBytes
		return true, nil
	}
	return false, nil
}

func (s *Store) Height() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	blockNumBytes, err := s.trieDataDB.Get(lastBlockNs, &opt.ReadOptions{})
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(blockNumBytes), nil
}

func (s *Store) CommitChanges(blockNum uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	batch := new(leveldb.Batch)

	blockNumBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(blockNumBytes, blockNum)
	batch.Put(lastBlockNs, blockNumBytes)

	for k, v := range s.valuesToPersist {
		batch.Put(append(trieValueNs, []byte(k)...), v)
	}

	for k, n := range s.nodesToPersist {
		batch.Put(append(trieNodesNs, []byte(k)...), n)
	}
	if err := s.trieDataDB.Write(batch, &opt.WriteOptions{Sync: true}); err != nil {
		return err
	}
	s.nodesToPersist = make(map[string][]byte)
	s.valuesToPersist = make(map[string][]byte)
	s.inMemoryNodes = make(map[string][]byte)
	s.inMemoryValues = make(map[string][]byte)
	return nil
}

func (s *Store) RollbackChanges() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.nodesToPersist = make(map[string][]byte)
	s.valuesToPersist = make(map[string][]byte)
	s.inMemoryNodes = make(map[string][]byte)
	s.inMemoryValues = make(map[string][]byte)
	return nil
}

func (s *Store) IsDisabled() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.disabled
}

func (s *Store) SetDisabled(disabled bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.disabled = disabled
}
