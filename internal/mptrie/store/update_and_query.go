package store

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"

	"github.com/syndtr/goleveldb/leveldb"

	"github.com/IBM-Blockchain/bcdb-server/internal/mptrie"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

const (
	Branch    = "B"
	Extension = "E"
	Value     = "V"
)

func (s *Store) GetNode(nodePtr []byte) (mptrie.TrieNode, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	key := base64.StdEncoding.EncodeToString(nodePtr)
	nodeBytes, ok := s.inMemoryNodes[key]
	if !ok {
		nodeBytes, ok = s.nodesToPersist[key]
		if !ok {
			storedNodeBytes, err := s.trieDataDB.Get(append(trieNodesNs, []byte(key)...), &opt.ReadOptions{})
			if err != nil {
				return nil, err
			}
			nodeBytes = &NodeBytesWithType{}
			if err = json.Unmarshal(storedNodeBytes, nodeBytes); err != nil {
				return nil, err
			}
		}
	}
	switch nodeBytes.NodeType {
	case Branch:
		branchNode := &mptrie.BranchNode{
			Children: make([][]byte, 16),
		}
		if err := json.Unmarshal(nodeBytes.NodeBytes, branchNode); err != nil {
			return nil, err
		}
		return branchNode, nil
	case Extension:
		extensionNode := &mptrie.ExtensionNode{}
		if err := json.Unmarshal(nodeBytes.NodeBytes, extensionNode); err != nil {
			return nil, err
		}
		return extensionNode, nil
	case Value:
		valueNode := &mptrie.ValueNode{}
		if err := json.Unmarshal(nodeBytes.NodeBytes, valueNode); err != nil {
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
	nodeBytes, err := json.Marshal(node)
	if err != nil {
		return err
	}
	nodeBytesWithType := &NodeBytesWithType{
		NodeBytes: nodeBytes,
	}
	switch node.(type) {
	case *mptrie.BranchNode:
		nodeBytesWithType.NodeType = Branch
	case *mptrie.ExtensionNode:
		nodeBytesWithType.NodeType = Extension
	case *mptrie.ValueNode:
		nodeBytesWithType.NodeType = Value
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
		nodeBytes, err := json.Marshal(n)
		if err != nil {
			return err
		}
		batch.Put(append(trieNodesNs, []byte(k)...), nodeBytes)
	}
	if err := s.trieDataDB.Write(batch, &opt.WriteOptions{Sync: true}); err != nil {
		return err
	}
	s.nodesToPersist = make(map[string]*NodeBytesWithType)
	s.valuesToPersist = make(map[string][]byte)
	s.inMemoryNodes = make(map[string]*NodeBytesWithType)
	s.inMemoryValues = make(map[string][]byte)
	return nil
}

func (s *Store) RollbackChanges() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.nodesToPersist = make(map[string]*NodeBytesWithType)
	s.valuesToPersist = make(map[string][]byte)
	s.inMemoryNodes = make(map[string]*NodeBytesWithType)
	s.inMemoryValues = make(map[string][]byte)
	return nil
}
