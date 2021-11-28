---
id: state-merkle-patricia-tree
title: State Merkle Patricia Tree
---
## Merkle-Patricia Trie

Merkle-Patricia trie is combination of two tree data structures:
* Merkle tree – tree that uses child hash instead pointer
* Patricia trie – optimized dictionary tree

### Patricia trie
Main difference between Patricia trie and regular dictionary tree is algorithm to compress long one-child branches into single node.
![Trie ethereum style](worldstatetrie.png)

Different implementations contain between 2 and 3 types of different nodes, but there are 2 important types:
* Branch node – has more than 1 child – contains Children[ALPHABEIT_SIZE] list of children pointers.
* Extension node – non leaf node represent sequence of nodes that has only one child, contains Key that represents all these nodes as one.
* _Optional third type_ is Value node - leaf node, similar to Extension node, but instead pointer to next node, it contains value.
    * Some implementations combine Extension and Value nodes into single node type.

![Trie Node Types](MPT-Node-Types.png)

#### Trie example

Following figure shows example trie with all possible types of inter-trie connections, and it contains following data:
* 5        -> A
* A123     -> B
* FAB23    -> C
* DA1F     -> D
* DA1F4    -> E
* DA1F111  -> F
* 50FF1    -> G
* 50FFC561 -> H
* DA1F48A  -> I

![Sample Trie](MPT-Sample-Trie.png)

### Trie update algorithm

* If node with updated key exist, just update Value and all Hash pointers up to the root.
* If such a node doesn't exist, traverse trie for all key matching nodes.
    * If last matching node is Value node:
        * Convert it to an ExtensionNode followed by BranchNode and add new ValueNode with the remaining path without first character
    * If last matching node is Branch node, find next node - pointed by _node.Children[nibble]_:
        * When next node is an NIL, replace it with a new ValueNode with the remaining path.
        * When next node is a ValueNode, add ExtensionNode with matching path, followed by BranchNode, and make new BranchNode point to original ValueNode with rest of its path and new ValueNode.
        * When next node is an ExtensionNode, add another ExtensionNode with common path and create a new BranchNode points to the original ExtensionNode and new ValueNode.
    * Update all Hashes up to root

### Trie update example
This example illustrates how one update can affect multiple nodes in trie, not mention hashes in whole branch

We start from trie that contains only 3 Value nodes, with three values
* A123 -> B
* DA1F -> D
* 50FF1A -> G

![Original Trie](MPT-Update-1.png)


After the insert of (DA1FE111 -> F) we see 3 new nodes - one Extension node, one Branch node
that replaced Value node holding value (D), and one Value node that holds new value (F)

![Updated Trie](MPT-Update-2.png)

### Trie value delete

We just mark node that contains deleted value with tombstone bit - `isDeleted` flag. Thus value delete is same as value update, while `isDeleted` changes instead of value. Because this flag affects node hash, it is tamper resistant.

### Trie proofs

First, lets define path from trie root to value. It is ordered list of nodes from root node to Value node or to Branch node that actually contains value.

Each node in path represented as list of byte arrays. Each byte array can be hash of child node(s) (branch node, extension node) or part of key (extension node, value node).
In addition, in our implementation, no actual value is stored in node, but only hash of `<key, value>` pair too. `isDeleted` tombstone bit represented as separate byte array as well.

Proof generation:
- Arguments are block number and key (in our case db+key).
    - As optional argument, `isDeleted` can be passed too.
- Server loads snapshot of MPTrie at time of block `N` ![Trie Block snapshot](PatriciaMerkleTrie.png)
- Server looks for path that contains `key`.
    - If path exist, it returned to user in form of list of lists of byte arrays.

Proof validation:
- Hash of `<key, value>` pair calculated and marked as `current hash`
- Validation starts from leaf node.
- For each node in path, check if any of any byte array in list that represent node equal to `current hash`.
    - If no such byte array, validation fails, exit.
    - Calculate node hash by concatenating all byte arrays in list and hashing result.
    - Make node hash new `current hash`.
- For last node in path, check is it hash equals to trie root hash stored in block header.

### BCDB implementation details

Server keeps dynamically update state trie. Its root stored as part of block header.
Once block committed to block store, all block transaction applied to trie and all changes (deltas) to trie are stored in associated trie store.

Server side implementation composed of 2 parts - one is MPTrie structure and another is trie Store.

`MPTrie` implements all trie logic, like adding/updating/deleting values.

Trie `Store` stores all Nodes in underlying storage. `nodePtr` and `valuePrt` are basically node hash and `<db, key, value>` tuple hash.

Both structures operate `TrieNode`. Each type of node exposes its `Hash` and `[][]byte` array of all inside hashes used to calculate node hash.
```go
type TrieNode interface {
	hash() ([]byte, error)
	bytes() [][]byte
}
```

`Trie` API:
```go
// NewTrie creates new Merkle-Patricia Trie, with backend store.
// If root node Hash is not nil, root node loaded from store, otherwise, empty trie is created
func NewTrie(rootHash []byte, store Store) (*MPTrie, error)
// Calculates trie hash, actually, it is root node hash
func (t *MPTrie) Hash() ([]byte, error)
// Get returns value associated with key
func (t *MPTrie) Get(key []byte) ([]byte, error)
// Update key value
func (t *MPTrie) Update(key, value []byte) error
// Delete key
func (t *MPTrie) Delete(key []byte) ([]byte, error)
// GetProof calculates proof (path) from node contains value to root node in trie
// for given key and delete flag, i.e. if value was deleted, but delete flag id false,
// no proof will be calculated
func (t *MPTrie) GetProof(key []byte, isDeleted bool) (*state.Proof, error)

type Proof struct {
    // For each node in trie path, it contains bytes of all node fields and []byte{1} in case of deleted flag true
    // Path is from rom node contains value to root node
    // Exactly same byte slices used to calculate node hash.
    Path []*types.MPTrieProofElement
}

func (p *Proof) Verify(leafHash, rootHash []byte, isDeleted bool) (bool, error)
```

Trie `Store` API:
```go

// Trie store 
type Store interface {
	// GetNode returns TrieNode associated with key/ptr. It may be temporal node
	// created by PutNode, node market to persist after PersistNode or after executing
	// CommitPersistChanges actually stored in backend storage node
	GetNode(nodePtr []byte) (TrieNode, error)
	// GetValue return value bytes associated with value ptr. Same logic as in GetNode applies.
	GetValue(valuePtr []byte) ([]byte, error)
	// PutNode store node data it temporal way - it my be accessed by GetNode, but will not stored in backend store.
	PutNode(nodePtr []byte, node TrieNode) error
	// PutValue do the same as PutNode, but for value
	PutValue(valuePtr, value []byte) error
	// PersistNode mark temporal node to be persisted to backend storage in next call to CommitPersistChanges
	PersistNode(nodePtr []byte) (bool, error)
	// PersistValue do same as PersistNode, but for value
	PersistValue(valuePtr []byte) (bool, error)
	// Height returns number of last block trie was persist for
	Height() (uint64, error)
	// CommitChanges frees all inMemory nodes and actually stores nodes and value marked to be persist by
	// PersistNode and PersistValue in single backend store update - usually used with block number
	CommitChanges(blockNum uint64) error
	// RollbackChanges free all in memory nodes and nodes marked to be persist, without storing anything in
	// underlying database. Operation can cause to current MPTrie become invalid, so always reload trie
	// after the call
	RollbackChanges() error
}
```