package mptrie

import (
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/hyperledger-labs/orion-server/pkg/state"

	"github.com/stretchr/testify/require"
)

func TestMPTrieGetProof(t *testing.T) {
	keysToInsert := [][]byte{
		convertHexToKey(t, []byte("123456789abc")),
		convertHexToKey(t, []byte("123456789a")),
		convertHexToKey(t, []byte("123456789abd")),
		convertHexToKey(t, []byte("12345678")),
		convertHexToKey(t, []byte("12345679")),
		convertHexToKey(t, []byte("123456")),
		convertHexToKey(t, []byte("1234")),
	}

	nonExistKeys := [][]byte{
		convertHexToKey(t, []byte("123456789abf")),
		convertHexToKey(t, []byte("123456789b")),
		convertHexToKey(t, []byte("123456789abe")),
		convertHexToKey(t, []byte("12345670")),
		convertHexToKey(t, []byte("123457")),
		convertHexToKey(t, []byte("1134")),
	}

	keysToDelete := [][]byte{
		convertHexToKey(t, []byte("1234568a")),
		convertHexToKey(t, []byte("1234569ab0")),
	}

	values := [][]byte{
		[]byte("A"),
		[]byte("B"),
		[]byte("C"),
		[]byte("D"),
		[]byte("E"),
		[]byte("F"),
		[]byte("G"),
	}

	store := newMockStore()
	trie, err := NewTrie(nil, store)
	require.NoError(t, err)
	require.NotNil(t, trie)

	for i, key := range keysToInsert {
		require.NoError(t, trie.Update(key, values[i]))
	}

	for _, key := range keysToDelete {
		require.NoError(t, trie.Update(key, []byte("TO_DELETE")))
	}

	fmt.Println("Trie before delete")
	printTrie(t, trie, 20)
	rootHashBeforeDelete, err := trie.Hash()
	require.NoError(t, err)
	require.NotNil(t, rootHashBeforeDelete)

	for _, key := range keysToDelete {
		_, err := trie.Delete(key)
		require.NoError(t, err)
	}

	fmt.Println("Trie after delete")
	printTrie(t, trie, 20)
	rootHashAfterDelete, err := trie.Hash()
	require.NoError(t, err)
	require.NotNil(t, rootHashAfterDelete)

	trie, err = NewTrie(rootHashBeforeDelete, store)

	for i, key := range keysToInsert {
		proof, err := trie.GetProof(key, false)
		require.NoError(t, err)
		require.NotNil(t, proof)
		valPtr, err := state.CalculateKeyValueHash(key, values[i])
		require.NoError(t, err)
		require.NotNil(t, valPtr)
		isValid, err := proof.Verify(valPtr, rootHashBeforeDelete, false)
		require.NoError(t, err)
		require.True(t, isValid, fmt.Sprintf("Key is: %s", convertKeyToHex(t, key)))

		proof, err = trie.GetProof(key, true)
		require.NoError(t, err)
		require.Nil(t, proof)
	}

	for _, key := range keysToDelete {
		proof, err := trie.GetProof(key, false)
		require.NoError(t, err)
		require.NotNil(t, proof)
		valPtr, err := state.CalculateKeyValueHash(key, []byte("TO_DELETE"))
		require.NoError(t, err)
		require.NotNil(t, valPtr)
		isValid, err := proof.Verify(valPtr, rootHashBeforeDelete, false)
		require.NoError(t, err)
		require.True(t, isValid)

		proof, err = trie.GetProof(key, true)
		require.NoError(t, err)
		require.Nil(t, proof)
	}

	for _, key := range nonExistKeys {
		proof, err := trie.GetProof(key, false)
		require.NoError(t, err)
		require.Nil(t, proof)

		proof, err = trie.GetProof(key, true)
		require.NoError(t, err)
		require.Nil(t, proof)
	}

	trie, err = NewTrie(rootHashAfterDelete, store)

	for i, key := range keysToInsert {
		proof, err := trie.GetProof(key, false)
		require.NoError(t, err)
		require.NotNil(t, proof)
		valPtr, err := state.CalculateKeyValueHash(key, values[i])
		require.NoError(t, err)
		require.NotNil(t, valPtr)
		isValid, err := proof.Verify(valPtr, rootHashAfterDelete, false)
		require.NoError(t, err)
		require.True(t, isValid)

		proof, err = trie.GetProof(key, true)
		require.NoError(t, err)
		require.Nil(t, proof)
	}

	for _, key := range keysToDelete {
		proof, err := trie.GetProof(key, true)
		require.NoError(t, err)
		require.NotNil(t, proof)
		valPtr, err := state.CalculateKeyValueHash(key, []byte("TO_DELETE"))
		require.NoError(t, err)
		require.NotNil(t, valPtr)
		isValid, err := proof.Verify(valPtr, rootHashAfterDelete, true)
		require.NoError(t, err)
		require.True(t, isValid)
		isValid, err = proof.Verify(valPtr, rootHashBeforeDelete, false)
		require.NoError(t, err)
		require.False(t, isValid)

		proof, err = trie.GetProof(key, false)
		require.NoError(t, err)
		require.Nil(t, proof)
	}

	for _, key := range nonExistKeys {
		proof, err := trie.GetProof(key, false)
		require.NoError(t, err)
		require.Nil(t, proof)

		proof, err = trie.GetProof(key, true)
		require.NoError(t, err)
		require.Nil(t, proof)
	}

}

func convertKeyToHex(t *testing.T, key []byte) []byte {
	res := make([]byte, hex.EncodedLen(len(key)))
	hex.Encode(res, key)

	return res
}
