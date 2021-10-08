package stateindex

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOrderPreservingEncodingDecoding(t *testing.T) {
	for i := 0; i < 10000; i++ {
		testEncodeAndDecode(t, uint64(i))
	}
	testEncodeAndDecode(t, math.MaxUint64-1)
}

func testEncodeAndDecode(t *testing.T, n uint64) {
	value := EncodeOrderPreservingVarUint64(n)
	nextValue := EncodeOrderPreservingVarUint64(n + 1)
	if !(value < nextValue) {
		t.Fatalf("A smaller integer should result into smaller bytes. Encoded bytes for [%d] is [%x] and for [%d] is [%x]",
			n, value, n+1, nextValue)
	}
	decodedValue, nt, err := decodeVarUint64(value)
	require.NoError(t, err)
	require.Equal(t, normalOrder, nt)
	if decodedValue != n {
		t.Fatalf("Value not same after decoding. Original value = [%d], decode value = [%d]", n, decodedValue)
	}
}

func TestReverseOrderPreservingEncodingDecoding(t *testing.T) {
	for i := 0; i < 10000; i++ {
		testReverseOrderEncodingAndDecoding(t, uint64(i))
	}
	testReverseOrderEncodingAndDecoding(t, math.MaxUint64-1)
}

func testReverseOrderEncodingAndDecoding(t *testing.T, n uint64) {
	value := EncodeReverseOrderVarUint64(n)
	nextValue := EncodeReverseOrderVarUint64(n + 1)
	if !(value > nextValue) {
		t.Fatalf("A smaller integer should result into greater bytes. Encoded bytes for [%d] is [%x] and for [%d] is [%x]",
			n, value, n+1, nextValue)
	}
	decodedValue, nt, err := decodeVarUint64(value)
	require.Equal(t, reverseOrder, nt)
	require.NoError(t, err)
	if decodedValue != n {
		t.Fatalf("Value not same after decoding. Original value = [%d], decode value = [%d]", n, decodedValue)
	}
}
