package stateindex

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncodingInt64(t *testing.T) {
	tests := []struct {
		name string
		n    int64
	}{
		{
			name: "maximum negative value",
			n:    -math.MaxInt64,
		},
		{
			name: "minimum negative value",
			n:    -1,
		},
		{
			name: "maximum positive value",
			n:    math.MaxInt64,
		},
		{
			name: "minimum positive value",
			n:    0,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			en := EncodeInt64(tt.n)
			n, err := decodeInt64(en)
			require.NoError(t, err)
			require.Equal(t, tt.n, n)
		})
	}
}

func TestOrderPreservingEncodingDecoding(t *testing.T) {
	for i := 0; i < 10000; i++ {
		testEncodeAndDecode(t, uint64(i))
	}
	testEncodeAndDecode(t, math.MaxUint64-1)
}

func testEncodeAndDecode(t *testing.T, n uint64) {
	value := encodeOrderPreservingVarUint64(n)
	nextValue := encodeOrderPreservingVarUint64(n + 1)
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
	value := encodeReverseOrderVarUint64(n)
	nextValue := encodeReverseOrderVarUint64(n + 1)
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
