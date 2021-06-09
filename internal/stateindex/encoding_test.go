package stateindex

import (
	"bytes"
	"math"
	"testing"
)

func TestOrderPreservingEncodingDecoding(t *testing.T) {
	for i := 0; i < 10000; i++ {
		testEncodeAndDecode(t, uint64(i))
	}
	testEncodeAndDecode(t, math.MaxUint64-1)
}

func testEncodeAndDecode(t *testing.T, n uint64) {
	value := encodeOrderPreservingVarUint64(n)
	nextValue := encodeOrderPreservingVarUint64(n + 1)
	if !(bytes.Compare(value, nextValue) < 0) {
		t.Fatalf("A smaller integer should result into smaller bytes. Encoded bytes for [%d] is [%x] and for [%d] is [%x]",
			n, n+1, value, nextValue)
	}
	decodedValue := decodeOrderPreservingVarUint64(value)
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
	if !(bytes.Compare(value, nextValue) > 0) {
		t.Fatalf("A smaller integer should result into greater bytes. Encoded bytes for [%d] is [%x] and for [%d] is [%x]",
			n, n+1, value, nextValue)
	}
	decodedValue := decodeReverseOrderVarUint64(value)
	if decodedValue != n {
		t.Fatalf("Value not same after decoding. Original value = [%d], decode value = [%d]", n, decodedValue)
	}
}
