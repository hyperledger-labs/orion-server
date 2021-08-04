package stateindex

import (
	"math"
)

// encodeOrderPreservingVarUint64 returns a byte-representation for a uint64 number such that
// all zero-bits starting bytes are trimmed in order to reduce the length of the array
// For preserving the order in a default bytes-comparison, first byte contains the number of remaining bytes.
func encodeOrderPreservingVarUint64(n uint64) []byte {
	var bytePosition int
	for bytePosition = 0; bytePosition <= 7; bytePosition++ {
		if byte(n>>(56-(bytePosition*8))) != 0x00 {
			break
		}
	}

	size := int8(8 - bytePosition)
	encodedBytes := make([]byte, size+1)
	encodedBytes[0] = byte(size)
	p := 1
	for r := bytePosition; r <= 7; r++ {
		encodedBytes[p] = byte(n >> (56 - (r * 8)))
		p++
	}
	return encodedBytes
}

// decodeOrderPreservingVarUint64 decodes the number from the bytes obtained from method 'encodeOrderPreservingVarUint64'.
// It returns the decoded number.
func decodeOrderPreservingVarUint64(b []byte) uint64 {
	size := int(b[0])
	var v uint64
	for i := 7; i >= 8-size; i-- {
		v = v | uint64(b[size-(7-i)])<<(56-(i*8))
	}
	return v
}

// encodeReverseOrderVarUint64 returns a byte-representation for a uint64 number such that
// the number is first subtracted from MaxUint64 and then all the leading 0xff bytes
// are trimmed and replaced by the number of such trimmed bytes. This helps in reducing the size.
// In the byte order comparison this encoding ensures that encodeReverseOrderVarUint64(A) > encodeReverseOrderVarUint64(B),
// If B > A
func encodeReverseOrderVarUint64(n uint64) []byte {
	n = math.MaxUint64 - n

	var bytePosition int
	for bytePosition = 0; bytePosition <= 7; bytePosition++ {
		if byte(n>>(56-(bytePosition*8))) != 0xff {
			break
		}
	}

	size := int8(8 - bytePosition)
	encodedBytes := make([]byte, size+1)
	encodedBytes[0] = byte(bytePosition)
	p := 1
	for r := bytePosition; r <= 7; r++ {
		encodedBytes[p] = byte(n >> (56 - (r * 8)))
		p++
	}
	return encodedBytes
}

// decodeReverseOrderVarUint64 decodes the number from the bytes obtained from function 'encodeReverseOrderVarUint64'.
func decodeReverseOrderVarUint64(b []byte) uint64 {
	bytePosition := int(b[0])

	size := 8 - bytePosition

	var v uint64
	var i int
	for i = 7; i >= 8-size; i-- {
		v = v | uint64(b[size-(7-i)])<<(56-(i*8))
	}

	for j := i; j >= 0; j-- {
		v = v | uint64(0xff)<<(56-(j*8))
	}

	return math.MaxUint64 - v
}
