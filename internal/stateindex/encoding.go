package stateindex

import (
	"errors"
	"math"
)

const hextable = "0123456789abcdef"

// EncodeOrderPreservingVarUint64 returns a string-representation for a uint64 number such that
// all zero-bits starting bytes are trimmed in order to reduce the length of the array
// For preserving the order in a default bytes-comparison, first byte contains the number of remaining bytes.
func EncodeOrderPreservingVarUint64(n uint64) string {
	var bytePosition int
	for bytePosition = 0; bytePosition <= 7; bytePosition++ {
		if byte(n>>(56-(bytePosition*8))) != 0x00 {
			break
		}
	}

	size := int8(8 - bytePosition)
	encodedBytes := make([]byte, encodedLen(int(size)+1))
	b := byte(size)
	// given that the size is <= 8, the 4 most significant bits would always be 0
	encodedBytes[0] = '0'
	encodedBytes[1] = hextable[b]

	j := 2
	for r := bytePosition; r <= 7; r++ {
		b = byte(n >> (56 - (r * 8)))
		encodedBytes[j] = hextable[b>>4]
		encodedBytes[j+1] = hextable[b&0x0f]
		j += 2
	}
	return string(encodedBytes)
}

// decodeOrderPreservingVarUint64 decodes the number from the string obtained from method 'EncodeOrderPreservingVarUint64'.
// It returns the decoded number and error if any occurred during the decoding process.
func decodeOrderPreservingVarUint64(s string) (uint64, error) {
	bs := []byte(s)
	sizeByte, err := convertHexAtLocationToBinary(bs, 0)
	if err != nil {
		return 0, err
	}
	size := int(sizeByte)
	var v uint64

	for i := 7; i >= 8-size; i-- {
		location := (size - (7 - i)) * 2 // 2 character per hex
		b, err := convertHexAtLocationToBinary(bs, location)
		if err != nil {
			return 0, err
		}
		v = v | uint64(b)<<(56-(i*8))
	}

	return v, nil
}

// EncodeReverseOrderVarUint64 returns a string-representation for a uint64 number such that
// the number is first subtracted from MaxUint64 and then all the leading 0xff bytes
// are trimmed and replaced by the number of such trimmed bytes. This helps in reducing the size.
// In the byte order comparison this encoding ensures that EncodeReverseOrderVarUint64(A) > EncodeReverseOrderVarUint64(B),
// If B > A
func EncodeReverseOrderVarUint64(n uint64) string {
	n = math.MaxUint64 - n

	var bytePosition int
	for bytePosition = 0; bytePosition <= 7; bytePosition++ {
		if byte(n>>(56-(bytePosition*8))) != 0xff {
			break
		}
	}

	size := int8(8 - bytePosition)
	encodedBytes := make([]byte, encodedLen(int(size)+1))
	b := byte(bytePosition)
	encodedBytes[0] = '0'
	encodedBytes[1] = hextable[b]

	j := 2
	for r := bytePosition; r <= 7; r++ {
		b = byte(n >> (56 - (r * 8)))
		encodedBytes[j] = hextable[b>>4]
		encodedBytes[j+1] = hextable[b&0x0f]
		j += 2
	}
	return string(encodedBytes)
}

// decodeReverseOrderVarUint64 decodes the number from the string obtained from function 'encodeReverseOrderVarUint64'.
func decodeReverseOrderVarUint64(s string) (uint64, error) {
	bs := []byte(s)
	bytePosition, err := convertHexAtLocationToBinary(bs, 0)
	if err != nil {
		return 0, err
	}
	size := 8 - int(bytePosition)

	var v uint64
	var i int
	for i = 7; i >= 8-size; i-- {
		location := (size - (7 - i)) * 2 // 2 character per hex
		b, err := convertHexAtLocationToBinary(bs, location)
		if err != nil {
			return 0, err
		}
		v = v | uint64(b)<<(56-(i*8))
	}

	for j := i; j >= 0; j-- {
		v = v | uint64(0xff)<<(56-(j*8))
	}

	return math.MaxUint64 - v, nil
}

func convertHexAtLocationToBinary(b []byte, location int) (byte, error) {
	firstChar, err := fromHexChar(b[location])
	if err != nil {
		return 0, err
	}

	secondChar, err := fromHexChar(b[location+1])
	if err != nil {
		return 0, err
	}

	return (firstChar << 4) | secondChar, nil
}

// EncodedLen returns the length of an encoding of n source bytes.
// Specifically, it returns n * 2.
func encodedLen(n int) int { return n * 2 }

// fromHexChar converts a hex character into its value
func fromHexChar(c byte) (byte, error) {
	switch {
	case '0' <= c && c <= '9':
		return c - '0', nil
	case 'a' <= c && c <= 'f':
		return c - 'a' + 10, nil
	case 'A' <= c && c <= 'F':
		return c - 'A' + 10, nil
	default:
		return 0, errors.New("invalid hex character [" + string(c) + "]")
	}
}
