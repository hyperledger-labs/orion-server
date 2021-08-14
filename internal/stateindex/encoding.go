package stateindex

import (
	"errors"
	"math"
)

const (
	hextable     = "0123456789abcdef"
	reverseOrder = '0'
	normalOrder  = '1'
)

// EncodeInt64 encodes a given int64 value to a hexadecimal representation to
// preserve the order of actual value, i.e., -100 < -10 < 0 < 100 < 1000
func EncodeInt64(n int64) string {
	if n < 0 {
		return encodeReverseOrderVarUint64(-uint64(n))
	}
	return encodeOrderPreservingVarUint64(uint64(n))
}

// encodeOrderPreservingVarUint64 returns a string-representation for a uint64 number such that
// all zero-bits starting bytes are trimmed in order to reduce the length of the array
// For preserving the order in a default bytes-comparison, first byte contains the type of
// encoding and the second byte contains the number of remaining bytes.
func encodeOrderPreservingVarUint64(n uint64) string {
	var bytePosition int
	for bytePosition = 0; bytePosition <= 7; bytePosition++ {
		if byte(n>>(56-(bytePosition*8))) != 0x00 {
			break
		}
	}

	size := int8(8 - bytePosition)
	encodedBytes := make([]byte, encodedLen(int(size)+1))
	b := byte(size)
	// given that size will never be greater than 8, we use the first
	// byte to denote the normal order encoding
	encodedBytes[0] = normalOrder
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

// encodeReverseOrderVarUint64 returns a string-representation for a uint64 number such that
// the number is first subtracted from MaxUint64 and then all the leading 0xff bytes
// are trimmed and replaced by the number of such trimmed bytes. This helps in reducing the size.
// In the byte order comparison this encoding ensures that EncodeReverseOrderVarUint64(A) > EncodeReverseOrderVarUint64(B),
// If B > A
func encodeReverseOrderVarUint64(n uint64) string {
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
	// given that size will never be greater than 8, we use the first
	// byte to denote the reverse order encoding
	encodedBytes[0] = reverseOrder
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

func decodeInt64(s string) (int64, error) {
	n, o, err := decodeVarUint64(s)
	if err != nil {
		return 0, err
	}

	switch o {
	case normalOrder:
		return int64(n), nil
	default:
		return -int64(n), nil
	}
}

func decodeVarUint64(s string) (uint64, int32, error) {
	bs := []byte(s)
	encodingType := bs[0]
	switch encodingType {
	case normalOrder:
		bs[0] = '0'
		n, err := decodeOrderPreservingVarUint64(bs)
		if err != nil {
			return 0, 0, err
		}
		return n, normalOrder, nil
	case reverseOrder:
		bs[0] = '0'
		n, err := decodeReverseOrderVarUint64(bs)
		if err != nil {
			return 0, 0, err
		}
		return n, reverseOrder, nil
	default:
		return 0, 0, errors.New("unexpected prefix [" + string(bs[0]) + "]")
	}
}

// decodeOrderPreservingVarUint64 decodes the number from the string obtained from method 'EncodeOrderPreservingVarUint64'.
// It returns the decoded number and error if any occurred during the decoding process.
func decodeOrderPreservingVarUint64(bs []byte) (uint64, error) {
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

// decodeReverseOrderVarUint64 decodes the number from the string obtained from function 'encodeReverseOrderVarUint64'.
func decodeReverseOrderVarUint64(bs []byte) (uint64, error) {
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
