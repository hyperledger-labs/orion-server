// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package crypto

import (
	"bytes"
	"crypto"
)

func ComputeSHA256Hash(msgBytes []byte) ([]byte, error) {
	digest := crypto.SHA256.New()
	_, err := digest.Write(msgBytes)
	if err != nil {
		return nil, err
	}
	return digest.Sum(nil), nil
}

// Concatenate two hashes and calculate hash of result
// QLDB style
func ConcatenateHashes(h1, h2 []byte) ([]byte, error) {
	digest := crypto.SHA256.New()
	if len(h1) == 0 {
		return h2, nil
	}
	if len(h2) == 0 {
		return h1, nil
	}
	if bytes.Compare(h1, h2) < 0 {
		_, err := digest.Write(h1)
		if err != nil {
			return nil, err
		}
		_, err = digest.Write(h2)
		if err != nil {
			return nil, err
		}
	} else {
		_, err := digest.Write(h2)
		if err != nil {
			return nil, err
		}
		_, err = digest.Write(h1)
		if err != nil {
			return nil, err
		}
	}
	return digest.Sum(nil), nil
}
