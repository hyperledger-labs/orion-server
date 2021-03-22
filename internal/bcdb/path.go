// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package bcdb

import "path/filepath"

func constructWorldStatePath(dir string) string {
	return filepath.Join(dir, "worldstate")
}

func constructBlockStorePath(dir string) string {
	return filepath.Join(dir, "blockstore")
}

func constructProvenanceStorePath(dir string) string {
	return filepath.Join(dir, "provenancestore")
}

func constructStateTrieStorePath(dir string) string {
	return filepath.Join(dir, "statetriestore")
}
