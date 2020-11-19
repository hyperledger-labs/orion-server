package backend

import "path/filepath"

func constructWorldStatePath(dir string) string {
	return filepath.Join(dir, "worldstate")
}

func constructBlockStorePath(dir string) string {
	return filepath.Join(dir, "blockstore")
}
