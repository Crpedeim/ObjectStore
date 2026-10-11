// Package fsutil holds the file-system operations that need care to survive
// a crash. The store uses them for its own files; gossip uses them to
// remember the cluster's members.
package fsutil

import (
	"fmt"
	"os"
	"path/filepath"
)

// WriteFileAtomic replaces path with data: a temp file beside it, fsync,
// rename over path, fsync the directory. A crash leaves either the old file
// or the new one — and possibly a leftover temp file, which CleanTemps
// removes.
func WriteFileAtomic(path string, data []byte) error {
	dir, name := filepath.Split(path)
	if dir == "" {
		dir = "."
	}
	f, err := os.CreateTemp(dir, name+".tmp-*")
	if err != nil {
		return fmt.Errorf("fsutil: write %s: %w", path, err)
	}
	fail := func(err error) error {
		f.Close()
		os.Remove(f.Name())
		return fmt.Errorf("fsutil: write %s: %w", path, err)
	}
	if _, err := f.Write(data); err != nil {
		return fail(err)
	}
	if err := f.Sync(); err != nil {
		return fail(err)
	}
	if err := f.Close(); err != nil {
		os.Remove(f.Name())
		return fmt.Errorf("fsutil: write %s: %w", path, err)
	}
	if err := os.Rename(f.Name(), path); err != nil {
		os.Remove(f.Name())
		return fmt.Errorf("fsutil: write %s: %w", path, err)
	}
	return SyncDir(dir)
}

// CleanTemps removes temp files that WriteFileAtomic left beside path by
// crashing before its rename.
func CleanTemps(path string) error {
	leftovers, err := filepath.Glob(path + ".tmp-*")
	if err != nil {
		return err
	}
	for _, f := range leftovers {
		if err := os.Remove(f); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("fsutil: remove leftover %s: %w", f, err)
		}
	}
	return nil
}
