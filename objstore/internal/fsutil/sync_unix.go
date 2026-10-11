//go:build !windows

package fsutil

import (
	"fmt"
	"os"
)

// SyncDir fsyncs a directory. On Linux, a rename or unlink is not durable
// until the containing directory's metadata is flushed.
func SyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("fsutil: open dir: %w", err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("fsutil: fsync dir: %w", err)
	}
	return nil
}
