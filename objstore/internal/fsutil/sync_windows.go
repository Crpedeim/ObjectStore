//go:build windows

package fsutil

// SyncDir is a no-op on Windows. Sync on a directory calls FlushFileBuffers,
// which requires a handle opened for writing, and Go opens directories
// read-only — so the call fails with "Access is denied". NTFS journals
// metadata changes itself, and MoveFileEx is flushed as part of the rename,
// so there is no directory-level fsync to perform. etcd and Prometheus make
// the same split.
func SyncDir(dir string) error { return nil }
