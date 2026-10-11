package fsutil

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWriteFileAtomicReplaces(t *testing.T) {
	path := filepath.Join(t.TempDir(), "F")
	for _, want := range []string{"first", "second, longer than the first", "3"} {
		if err := WriteFileAtomic(path, []byte(want)); err != nil {
			t.Fatal(err)
		}
		if got, _ := os.ReadFile(path); string(got) != want {
			t.Fatalf("file holds %q, want %q", got, want)
		}
	}
	if left, _ := filepath.Glob(path + ".tmp-*"); len(left) != 0 {
		t.Fatalf("temp files left behind: %v", left)
	}
}

// A crash between creating the temp file and renaming it leaves the temp
// file. CleanTemps removes those, and only those.
func TestCleanTempsRemovesOnlyLeftovers(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "F")
	os.WriteFile(path, []byte("keep"), 0o644)
	os.WriteFile(path+".tmp-123", []byte("leftover"), 0o644)
	os.WriteFile(filepath.Join(dir, "G.tmp-1"), []byte("another file's"), 0o644)
	if err := CleanTemps(path); err != nil {
		t.Fatal(err)
	}
	entries, _ := os.ReadDir(dir)
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	if len(names) != 2 || names[0] != "F" || names[1] != "G.tmp-1" {
		t.Fatalf("after CleanTemps: %v; want F and G.tmp-1", names)
	}
}
