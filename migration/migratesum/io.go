package migratesum

import (
	"fmt"
	"os"
	"path/filepath"
)

// Write computes the sum of the migrations directory at dir and writes it to
// dir/ptah.sum, returning the computed sum. The ptah.sum file is excluded
// from its own hash because it is not a migration file.
func Write(dir string) (*SumFile, error) {
	sum, err := Compute(os.DirFS(dir))
	if err != nil {
		return nil, err
	}
	// ptah.sum is committed alongside the migrations and read by everyone who
	// checks out the repo, so it uses the same 0644 as generated migration
	// files rather than a private 0600.
	if err := os.WriteFile(filepath.Join(dir, FileName), sum.Bytes(), 0644); err != nil { //nolint:gosec // 0644 is fine
		return nil, fmt.Errorf("failed to write %s: %w", FileName, err)
	}
	return sum, nil
}

// VerifyDir verifies the migrations directory at dir against its ptah.sum.
func VerifyDir(dir string) (*Result, error) {
	return Verify(os.DirFS(dir))
}
