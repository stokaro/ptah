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
	if err := os.WriteFile(filepath.Join(dir, FileName), sum.Bytes(), 0o600); err != nil {
		return nil, fmt.Errorf("failed to write %s: %w", FileName, err)
	}
	return sum, nil
}

// VerifyDir verifies the migrations directory at dir against its ptah.sum.
func VerifyDir(dir string) (*Result, error) {
	return Verify(os.DirFS(dir))
}
