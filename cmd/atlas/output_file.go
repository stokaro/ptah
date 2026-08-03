package atlas

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"go.5x5.cz/ptah/internal/fsdurable"
)

// writeAtlasOutputFile publishes a rendered document at path atomically.
//
// The document is staged in the destination directory and then renamed over the
// destination, so a concurrent reader sees either the previous file or the
// complete new one. A schema inspection redirected into a file is routinely read
// back by the next pipeline step (`schema apply --to file://…`), and a truncated
// read there is a schema with objects missing, which is exactly the failure a
// half-written file would produce.
func writeAtlasOutputFile(path string, document []byte) error {
	dir := filepath.Dir(path)
	staged, err := os.CreateTemp(dir, ".ptah-output-*.tmp")
	if err != nil {
		return fmt.Errorf("stage output file: %w", err)
	}
	stagedPath := staged.Name()
	if err := prepareAtlasOutputFile(staged, stagedPath, document); err != nil {
		return err
	}
	if err := fsdurable.ReplaceFile(stagedPath, path); err != nil {
		return errors.Join(fmt.Errorf("publish output file: %w", err), removeAtlasOutputStage(stagedPath))
	}
	if err := fsdurable.SyncDir(dir); err != nil {
		return fmt.Errorf("sync published output file directory: %w", err)
	}
	return nil
}

func prepareAtlasOutputFile(staged *os.File, path string, document []byte) error {
	if _, err := staged.Write(document); err != nil {
		return errors.Join(err, staged.Close(), removeAtlasOutputStage(path))
	}
	if err := staged.Chmod(0o644); err != nil {
		return errors.Join(err, staged.Close(), removeAtlasOutputStage(path))
	}
	if err := staged.Sync(); err != nil {
		return errors.Join(err, staged.Close(), removeAtlasOutputStage(path))
	}
	if err := staged.Close(); err != nil {
		return errors.Join(err, removeAtlasOutputStage(path))
	}
	return nil
}

func removeAtlasOutputStage(path string) error {
	err := os.Remove(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}
