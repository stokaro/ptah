package atlas

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"go.5x5.cz/ptah/internal/fsdurable"
)

// writeAtlasPlanDocument stages a complete plan beside its destination before
// publishing it atomically. Explicit --output replaces the destination;
// default-name publication refuses an existing path without a stat/write race.
func writeAtlasPlanDocument(path string, document []byte, outputMode atlasSchemaPlanOutputMode) error {
	dir := filepath.Dir(path)
	staged, err := os.CreateTemp(dir, ".ptah-plan-*.tmp")
	if err != nil {
		return fmt.Errorf("stage plan file: %w", err)
	}
	stagedPath := staged.Name()
	if err := prepareAtlasPlanDocument(staged, stagedPath, document); err != nil {
		return err
	}
	if outputMode == atlasSchemaPlanExplicitOutput {
		err = fsdurable.ReplaceFile(stagedPath, path)
	} else {
		err = fsdurable.MoveFileNoReplace(stagedPath, path)
	}
	if err != nil {
		return errors.Join(fmt.Errorf("publish plan file: %w", err), removeAtlasPlanStage(stagedPath))
	}
	if err := fsdurable.SyncDir(dir); err != nil {
		return fmt.Errorf("sync published plan file directory: %w", err)
	}
	return nil
}

func prepareAtlasPlanDocument(staged *os.File, path string, document []byte) error {
	if _, err := staged.Write(document); err != nil {
		return errors.Join(err, staged.Close(), removeAtlasPlanStage(path))
	}
	if err := staged.Chmod(0o644); err != nil {
		return errors.Join(err, staged.Close(), removeAtlasPlanStage(path))
	}
	if err := staged.Sync(); err != nil {
		return errors.Join(err, staged.Close(), removeAtlasPlanStage(path))
	}
	if err := staged.Close(); err != nil {
		return errors.Join(err, removeAtlasPlanStage(path))
	}
	return nil
}

func removeAtlasPlanStage(path string) error {
	err := os.Remove(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}
