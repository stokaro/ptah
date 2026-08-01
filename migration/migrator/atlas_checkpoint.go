package migrator

import (
	"fmt"
	"strings"
)

// atlasCheckpointDirective is the file directive Atlas writes as the first
// line of a checkpoint migration produced by `atlas migrate checkpoint`.
const atlasCheckpointDirective = "-- atlas:checkpoint"

// atlasDirectivePrefix starts every Atlas file directive line (txtar,
// checkpoint, txmode, ...). The leading directive block of a migration file is
// the run of such lines (blank lines allowed between them) before any other
// content.
const atlasDirectivePrefix = "-- atlas:"

// AtlasCheckpointTxtarConflictError reports a migration file whose leading
// directive block declares both `-- atlas:checkpoint` and `-- atlas:txtar`.
// Measured Atlas checkpoints are plain single-statement-stream SQL files, so
// the combination has no measured semantics to mirror; executing either
// interpretation could silently run the wrong SQL, so the file is rejected.
type AtlasCheckpointTxtarConflictError struct {
	// Path is the migration file that declares both directives.
	Path string
}

func (e *AtlasCheckpointTxtarConflictError) Error() string {
	return fmt.Sprintf(
		"invalid Atlas migration %s: file declares both %s and %s directives; checkpoint semantics for txtar archives are undefined, split the file",
		e.Path, atlasCheckpointDirective, atlasTxtarDirective,
	)
}

// atlasCheckpointFromSQL reports whether sql is an Atlas checkpoint migration.
// Matching mirrors measured Atlas semantics: only a `-- atlas:checkpoint`
// directive on the FIRST line marks a checkpoint; the same text further down
// is ordinary comment content. A file whose leading directive block combines
// checkpoint and txtar directives is rejected with
// [AtlasCheckpointTxtarConflictError].
func atlasCheckpointFromSQL(path, sql string) (bool, error) {
	if err := validateAtlasCheckpointTxtarExclusive(path, sql); err != nil {
		return false, err
	}
	return isAtlasCheckpointDirectiveLine(firstLine(sql)), nil
}

// validateAtlasCheckpointTxtarExclusive scans the leading directive block of
// sql and fails when it declares both the checkpoint and the txtar directive,
// in either order.
func validateAtlasCheckpointTxtarExclusive(path, sql string) error {
	sawCheckpoint := false
	sawTxtar := false
	for line := range strings.Lines(sql) {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if !strings.HasPrefix(trimmed, atlasDirectivePrefix) {
			break
		}
		if isAtlasCheckpointDirectiveLine(line) {
			sawCheckpoint = true
		}
		if trimmed == atlasTxtarDirective {
			sawTxtar = true
		}
		if sawCheckpoint && sawTxtar {
			return &AtlasCheckpointTxtarConflictError{Path: path}
		}
	}
	return nil
}

// isAtlasCheckpointDirectiveLine reports whether line is the checkpoint
// directive, alone or with trailing arguments (future Atlas versions may add
// arguments; they do not change the bootstrap-or-skip semantics).
func isAtlasCheckpointDirectiveLine(line string) bool {
	trimmed := strings.TrimSpace(line)
	if trimmed == atlasCheckpointDirective {
		return true
	}
	return strings.HasPrefix(trimmed, atlasCheckpointDirective+" ")
}

func firstLine(sql string) string {
	line, _, _ := strings.Cut(sql, "\n")
	return line
}
