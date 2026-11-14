package atlasreport

import (
	"errors"
	"fmt"
	"io/fs"
	"strconv"
	"strings"

	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

// atlasNonLinearErrorDocs is the page the community binary's out-of-order
// refusal points to, in `migrate apply` and in `migrate status` alike.
const atlasNonLinearErrorDocs = "https://atlasgo.io/versioned/apply#non-linear-error"

// MigrateApplyOutOfOrderError is the refusal `migrate apply` reports for the
// pending migrations linear execution refuses, worded as the pinned community
// binary v1.3.0 words it. Measured on SQLite, one file and two:
//
//	migration file 2_b.sql was added out of order. See: https://atlasgo.io/versioned/apply#non-linear-error
//	migration files 2_b.sql, 3_c.sql were added out of order. See: https://atlasgo.io/versioned/apply#non-linear-error
//
// The files are listed in execution order and named as the apply report names
// them: the file of fsys, or the source file a converted layout names, so a
// Flyway file is `V10__b.sql` rather than the numeric file its conversion
// runs. Unwrap returns the migrator's refusal, so [errors.As] still finds the
// [migrator.OutOfOrderError] it was read from.
type MigrateApplyOutOfOrderError struct {
	// Files are the refused migration files, named as the apply report
	// names them.
	Files []string

	cause error
}

func (e *MigrateApplyOutOfOrderError) Error() string {
	if len(e.Files) == 1 {
		return fmt.Sprintf("migration file %s was added out of order. See: %s", e.Files[0], atlasNonLinearErrorDocs)
	}
	return fmt.Sprintf("migration files %s were added out of order. See: %s", strings.Join(e.Files, ", "), atlasNonLinearErrorDocs)
}

func (e *MigrateApplyOutOfOrderError) Unwrap() error { return e.cause }

// NewMigrateApplyOutOfOrderError reads a linear-order refusal out of err and
// names its files. fsys is the directory the run executed, and sources names
// the files of a layout whose conversion renamed them, as
// [MigrateApplyResultOptions.SourceFiles] does.
//
// It returns nil, nil when err is not such a refusal, and an error when fsys
// cannot be read for the names.
func NewMigrateApplyOutOfOrderError(
	err error,
	fsys fs.FS,
	sources map[int64]MigrateApplySourceFile,
) (*MigrateApplyOutOfOrderError, error) {
	refused, ok := errors.AsType[*migrator.OutOfOrderError](err)
	if !ok || len(refused.Versions) == 0 {
		return nil, nil
	}
	names, nameErr := migrateApplyFileNames(fsys, sources)
	if nameErr != nil {
		return nil, nameErr
	}
	files := make([]string, 0, len(refused.Versions))
	for _, version := range refused.Versions {
		name, named := names[version]
		if !named {
			name = strconv.FormatInt(version, 10)
		}
		files = append(files, name)
	}
	return &MigrateApplyOutOfOrderError{Files: files, cause: err}, nil
}

// migrateApplyFileNames maps the execution-order key of each up file of fsys
// to the file the operator's directory holds: the file itself, or for a
// converted layout, the file sources says it came from.
func migrateApplyFileNames(fsys fs.FS, sources map[int64]MigrateApplySourceFile) (map[int64]string, error) {
	discovered, err := migrationfile.Discover(fsys, migrationfile.DirFormatAtlas)
	if err != nil {
		return nil, fmt.Errorf("discover Atlas migration files: %w", err)
	}
	names := make(map[int64]string, len(discovered))
	for _, file := range discovered {
		if file.Direction == "up" {
			names[file.Version] = file.Path
		}
	}
	for version, source := range sources {
		names[version] = source.Name
	}
	return names, nil
}
