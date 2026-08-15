package migratevalidate_test

import (
	"fmt"
	"io/fs"
	"os"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/migratevalidate"
)

// TestAtlasDirectoryError_AdaptsWhicheverNameStatCarries pins that the Atlas
// wording is produced on every operating system.
//
// os.Stat names its os.PathError Op "stat" on Unix and "GetFileAttributesEx"
// on Windows. The adapter compared that name to the literal "stat", so on
// Windows it declined to adapt and `ptah-compat migrate validate` and
// `migrate import` printed Go's native sentence instead of Atlas's -- a
// compatibility divergence, on a surface whose whole purpose is not to have
// one.
//
// The rows hand the Op in rather than calling os.Stat, which is what makes the
// Windows answer reachable from a Unix runner. A test that statted a missing
// path would assert only the platform it happened to run on, and would have
// stayed green through the entire defect.
func TestAtlasDirectoryError_AdaptsWhicheverNameStatCarries(t *testing.T) {
	const dir = "migrations"

	tests := []struct {
		name string
		op   string
	}{
		{name: "the Unix name", op: "stat"},
		{name: "the Windows name", op: "GetFileAttributesEx"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			pathErr := &os.PathError{Op: test.op, Path: dir, Err: fs.ErrNotExist}
			wrapped := fmt.Errorf("migrations directory %s: %w", dir, pathErr)

			adapted := migratevalidate.AtlasDirectoryError(dir, wrapped)

			c.Assert(adapted.Error(), qt.Equals, "sql/migrate: "+pathErr.Error())
			// The original stays reachable, so errors.Is still answers about
			// the failure rather than about the display wrapper.
			c.Assert(adapted, qt.ErrorIs, fs.ErrNotExist)
		})
	}
}

// TestAtlasDirectoryError_LeavesEverythingElseAlone is the control. The Op was
// one of five clauses, and dropping it must not widen what gets adapted.
func TestAtlasDirectoryError_LeavesEverythingElseAlone(t *testing.T) {
	const dir = "migrations"

	tests := []struct {
		name string
		err  error
	}{
		{
			name: "a different path",
			err: fmt.Errorf("migrations directory %s: %w", dir,
				&os.PathError{Op: "stat", Path: "elsewhere", Err: fs.ErrNotExist}),
		},
		{
			name: "a failure that is not absence",
			err: fmt.Errorf("migrations directory %s: %w", dir,
				&os.PathError{Op: "stat", Path: dir, Err: fs.ErrPermission}),
		},
		{
			name: "wrapping this adapter does not recognize",
			err: fmt.Errorf("reading %s: %w", dir,
				&os.PathError{Op: "stat", Path: dir, Err: fs.ErrNotExist}),
		},
		{
			name: "not a path error at all",
			err:  fmt.Errorf("migrations directory %s: not a directory", dir),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(migratevalidate.AtlasDirectoryError(dir, test.err).Error(), qt.Equals, test.err.Error())
		})
	}
}
