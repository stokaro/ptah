package atlas

// White-box testing required: this file verifies the narrow structural match
// and unwrap chain of an unexported compatibility error adapter.

import (
	"errors"
	"fmt"
	"os"
	"syscall"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestAtlasMigrateLintDirCaptureError_AdaptsMissingOpen(t *testing.T) {
	c := qt.New(t)
	pathErr := &os.PathError{Op: "openat", Path: "missing", Err: syscall.ENOENT}
	captureErr := fmt.Errorf("open migrations directory: %w", pathErr)

	got := atlasMigrateLintDirCaptureError("nested/../missing", "", captureErr)

	c.Assert(got.Error(), qt.Equals, "sql/migrate: stat missing: no such file or directory")
	c.Assert(got, qt.ErrorIs, captureErr)
	c.Assert(got, qt.ErrorIs, syscall.ENOENT)
	var gotPathErr *os.PathError
	c.Assert(got, qt.ErrorAs, &gotPathErr)
	c.Assert(gotPathErr, qt.Equals, pathErr)
	c.Assert(errors.Unwrap(got).Error(), qt.Equals,
		"atlas migrate lint --dir: open migrations directory: openat missing: no such file or directory")
}

func TestAtlasMigrateLintDirCaptureError_RootedPathUsesObservedSpelling(t *testing.T) {
	c := qt.New(t)
	pathErr := &os.PathError{Op: "openat", Path: "missing", Err: syscall.ENOENT}
	captureErr := fmt.Errorf("open migrations directory: %w", pathErr)

	got := atlasMigrateLintDirCaptureError("/project/missing", "/project", captureErr)

	c.Assert(got.Error(), qt.Equals, "sql/migrate: stat missing: no such file or directory")
	c.Assert(got, qt.ErrorIs, captureErr)
	var gotPathErr *os.PathError
	c.Assert(got, qt.ErrorAs, &gotPathErr)
	c.Assert(gotPathErr, qt.Equals, pathErr)
}

func TestAtlasMigrateLintDirCaptureError_PreservesNonmatchingErrors(t *testing.T) {
	missingOpen := &os.PathError{Op: "openat", Path: "missing", Err: syscall.ENOENT}

	tests := []struct {
		name string
		err  error
	}{
		{
			name: "regular file",
			err: fmt.Errorf("open migrations directory: %w", &os.PathError{
				Op: "openat", Path: "regular", Err: syscall.ENOTDIR,
			}),
		},
		{
			name: "permission",
			err: fmt.Errorf("open migrations directory: %w", &os.PathError{
				Op: "open", Path: "private", Err: syscall.EACCES,
			}),
		},
		{
			name: "outside root",
			err:  errors.New(`invalid migrations directory: "/outside" is outside allowed root "/project"`),
		},
		{
			name: "malformed URL",
			err:  errors.New("decode migration directory URL path: invalid URL escape"),
		},
		{
			name: "capture",
			err:  fmt.Errorf("capture migrations directory: %w", missingOpen),
		},
		{
			name: "close",
			err: fmt.Errorf("close migrations directory: %w", &os.PathError{
				Op: "close", Path: "missing", Err: syscall.ENOENT,
			}),
		},
		{
			name: "unrelated missing stat",
			err: fmt.Errorf("open migrations directory: %w", &os.PathError{
				Op: "stat", Path: "missing", Err: syscall.ENOENT,
			}),
		},
		{
			name: "different direct-open path",
			err: fmt.Errorf("open migrations directory: %w", &os.PathError{
				Op: "openat", Path: "other", Err: syscall.ENOENT,
			}),
		},
		{
			name: "additional wrapper",
			err:  fmt.Errorf("prepare source: %w", fmt.Errorf("open migrations directory: %w", missingOpen)),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := atlasMigrateLintDirCaptureError("missing", "", test.err)

			c.Assert(got.Error(), qt.Equals, "atlas migrate lint --dir: "+test.err.Error())
			c.Assert(errors.Unwrap(got), qt.Equals, test.err)
			c.Assert(got, qt.ErrorIs, test.err)
		})
	}
}

func TestAtlasMigrateLintPathErrorMatches_SymmetricRootedPaths(t *testing.T) {
	tests := []struct {
		name        string
		path        string
		allowedRoot string
		errorPath   string
		want        bool
	}{
		{
			name:        "both relative",
			path:        "nested/../missing",
			allowedRoot: "/project",
			errorPath:   "missing",
			want:        true,
		},
		{
			name:        "requested absolute and error relative",
			path:        "/project/missing",
			allowedRoot: "/project",
			errorPath:   "missing",
			want:        true,
		},
		{
			name:        "requested relative and error absolute",
			path:        "missing",
			allowedRoot: "/project",
			errorPath:   "/project/missing",
			want:        true,
		},
		{
			name:        "different rooted path",
			path:        "missing",
			allowedRoot: "/project",
			errorPath:   "/other/missing",
			want:        false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(
				atlasMigrateLintPathErrorMatches(test.path, test.allowedRoot, test.errorPath),
				qt.Equals,
				test.want,
			)
		})
	}
}
