package atlas

// White-box testing required: displayAtlasSchemaApplyError is the unexported
// compatibility-boundary adapter for one exact diagnostic prefix. Driving its
// error-identity branches through the exported command requires filesystem and
// SQLite boundaries, which belong to the tagged integration contour.

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestDisplayAtlasSchemaApplyError(t *testing.T) {
	t.Run("exact HCL loader context is hidden", func(t *testing.T) {
		c := qt.New(t)
		sentinel := errors.New("sentinel")
		sourceErr := fmt.Errorf(
			"load --to schema: parse HCL schema: malformed schema: %w",
			sentinel,
		)

		got := displayAtlasSchemaApplyError(sourceErr, nil)

		c.Assert(got.Error(), qt.Equals, "malformed schema: sentinel")
		c.Assert(got, qt.ErrorIs, sentinel)
		c.Assert(errors.Unwrap(got), qt.Equals, sourceErr)
	})

	t.Run("other errors pass through", func(t *testing.T) {
		tests := []struct {
			name    string
			message string
		}{
			{
				name:    "unrelated loader context",
				message: "load --to schema: schema file does not exist",
			},
			{
				name:    "near match",
				message: "load --to schema: parse SQL schema: malformed schema",
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				c := qt.New(t)
				sentinel := errors.New("sentinel")
				sourceErr := fmt.Errorf("%s: %w", test.message, sentinel)

				got := displayAtlasSchemaApplyError(sourceErr, nil)

				c.Assert(got.Error(), qt.Equals, test.message+": sentinel")
				c.Assert(got, qt.ErrorIs, sentinel)
				c.Assert(got, qt.Equals, sourceErr)
			})
		}
	})
}

// TestAtlasSchemaApplyRelativeDiagnostic covers cell 9.13's second half: the
// pinned community binary v1.3.0 echoes the desired-schema path in the form the
// operator wrote it, and this surface resolved every form to an absolute path.
//
// Every case is anchored on a real working directory so the absolute form the
// loader produces is the absolute form the assertion expects. The rows that
// must NOT rewrite are the point of the table: an absolute --to already agreed
// with the pinned binary before this adapter existed, and widening the rewrite
// to cover it would break a cell that was passing.
func TestAtlasSchemaApplyRelativeDiagnostic(t *testing.T) {
	const diagnostic = ":5,15-16: Unclosed configuration block; there is no closing brace."

	workdir := t.TempDir()
	// The temporary directory may itself be a symlink on macOS; the adapter
	// compares against filepath.Abs of a relative path resolved from the
	// process working directory, so anchor the expectations the same way.
	t.Chdir(workdir)
	resolved, err := filepath.Abs(".")
	qt.Assert(t, err, qt.IsNil)

	tests := []struct {
		name    string
		toURLs  []string
		message string
		want    string
	}{
		{
			name:    "relative file URL is rendered relative",
			toURLs:  []string{"file://fx/bad.hcl"},
			message: filepath.Join(resolved, "fx/bad.hcl") + diagnostic,
			want:    filepath.Join("fx", "bad.hcl") + diagnostic,
		},
		{
			name:    "dot-relative file URL is normalized, matching the oracle",
			toURLs:  []string{"file://./fx/bad.hcl"},
			message: filepath.Join(resolved, "fx/bad.hcl") + diagnostic,
			want:    filepath.Join("fx", "bad.hcl") + diagnostic,
		},
		{
			name:    "nested relative path keeps its segments",
			toURLs:  []string{"file://fx/sub/bad.hcl"},
			message: filepath.Join(resolved, "fx/sub/bad.hcl") + diagnostic,
			want:    filepath.Join("fx", "sub", "bad.hcl") + diagnostic,
		},
		{
			name:    "bare relative path with no scheme",
			toURLs:  []string{"fx/bad.hcl"},
			message: filepath.Join(resolved, "fx/bad.hcl") + diagnostic,
			want:    filepath.Join("fx", "bad.hcl") + diagnostic,
		},
		{
			name:    "the matching value is found past a non-local one",
			toURLs:  []string{"sqlite://dev?mode=memory", "file://fx/bad.hcl"},
			message: filepath.Join(resolved, "fx/bad.hcl") + diagnostic,
			want:    filepath.Join("fx", "bad.hcl") + diagnostic,
		},
		{
			name:    "absolute --to is left absolute",
			toURLs:  []string{"file://" + filepath.Join(resolved, "fx/bad.hcl")},
			message: filepath.Join(resolved, "fx/bad.hcl") + diagnostic,
			want:    filepath.Join(resolved, "fx/bad.hcl") + diagnostic,
		},
		{
			name:    "no --to at all",
			toURLs:  nil,
			message: filepath.Join(resolved, "fx/bad.hcl") + diagnostic,
			want:    filepath.Join(resolved, "fx/bad.hcl") + diagnostic,
		},
		{
			name:    "a diagnostic about a different file is untouched",
			toURLs:  []string{"file://fx/bad.hcl"},
			message: filepath.Join(resolved, "fx/other.hcl") + diagnostic,
			want:    filepath.Join(resolved, "fx/other.hcl") + diagnostic,
		},
		{
			name:    "the path must start the message, not merely appear in it",
			toURLs:  []string{"file://fx/bad.hcl"},
			message: "while reading " + filepath.Join(resolved, "fx/bad.hcl") + diagnostic,
			want:    "while reading " + filepath.Join(resolved, "fx/bad.hcl") + diagnostic,
		},
		{
			name:    "the path must be followed by the position colon",
			toURLs:  []string{"file://fx/bad.hcl"},
			message: filepath.Join(resolved, "fx/bad.hcl") + "-backup:5,15-16: Unclosed.",
			want:    filepath.Join(resolved, "fx/bad.hcl") + "-backup:5,15-16: Unclosed.",
		},
		{
			name:    "a database --to names no local file",
			toURLs:  []string{"sqlite://fx/bad.hcl"},
			message: filepath.Join(resolved, "fx/bad.hcl") + diagnostic,
			want:    filepath.Join(resolved, "fx/bad.hcl") + diagnostic,
		},
		{
			name:    "an empty file URL selects nothing",
			toURLs:  []string{"file://"},
			message: filepath.Join(resolved, "fx/bad.hcl") + diagnostic,
			want:    filepath.Join(resolved, "fx/bad.hcl") + diagnostic,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := atlasSchemaApplyRelativeDiagnostic(test.message, test.toURLs)

			c.Check(got, qt.Equals, test.want)
		})
	}
}

// TestDisplayAtlasSchemaApplyErrorRendersRelativePath joins the two halves of
// cell 9.13: the loader context is dropped AND the path is rendered as written.
// Asserting them separately would let a change satisfy each half in a build
// where the composed output is still wrong.
func TestDisplayAtlasSchemaApplyErrorRendersRelativePath(t *testing.T) {
	c := qt.New(t)

	workdir := t.TempDir()
	t.Chdir(workdir)
	resolved, err := filepath.Abs(".")
	c.Assert(err, qt.IsNil)

	sentinel := errors.New("Unclosed configuration block")
	sourceErr := fmt.Errorf(
		"load --to schema: parse HCL schema: %s:5,15-16: %w",
		filepath.Join(resolved, "fx/bad.hcl"),
		sentinel,
	)

	got := displayAtlasSchemaApplyError(sourceErr, []string{"file://fx/bad.hcl"})

	c.Assert(got.Error(), qt.Equals,
		filepath.Join("fx", "bad.hcl")+":5,15-16: Unclosed configuration block")
	c.Assert(got, qt.ErrorIs, sentinel)
	c.Assert(errors.Unwrap(got), qt.Equals, sourceErr)
}
