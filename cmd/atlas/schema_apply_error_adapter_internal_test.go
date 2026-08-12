package atlas

// White-box testing required: displayAtlasSchemaApplyError is the unexported
// compatibility-boundary adapter for one exact diagnostic prefix. Driving its
// error-identity branches through the exported command requires filesystem and
// SQLite boundaries, which belong to the tagged integration contour.

import (
	"errors"
	"fmt"
	"os"
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
// Every case is anchored on an explicit platform-native working-directory
// string. The rows that
// must NOT rewrite are the point of the table: an absolute --to already agreed
// with the pinned binary before this adapter existed, and widening the rewrite
// to cover it would break a cell that was passing.
func TestAtlasSchemaApplyRelativeDiagnostic(t *testing.T) {
	const diagnostic = ":5,15-16: Unclosed configuration block; there is no closing brace."

	workdir := filepath.Join(os.TempDir(), "ptah-schema-apply-workdir")

	tests := []struct {
		name    string
		paths   []atlasSchemaApplyDiagnosticPath
		message string
		want    string
	}{
		{
			name:    "relative file URL is rendered relative",
			paths:   []atlasSchemaApplyDiagnosticPath{{authored: filepath.Join("fx", "bad.hcl"), resolved: filepath.Join(workdir, "fx/bad.hcl")}},
			message: filepath.Join(workdir, "fx/bad.hcl") + diagnostic,
			want:    filepath.Join("fx", "bad.hcl") + diagnostic,
		},
		{
			name:    "dot-relative file URL is normalized, matching the oracle",
			paths:   []atlasSchemaApplyDiagnosticPath{{authored: filepath.Join("fx", "bad.hcl"), resolved: filepath.Join(workdir, "fx/bad.hcl")}},
			message: filepath.Join(workdir, "fx/bad.hcl") + diagnostic,
			want:    filepath.Join("fx", "bad.hcl") + diagnostic,
		},
		{
			name:    "nested relative path keeps its segments",
			paths:   []atlasSchemaApplyDiagnosticPath{{authored: filepath.Join("fx", "sub", "bad.hcl"), resolved: filepath.Join(workdir, "fx/sub/bad.hcl")}},
			message: filepath.Join(workdir, "fx/sub/bad.hcl") + diagnostic,
			want:    filepath.Join("fx", "sub", "bad.hcl") + diagnostic,
		},
		{
			name:    "decoded path is used for display",
			paths:   []atlasSchemaApplyDiagnosticPath{{authored: filepath.Join("fx", "escaped name.hcl"), resolved: filepath.Join(workdir, "fx/escaped name.hcl")}},
			message: filepath.Join(workdir, "fx/escaped name.hcl") + diagnostic,
			want:    filepath.Join("fx", "escaped name.hcl") + diagnostic,
		},
		{
			name:    "resolved symlink target maps back to the authored path",
			paths:   []atlasSchemaApplyDiagnosticPath{{authored: filepath.Join("fx", "linked.hcl"), resolved: filepath.Join(workdir, "fx/target.hcl")}},
			message: filepath.Join(workdir, "fx/target.hcl") + diagnostic,
			want:    filepath.Join("fx", "linked.hcl") + diagnostic,
		},
		{
			name: "directory member maps beneath the authored directory",
			paths: []atlasSchemaApplyDiagnosticPath{{
				authored:  filepath.Join("schemas"),
				resolved:  filepath.Join(workdir, "schemas"),
				directory: true,
			}},
			message: filepath.Join(workdir, "schemas", "nested", "bad.hcl") + diagnostic,
			want:    filepath.Join("schemas", "nested", "bad.hcl") + diagnostic,
		},
		{
			name: "symlinked directory member maps to its authored entry",
			paths: []atlasSchemaApplyDiagnosticPath{{
				authored:  filepath.Join("schemas"),
				resolved:  filepath.Join(workdir, "schemas"),
				directory: true,
				members: []atlasSchemaApplyDiagnosticPath{{
					authored: filepath.Join("schemas", "bad.hcl"),
					resolved: filepath.Join(workdir, "fixtures", "target.hcl"),
				}},
			}},
			message: filepath.Join(workdir, "fixtures", "target.hcl") + diagnostic,
			want:    filepath.Join("schemas", "bad.hcl") + diagnostic,
		},
		{
			name: "SQL-named symlink to HCL maps to its authored entry",
			paths: []atlasSchemaApplyDiagnosticPath{{
				authored:  filepath.Join("schemas"),
				resolved:  filepath.Join(workdir, "schemas"),
				directory: true,
				members: []atlasSchemaApplyDiagnosticPath{{
					authored: filepath.Join("schemas", "bad.sql"),
					resolved: filepath.Join(workdir, "fixtures", "target.hcl"),
				}},
			}},
			message: filepath.Join(workdir, "fixtures", "target.hcl") + diagnostic,
			want:    filepath.Join("schemas", "bad.sql") + diagnostic,
		},
		{
			name: "directory prefix collision is untouched",
			paths: []atlasSchemaApplyDiagnosticPath{{
				authored:  filepath.Join("schemas"),
				resolved:  filepath.Join(workdir, "schemas"),
				directory: true,
			}},
			message: filepath.Join(workdir, "schemas-other", "bad.hcl") + diagnostic,
			want:    filepath.Join(workdir, "schemas-other", "bad.hcl") + diagnostic,
		},
		{
			name:    "bare relative path with no scheme",
			paths:   []atlasSchemaApplyDiagnosticPath{{authored: filepath.Join("fx", "bad.hcl"), resolved: filepath.Join(workdir, "fx/bad.hcl")}},
			message: filepath.Join(workdir, "fx/bad.hcl") + diagnostic,
			want:    filepath.Join("fx", "bad.hcl") + diagnostic,
		},
		{
			name:    "the matching value is found past a non-local one",
			paths:   []atlasSchemaApplyDiagnosticPath{{authored: filepath.Join("fx", "bad.hcl"), resolved: filepath.Join(workdir, "fx/bad.hcl")}},
			message: filepath.Join(workdir, "fx/bad.hcl") + diagnostic,
			want:    filepath.Join("fx", "bad.hcl") + diagnostic,
		},
		{
			name:    "absolute --to is left absolute",
			paths:   nil,
			message: filepath.Join(workdir, "fx/bad.hcl") + diagnostic,
			want:    filepath.Join(workdir, "fx/bad.hcl") + diagnostic,
		},
		{
			name:    "no --to at all",
			paths:   nil,
			message: filepath.Join(workdir, "fx/bad.hcl") + diagnostic,
			want:    filepath.Join(workdir, "fx/bad.hcl") + diagnostic,
		},
		{
			name:    "a diagnostic about a different file is untouched",
			paths:   []atlasSchemaApplyDiagnosticPath{{authored: filepath.Join("fx", "bad.hcl"), resolved: filepath.Join(workdir, "fx/bad.hcl")}},
			message: filepath.Join(workdir, "fx/other.hcl") + diagnostic,
			want:    filepath.Join(workdir, "fx/other.hcl") + diagnostic,
		},
		{
			name:    "the path must start the message, not merely appear in it",
			paths:   []atlasSchemaApplyDiagnosticPath{{authored: filepath.Join("fx", "bad.hcl"), resolved: filepath.Join(workdir, "fx/bad.hcl")}},
			message: "while reading " + filepath.Join(workdir, "fx/bad.hcl") + diagnostic,
			want:    "while reading " + filepath.Join(workdir, "fx/bad.hcl") + diagnostic,
		},
		{
			name:    "the path must be followed by the position colon",
			paths:   []atlasSchemaApplyDiagnosticPath{{authored: filepath.Join("fx", "bad.hcl"), resolved: filepath.Join(workdir, "fx/bad.hcl")}},
			message: filepath.Join(workdir, "fx/bad.hcl") + "-backup:5,15-16: Unclosed.",
			want:    filepath.Join(workdir, "fx/bad.hcl") + "-backup:5,15-16: Unclosed.",
		},
		{
			name:    "a database --to names no local file",
			paths:   nil,
			message: filepath.Join(workdir, "fx/bad.hcl") + diagnostic,
			want:    filepath.Join(workdir, "fx/bad.hcl") + diagnostic,
		},
		{
			name:    "an empty file URL selects nothing",
			paths:   nil,
			message: filepath.Join(workdir, "fx/bad.hcl") + diagnostic,
			want:    filepath.Join(workdir, "fx/bad.hcl") + diagnostic,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := atlasSchemaApplyRelativeDiagnosticFrom(test.message, test.paths)

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

	workdir := filepath.Join(os.TempDir(), "ptah-schema-apply-workdir")

	sentinel := errors.New("Unclosed configuration block")
	sourceErr := fmt.Errorf(
		"load --to schema: parse HCL schema: %s:5,15-16: %w",
		filepath.Join(workdir, "fx/bad.hcl"),
		sentinel,
	)

	got := displayAtlasSchemaApplyErrorFrom(sourceErr, []atlasSchemaApplyDiagnosticPath{{
		authored: filepath.Join("fx", "bad.hcl"),
		resolved: filepath.Join(workdir, "fx/bad.hcl"),
	}})

	c.Assert(got.Error(), qt.Equals,
		filepath.Join("fx", "bad.hcl")+":5,15-16: Unclosed configuration block")
	c.Assert(got, qt.ErrorIs, sentinel)
	c.Assert(errors.Unwrap(got), qt.Equals, sourceErr)
}
