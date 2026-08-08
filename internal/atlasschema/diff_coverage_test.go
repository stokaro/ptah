package atlasschema_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlasschema"
)

// `schema diff` is the one verb whose CURRENT side can be a document rather
// than a database, so it is the only place a `// ptah:not-described` record can
// gate a creation. It gated too much: an object the --to document explicitly
// declared was deleted from the plan, and the command then printed the same
// words it prints for two identical schemas.
//
//	--from: // ptah:not-described extension
//	--to:   extension "citext" { if_not_exists = true }
//	stdout: Schemas are synced, no changes to be made.
//	stderr: (empty)                                       exit 0
//
// measured on PostgreSQL 17.10, where the same command with the record stripped
// from --from printed `CREATE EXTENSION IF NOT EXISTS "citext";`
// (stokaro/ptah#1276).

const coverageDiffFrom = `
schema "public" {
}
table "users" {
  schema = schema.public
  column "id" {
    type = int
  }
}
`

// TestDiffPlansAGuardedCreationTheCurrentDocumentCouldNotRuleOut is the refuted
// shape and its control in one table: the only difference between the two rows
// is the record on --from, and the answer must not move.
func TestDiffPlansAGuardedCreationTheCurrentDocumentCouldNotRuleOut(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name string
		from string
	}{
		{
			name: "--from records that it does not describe extensions",
			from: "// ptah:not-described extension\n" + coverageDiffFrom,
		},
		{
			name: "control: --from with the record stripped",
			from: coverageDiffFrom,
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			var diagnostics bytes.Buffer
			report, err := atlasschema.Diff(c.Context(), coverageDiffOptions(c, test.from, coverageDiffFrom+`
extension "citext" {
  if_not_exists = true
}
`, &diagnostics))

			c.Assert(err, qt.IsNil)
			sql, err := report.MarshalSQL()
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, `CREATE EXTENSION IF NOT EXISTS "citext";`)
			c.Assert(diagnostics.String(), qt.Equals, "")
		})
	}
}

// TestDiffNamesAnUnguardedCreationItWithheld covers the half that stays
// withheld. `CREATE SEQUENCE` has no guard unless the author asked for one, and
// running it against a sequence that already exists fails the migration -- so
// the statement is not planned, and the command says which object it is holding
// back instead of leaving stderr empty.
func TestDiffNamesAnUnguardedCreationItWithheld(t *testing.T) {
	c := qt.New(t)
	var diagnostics bytes.Buffer

	report, err := atlasschema.Diff(c.Context(), coverageDiffOptions(c,
		"// ptah:not-described sequence\n"+coverageDiffFrom,
		coverageDiffFrom+`
sequence "order_seq" {
  schema = schema.public
  type = "bigint"
}
`, &diagnostics))

	c.Assert(err, qt.IsNil)
	c.Assert(report.Changes, qt.HasLen, 0)
	c.Assert(diagnostics.String(), qt.Matches,
		`Warning: sequence "public\.order_seq" is declared by --to but no change was planned for it:`+
			" --from records `ptah:not-described sequence`.*no IF NOT EXISTS guard\\.\n")
}

// TestDiffPlansAnUnguardedCreationWithoutTheRecord is the control for the test
// above. Without it a comparator that never planned a sequence at all would
// pass, and the warning would be describing something that was not happening.
func TestDiffPlansAnUnguardedCreationWithoutTheRecord(t *testing.T) {
	c := qt.New(t)
	var diagnostics bytes.Buffer

	report, err := atlasschema.Diff(c.Context(), coverageDiffOptions(c, coverageDiffFrom, coverageDiffFrom+`
sequence "order_seq" {
  schema = schema.public
  type = "bigint"
}
`, &diagnostics))

	c.Assert(err, qt.IsNil)
	sql, err := report.MarshalSQL()
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `CREATE SEQUENCE "public"."order_seq" AS bigint;`)
	c.Assert(diagnostics.String(), qt.Equals, "")
}

// TestDiffStillSuppressesARemovalTheDesiredDocumentDoesNotDescribe is the
// original defect, re-measured at this surface from the other side. The
// additive refinement must not have reached across into the removal half: a
// document that declares it does not describe extensions still does not ask for
// one to be dropped.
func TestDiffStillSuppressesARemovalTheDesiredDocumentDoesNotDescribe(t *testing.T) {
	c := qt.New(t)
	from := coverageDiffFrom + `
extension "pgcrypto" {
  if_not_exists = true
}
`

	c.Run("--to records that it does not describe extensions", func(c *qt.C) {
		var diagnostics bytes.Buffer

		report, err := atlasschema.Diff(c.Context(), coverageDiffOptions(c, from,
			"// ptah:not-described extension\n"+coverageDiffFrom, &diagnostics))

		c.Assert(err, qt.IsNil)
		c.Assert(report.Changes, qt.HasLen, 0)
		c.Assert(diagnostics.String(), qt.Equals, "")
	})

	c.Run("control: --to with the record stripped", func(c *qt.C) {
		var diagnostics bytes.Buffer

		report, err := atlasschema.Diff(c.Context(), coverageDiffOptions(c, from, coverageDiffFrom, &diagnostics))

		c.Assert(err, qt.IsNil)
		sql, err := report.MarshalSQL()
		c.Assert(err, qt.IsNil)
		c.Assert(sql, qt.Contains, `DROP EXTENSION IF EXISTS "pgcrypto";`)
	})
}

func coverageDiffOptions(c *qt.C, from, to string, diagnostics *bytes.Buffer) atlasschema.DiffOptions {
	dir := c.TempDir()
	fromPath := filepath.Join(dir, "from.hcl")
	toPath := filepath.Join(dir, "to.hcl")
	c.Assert(os.WriteFile(fromPath, []byte(from), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(toPath, []byte(to), 0o600), qt.IsNil)
	return atlasschema.DiffOptions{
		FromURLs:    []string{"file://" + fromPath},
		ToURLs:      []string{"file://" + toPath},
		DevURL:      "postgres://localhost/dev",
		Diagnostics: diagnostics,
	}
}
