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
// gate a creation. A guarded extension addition is still undecidable now that
// installation schema is modeled: an extension may already exist in another
// schema, where CREATE EXTENSION IF NOT EXISTS succeeds without moving it
// (stokaro/ptah#1276, stokaro/ptah#1441).

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

func TestDiffNamesAGuardedExtensionPlacementItCannotConfirm(t *testing.T) {
	c := qt.New(t)
	var diagnostics bytes.Buffer

	report, err := atlasschema.Diff(c.Context(), coverageDiffOptions(c,
		"// ptah:not-described extension\n"+coverageDiffFrom,
		coverageDiffFrom+`
extension "citext" {
  if_not_exists = true
  schema        = "extensions"
}
`, &diagnostics))

	c.Assert(err, qt.IsNil)
	c.Assert(report.Changes, qt.HasLen, 0)
	c.Assert(diagnostics.String(), qt.Matches,
		`Warning: extension "citext" is declared by --to but no change was planned for it:`+
			" --from records `ptah:not-described extension`.*cannot safely converge from an unknown current state\\.\n")
}

func TestDiffPlansGuardedExtensionPlacementWhenCurrentIsAuthoritative(t *testing.T) {
	c := qt.New(t)
	var diagnostics bytes.Buffer

	report, err := atlasschema.Diff(c.Context(), coverageDiffOptions(c,
		coverageDiffFrom,
		coverageDiffFrom+`
extension "citext" {
  if_not_exists = true
  schema        = "extensions"
}
`, &diagnostics))

	c.Assert(err, qt.IsNil)
	sql, err := report.MarshalSQL()
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `CREATE EXTENSION IF NOT EXISTS "citext" WITH SCHEMA "extensions";`)
	c.Assert(diagnostics.String(), qt.Equals, "")
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
			" --from records `ptah:not-described sequence`.*cannot safely converge from an unknown current state\\.\n")
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
