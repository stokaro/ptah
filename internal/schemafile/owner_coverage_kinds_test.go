package schemafile_test

import (
	"os"
	"path/filepath"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/coverage"
	"ptah.run/dialect/spanner/spannerschema"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/schemafile"
)

// ownerCoverageKinds is every coverage kind an owner registers for a document
// header, which the bundled runtime selects.
func ownerCoverageKinds() []coverage.Kind {
	return slices.Concat(ydbschema.CoverageKinds(), spannerschema.CoverageKinds())
}

// ownerKindDocument writes one named record of kind, encoded the way
// coverage.Set writes a directive, in the leading comment header of a SQL or
// an HCL document.
func ownerKindDocument(c *qt.C, format string, kind coverage.Kind) (path, directive string) {
	c.Helper()
	directive = coverage.Set{}.With(coverage.Object{Kind: kind, Name: "events"}).Directives()[0]
	dir := c.TempDir()
	if format == "SQL" {
		path = filepath.Join(dir, "schema.sql")
		c.Assert(os.WriteFile(path, []byte("-- "+directive+"\nCREATE TABLE t (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
		return path, directive
	}
	path = filepath.Join(dir, "schema.hcl")
	c.Assert(os.WriteFile(path, []byte("// "+directive+"\nschema \"main\" {\n}\n"), 0o600), qt.IsNil)
	return path, directive
}

// TestOwnerCoverageKinds_RoundTripThroughEachHeader reads every owner kind
// back through the SQL and HCL headers with the bundled runtime's vocabulary,
// as the record that was written. DBML has no header, and the kinds it
// records as format limits are pinned beside the other formats' limits.
func TestOwnerCoverageKinds_RoundTripThroughEachHeader(t *testing.T) {
	runtime := builtintest.Runtime()
	for _, kind := range ownerCoverageKinds() {
		for _, format := range []string{"SQL", "HCL"} {
			t.Run(format+" "+string(kind), func(t *testing.T) {
				c := qt.New(t)
				path, directive := ownerKindDocument(c, format, kind)

				database, err := schemafile.LoadPath(path, schemafile.Options{YAML: runtime.YAML(), CoverageVocabulary: runtime.CoverageVocabulary()})

				c.Assert(err, qt.IsNil)
				c.Assert(database.NotDescribed.Directives(), qt.Contains, directive)
			})
		}
	}
}

// TestOwnerCoverageKinds_RefusedWithoutTheirOwner refuses each owner kind by
// name where the run's vocabulary does not hold it, rather than reading the
// header as one that makes no claim.
func TestOwnerCoverageKinds_RefusedWithoutTheirOwner(t *testing.T) {
	runtime := builtintest.Runtime()
	for _, kind := range ownerCoverageKinds() {
		for _, format := range []string{"SQL", "HCL"} {
			t.Run(format+" "+string(kind), func(t *testing.T) {
				c := qt.New(t)
				path, _ := ownerKindDocument(c, format, kind)

				database, err := schemafile.LoadPath(path, schemafile.Options{YAML: runtime.YAML()})

				c.Assert(err, qt.ErrorMatches, `(?s).*unknown coverage kind "`+string(kind)+`".*`)
				c.Assert(database, qt.IsNil)
			})
		}
	}
}
