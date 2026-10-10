package schemafile_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/coverage"
	"ptah.run/core/objectidentity"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/sqlite/sqlitetable"
	"ptah.run/dialect/timescaledb/tsschema"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/schemafile"
)

// A format that has no syntax for an object family records that it does not
// describe the family, so silence about a live object of that family is not
// read as a request to drop it.
//
// The record carries why. "Unsupported, derived from another fact Ptah holds"
// is the document's FORMAT speaking, not a read that failed and not a
// selection the user wrote, and a user told only that a family was "not
// described" cannot tell those three apart (stokaro/ptah#1346).
func TestAFormatThatCannotExpressAKindSaysSoAndSaysWhy(t *testing.T) {
	tests := []struct {
		name     string
		file     string
		contents string
		want     []coverage.Object
	}{
		{
			// HCL has the synonym and extended_property blocks
			// (stokaro/ptah#1031), and their owners' coverage says so -- and it still cannot
			// name a table's row deletion policy, a changefeed, or a YDB
			// resource pool or classifier. A secret, a topic, an external
			// object, an async replication, a transfer, a column family and a
			// SQLite virtual table are owned features the format makes no
			// claim about.
			name:     "HCL cannot name a TTL, a changefeed, a resource pool or classifier",
			file:     "schema.hcl",
			contents: "schema \"main\" {\n}\n",
			want:     unsupportedRecords(coverage.Changefeed, coverage.ColumnTable, coverage.TTL),
		},
		{
			// The control on the common families. Synonyms, extended
			// properties and virtual tables are owned features the format
			// makes no common claim about, so it records nothing at all.
			name:     "SQL can",
			file:     "schema.sql",
			contents: "CREATE TABLE users (id INTEGER PRIMARY KEY);\n",
			want:     nil,
		},
		{
			// YAML expresses the fewest families of the three, and the row is
			// what keeps the HCL narrowing from being read as "the loader no
			// longer records these kinds anywhere". It has keys for a row
			// deletion policy, a changefeed and a column family, as `.sql` has
			// Spanner's policy clause, so both rows are the control on the TTL,
			// changefeed and column family records HCL and DBML carry.
			name:     "YAML cannot name four families",
			file:     "schema.yaml",
			contents: "tables:\n  users:\n    fields:\n      id:\n        type: INTEGER\n",
			want: unsupportedRecords(
				coverage.Composite, coverage.Domain,
				coverage.Range, coverage.Sequence),
		},
		{
			// DBML declares the widest boundary of any format here, and that is
			// the point rather than an accident: it describes tables, columns,
			// enums, indexes and references and has no syntax for anything
			// else. Extension, Policy and Role appear in no other row, which is
			// what makes this one the exhaustive boundary #2065 asks for --
			// and coverage.Schema is absent from it because DBML qualifies a
			// name with a schema.
			name:     "DBML records unsupported object families",
			file:     "schema.dbml",
			contents: "Table users {\n  id integer [pk]\n}\n",
			want: unsupportedRecords(
				coverage.Changefeed, coverage.ColumnTable, coverage.Composite,
				coverage.Domain, coverage.Extension,
				coverage.Policy, coverage.Range, coverage.Role, coverage.Sequence,
				coverage.TTL),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			path := filepath.Join(t.TempDir(), test.file)
			c.Assert(os.WriteFile(path, []byte(test.contents), 0o600), qt.IsNil)

			database, err := schemafile.LoadPath(path, schemafile.Options{YAML: builtintest.Runtime().YAML()})

			c.Assert(err, qt.IsNil)
			c.Assert(database.NotDescribed.Objects, qt.DeepEquals, test.want)
		})
	}
}

// unsupportedRecords is the shape every limit this loader records has: the
// format cannot express the family, which follows from which format it is.
// Listing the kinds rather than asserting a length is what makes a limit added
// to the wrong branch visible.
func unsupportedRecords(kinds ...coverage.Kind) []coverage.Object {
	records := make([]coverage.Object, 0, len(kinds))
	for _, kind := range kinds {
		records = append(records, coverage.Object{
			Kind:       kind,
			Reason:     coverage.Unsupported,
			Provenance: coverage.DerivedFromFact,
		})
	}
	return records
}

// TestOnlyAFormatThatDeclaresSecretsClaimsTheirNamespace holds the secret
// namespace to the formats that can declare a secret. YAML and YQL claim it
// complete, so a secret only the database holds is planned for removal; HCL,
// DBML and SQL of another dialect make no claim, so applying one keeps every
// secret the database holds instead of planning DROP SECRET, which loses a
// value nothing can read back.
func TestOnlyAFormatThatDeclaresSecretsClaimsTheirNamespace(t *testing.T) {
	tests := []struct {
		name     string
		file     string
		dialect  string
		contents string
		want     schemaext.KnowledgeState
	}{
		{name: "YAML", file: "schema.yaml", contents: "secrets:\n  pw:\n    value_env: PTAH_SECRET_PW\n", want: schemaext.Complete},
		{name: "YQL", file: "schema.sql", dialect: "ydb", contents: "CREATE TABLE t (id Int64 NOT NULL, PRIMARY KEY (id));\n", want: schemaext.Complete},
		{name: "HCL", file: "schema.hcl", contents: "schema \"main\" {\n}\n", want: schemaext.Uninspected},
		{name: "DBML", file: "schema.dbml", contents: "Table users {\n  id integer [pk]\n}\n", want: schemaext.Uninspected},
		{name: "SQL", file: "schema.sql", contents: "CREATE TABLE users (id INTEGER PRIMARY KEY);\n", want: schemaext.Uninspected},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			path := filepath.Join(t.TempDir(), test.file)
			c.Assert(os.WriteFile(path, []byte(test.contents), 0o600), qt.IsNil)

			database, err := schemafile.LoadPath(path, schemafile.Options{YAML: builtintest.Runtime().YAML(), Dialect: test.dialect})

			c.Assert(err, qt.IsNil)
			c.Assert(database.FeatureCoverage.Lookup(ydbsecret.Kind, ydbsecret.Ref("", "held")).State, qt.Equals, test.want)
		})
	}
}

// TestOnlyASQLiteSQLDocumentDescribesVirtualTables holds the virtual table
// coverage to the one format that can declare one. A SQLite SQL document
// claims it complete, so leaving out a live virtual table asks for its
// removal; HCL, YAML, DBML and SQL of another dialect make no claim, so
// applying one keeps every virtual table the database holds instead of
// planning a DROP TABLE that deletes the index and everything in it
// (stokaro/ptah#1028).
func TestOnlyASQLiteSQLDocumentDescribesVirtualTables(t *testing.T) {
	tests := []struct {
		name     string
		file     string
		dialect  string
		contents string
		want     schemaext.KnowledgeState
	}{
		{name: "SQLite SQL", file: "schema.sql", dialect: "sqlite", contents: "CREATE TABLE users (id INTEGER PRIMARY KEY);\n", want: schemaext.Complete},
		{name: "SQLite SQL whose header declines them", file: "schema.sql", dialect: "sqlite",
			contents: "-- ptah:not-described virtual_table\nCREATE TABLE users (id INTEGER PRIMARY KEY);\n", want: schemaext.Uninspected},
		{name: "PostgreSQL SQL", file: "schema.sql", dialect: "postgres", contents: "CREATE TABLE users (id INTEGER PRIMARY KEY);\n", want: schemaext.Uninspected},
		{name: "HCL", file: "schema.hcl", dialect: "sqlite", contents: "schema \"main\" {\n}\n", want: schemaext.Uninspected},
		{name: "YAML", file: "schema.yaml", dialect: "sqlite", contents: "tables:\n  users:\n    fields:\n      id:\n        type: INTEGER\n", want: schemaext.Uninspected},
		{name: "DBML", file: "schema.dbml", dialect: "sqlite", contents: "Table users {\n  id integer [pk]\n}\n", want: schemaext.Uninspected},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			path := filepath.Join(t.TempDir(), test.file)
			c.Assert(os.WriteFile(path, []byte(test.contents), 0o600), qt.IsNil)

			database, err := schemafile.LoadPath(path, schemafile.Options{YAML: builtintest.Runtime().YAML(), Dialect: test.dialect})

			c.Assert(err, qt.IsNil)
			table := objectidentity.ID{Kind: objectidentity.KindTable, Name: objectidentity.Part{Source: "docs", Normalized: "docs"}}
			c.Assert(database.FeatureCoverage.Lookup(sqlitetable.VirtualKind, table).State, qt.Equals, test.want)
		})
	}
}

// TestASQLiteSQLDocumentRefusesANamedVirtualTableLimit refuses a header that
// declines one virtual table by name: the directive covers the kind, and
// widening it to every virtual table or matching a name the comparison may
// spell differently would each decide something the author did not write.
func TestASQLiteSQLDocumentRefusesANamedVirtualTableLimit(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(t.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte("-- ptah:not-described virtual_table \"docs\"\nCREATE TABLE users (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)

	database, err := schemafile.LoadPath(path, schemafile.Options{YAML: builtintest.Runtime().YAML(), Dialect: "sqlite"})

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `(?s).*ptah:not-described virtual_table names "docs"; the directive takes no name in a SQLite document.*`)
	c.Assert(database, qt.IsNil)
}

// TestTimescaleCoverageFollowsWhatAFormatCanName pins which formats claim to
// describe TimescaleDB state. HCL has a block for each model, so a document
// naming neither describes a database without either. The other formats have
// no syntax for them, and claim nothing: a comparison then keeps what the
// server holds rather than planning its removal.
func TestTimescaleCoverageFollowsWhatAFormatCanName(t *testing.T) {
	tests := []struct {
		file     string
		contents string
		want     schemaext.KnowledgeState
	}{
		{file: "schema.hcl", contents: "schema \"main\" {\n}\n", want: schemaext.Complete},
		{file: "schema.sql", contents: "CREATE TABLE users (id INTEGER PRIMARY KEY);\n", want: schemaext.Uninspected},
		{file: "schema.yaml", contents: "tables:\n  users:\n    fields:\n      id:\n        type: INTEGER\n", want: schemaext.Uninspected},
		{file: "schema.dbml", contents: "Table users {\n  id integer [pk]\n}\n", want: schemaext.Uninspected},
	}

	for _, test := range tests {
		t.Run(test.file, func(t *testing.T) {
			c := qt.New(t)
			path := filepath.Join(t.TempDir(), test.file)
			c.Assert(os.WriteFile(path, []byte(test.contents), 0o600), qt.IsNil)

			database, err := schemafile.LoadPath(path, schemafile.Options{YAML: builtintest.Runtime().YAML()})

			c.Assert(err, qt.IsNil)
			for _, kind := range []schemaext.Kind{tsschema.HypertableKind, tsschema.ContinuousAggregateKind} {
				c.Assert(database.FeatureCoverage.Lookup(kind, tsschema.ContinuousAggregateRef("", "users")).State, qt.Equals, test.want, qt.Commentf("%s", kind))
			}
		})
	}
}
