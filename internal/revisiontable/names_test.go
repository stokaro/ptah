package revisiontable_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/internal/revisiontable"
)

// sqlLiterals renders names the way a reader's NOT IN list spells them.
func sqlLiterals(names []string) string {
	literals := make([]string, len(names))
	for i, name := range names {
		literals[i] = "'" + strings.ReplaceAll(name, "'", "''") + "'"
	}
	return strings.Join(literals, ", ")
}

// TestSQLNamesMatchTheNameLists keeps the constants the readers' queries use
// equal to the lists the Go-side filters use, so neither can name a table the
// other does not.
func TestSQLNamesMatchTheNameLists(t *testing.T) {
	c := qt.New(t)
	c.Assert(revisiontable.NativeSQLNames, qt.Equals, sqlLiterals(revisiontable.NativeNames()))
	c.Assert(revisiontable.DefaultSQLNames, qt.Equals, sqlLiterals(revisiontable.DefaultNames()))
}

// TestIsNative answers for Ptah's own tables under their default names, the
// tags table included, and for nothing else, the Atlas revision table
// included: the community binary lists that one from `schema inspect`.
func TestIsNative(t *testing.T) {
	tests := []struct {
		name string
		want bool
	}{
		{name: "schema_migrations", want: true},
		{name: "schema_migrations_log", want: true},
		{name: "ptah_migration_tags", want: true},
		{name: "atlas_schema_revisions", want: false},
		{name: "custom_revs", want: false},
		{name: "SCHEMA_MIGRATIONS", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(revisiontable.IsNative(test.name), qt.Equals, test.want)
		})
	}
}

// TestIsDefault adds the Atlas revision table to the native set.
func TestIsDefault(t *testing.T) {
	tests := []struct {
		name string
		want bool
	}{
		{name: "schema_migrations", want: true},
		{name: "schema_migrations_log", want: true},
		{name: "atlas_schema_revisions", want: true},
		{name: "ptah_migration_tags", want: true},
		{name: "custom_revs", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(revisiontable.IsDefault(test.name), qt.Equals, test.want)
		})
	}
}

// TestConfigured names what the migrator writes under each setting: the
// revision table, for the native format its log beside it, and in either format
// the tags table, whose name no setting changes.
func TestConfigured(t *testing.T) {
	tests := []struct {
		name   string
		format string
		table  string
		want   []string
	}{
		{name: "native default", format: "ptah", want: []string{"schema_migrations", "schema_migrations_log", "ptah_migration_tags"}},
		{name: "empty format is native", format: "", want: []string{"schema_migrations", "schema_migrations_log", "ptah_migration_tags"}},
		{name: "native custom table", format: "ptah", table: "custom_revs", want: []string{"custom_revs", "custom_revs_log", "ptah_migration_tags"}},
		{name: "atlas default", format: "atlas", want: []string{"atlas_schema_revisions", "ptah_migration_tags"}},
		{name: "atlas custom table", format: "atlas", table: "revs", want: []string{"revs", "ptah_migration_tags"}},
		{name: "format spelling", format: " Atlas ", table: " revs ", want: []string{"revs", "ptah_migration_tags"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(revisiontable.Configured(test.format, test.table), qt.DeepEquals, test.want)
		})
	}
}

// TestWithout removes the named tables with the indexes and constraints on
// them, comparing names without regard to case, and leaves the rest and the
// input as they were.
func TestWithout(t *testing.T) {
	c := qt.New(t)
	schema := &catalog.Database{
		Tables: []catalog.Table{{Name: "asn"}, {Name: "SCHEMA_MIGRATIONS"}, {Name: "custom_revs_log"}},
		Indexes: []catalog.Index{
			{Name: "asn_n", TableName: "asn"},
			{Name: "schema_migrations_pkey", TableName: "schema_migrations"},
		},
		Constraints: []catalog.Constraint{
			{Name: "asn_pkey", TableName: "asn"},
			{Name: "log_pkey", TableName: "custom_revs_log"},
		},
	}

	got := revisiontable.Without(schema, []string{"schema_migrations", "custom_revs_log"})

	c.Assert(got.Tables, qt.DeepEquals, []catalog.Table{{Name: "asn"}})
	c.Assert(got.Indexes, qt.DeepEquals, []catalog.Index{{Name: "asn_n", TableName: "asn"}})
	c.Assert(got.Constraints, qt.DeepEquals, []catalog.Constraint{{Name: "asn_pkey", TableName: "asn"}})
	c.Assert(schema.Tables, qt.HasLen, 3)
}

// TestWithoutANilSchema answers an empty schema.
func TestWithoutANilSchema(t *testing.T) {
	c := qt.New(t)
	c.Assert(revisiontable.Without(nil, revisiontable.NativeNames()), qt.DeepEquals, &catalog.Database{})
}
