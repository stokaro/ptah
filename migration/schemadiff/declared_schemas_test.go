package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

// compareDeclaredSchemas compares a desired state declaring schemas with a read
// holding the named ones, on a connection whose unqualified names resolve to
// connected.
func compareDeclaredSchemas(c *qt.C, dialect, connected string, desired *schemamodel.Database, held ...string) []schemamodel.Schema {
	c.Helper()
	current := &catalog.Database{}
	for _, name := range held {
		current.Schemas = append(current.Schemas, catalog.Schema{Name: name})
	}
	semantics := identifier.ForDialect(dialect)
	semantics.DefaultSchema = connected
	diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), desired, current,
		catalog.ServerInfo{Dialect: dialect, Schema: connected, IdentifierSemantics: semantics}, nil, must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)
	c.Assert(diff.SchemasRemoved, qt.IsNil)
	c.Assert(diff.SchemasModified, qt.IsNil)
	return diff.SchemasAdded
}

// TestCompareDeclaredSchemas_HappyPath pins which declared schemas a comparison
// adds on PostgreSQL. It adds what the read does not hold, with what the
// declaration says about it, once and in name order. It never removes: the helper
// asserts that a held schema the document does not mention plans no removal.
func TestCompareDeclaredSchemas_HappyPath(t *testing.T) {
	app := schemamodel.Schema{Name: "app"}
	tests := []struct {
		name      string
		connected string
		desired   *schemamodel.Database
		held      []string
		want      []schemamodel.Schema
	}{
		{
			name: "absent", connected: "public",
			desired: &schemamodel.Database{Schemas: []schemamodel.Schema{app}},
			want:    []schemamodel.Schema{app},
		},
		{
			name: "absent, with its comment", connected: "public",
			desired: &schemamodel.Database{Schemas: []schemamodel.Schema{{Name: "app", Comment: "application"}}},
			want:    []schemamodel.Schema{{Name: "app", Comment: "application"}},
		},
		{
			name: "in name order", connected: "public",
			desired: &schemamodel.Database{Schemas: []schemamodel.Schema{{Name: "zeta"}, app}},
			want:    []schemamodel.Schema{app, {Name: "zeta"}},
		},
		{
			name: "declared twice, once", connected: "public",
			desired: &schemamodel.Database{Schemas: []schemamodel.Schema{app, app}},
			want:    []schemamodel.Schema{app},
		},
		{
			name: "held", connected: "public", held: []string{"public", "app"},
			desired: &schemamodel.Database{Schemas: []schemamodel.Schema{app}},
		},
		{
			name: "the schema the connection resolved, which the read does not report", connected: "public",
			desired: &schemamodel.Database{Schemas: []schemamodel.Schema{{Name: "public"}}},
		},
		{
			name: "a connection resolved to another schema", connected: "app",
			desired: &schemamodel.Database{Schemas: []schemamodel.Schema{app}},
		},
		{
			name: "a schema with no name", connected: "public",
			desired: &schemamodel.Database{Schemas: []schemamodel.Schema{{Name: "  "}}},
		},
		{
			name: "recorded as not described", connected: "public",
			desired: &schemamodel.Database{Schemas: []schemamodel.Schema{app}, NotDescribed: coverage.Set{}.WithObject(coverage.Schema, "app")},
		},
		{
			name: "another schema recorded as not described", connected: "public",
			desired: &schemamodel.Database{Schemas: []schemamodel.Schema{app}, NotDescribed: coverage.Set{}.WithObject(coverage.Schema, "crm")},
			want:    []schemamodel.Schema{app},
		},
		{
			name: "a schema an object only names, which comes with the object", connected: "public",
			desired: &schemamodel.Database{Tables: []schemamodel.Table{{StructName: "T", Name: "t", Schema: "app"}}},
		},
		{
			name: "held and not mentioned", connected: "public", held: []string{"public", "app"},
			desired: &schemamodel.Database{},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(compareDeclaredSchemas(c, platform.Postgres, test.connected, test.desired, test.held...), qt.DeepEquals, test.want)
		})
	}
}

// TestCompareDeclaredSchemas_ByEngine pins which engines the rule reaches. Where a
// schema is a namespace, it does. MySQL and MariaDB answer through a whole-server
// comparison and refuse a document naming another database under a URL naming
// one. ClickHouse's planner has no statement for it, and SQLite and Oracle have
// no schema to create.
func TestCompareDeclaredSchemas_ByEngine(t *testing.T) {
	app := schemamodel.Schema{Name: "app"}
	tests := []struct {
		dialect string
		want    []schemamodel.Schema
	}{
		{dialect: platform.Postgres, want: []schemamodel.Schema{app}},
		{dialect: platform.CockroachDB, want: []schemamodel.Schema{app}},
		{dialect: platform.YugabyteDB, want: []schemamodel.Schema{app}},
		{dialect: platform.Spanner, want: []schemamodel.Schema{app}},
		{dialect: platform.MySQL},
		{dialect: platform.MariaDB},
		{dialect: platform.SQLServer, want: []schemamodel.Schema{app}},
		{dialect: platform.ClickHouse},
		{dialect: platform.SQLite},
		{dialect: platform.Oracle},
	}
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			got := compareDeclaredSchemas(c, test.dialect, "default_schema",
				&schemamodel.Database{Schemas: []schemamodel.Schema{app}})
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}
