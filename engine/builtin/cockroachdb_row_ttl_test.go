package builtin_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/goschema"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/cockroachdb/crdbast"
	"ptah.run/dialect/cockroachdb/crdbdiff"
	"ptah.run/dialect/cockroachdb/crdbschema"
	"ptah.run/engine/builtin"
)

// TestCockroachDBRowTTL_OwnerSpellsTheTableAsTheRenderer pins the agreement
// between two spellings of one table name: the owner's ALTER statement and the
// PostgreSQL-family renderer's own ALTER in the same batch. They are written by
// different code, so a name with a dot inside quotes, an embedded quote or a
// non-ASCII letter is where they would part.
func TestCockroachDBRowTTL_OwnerSpellsTheTableAsTheRenderer(t *testing.T) {
	for _, table := range []string{"sessions", "audit.sessions", `"a.b"."c"`, `odd"name`, "Größe"} {
		t.Run(table, func(t *testing.T) {
			c := qt.New(t)
			node := &ast.AlterTableNode{Name: table, Operations: []ast.AlterOperation{
				&ast.DropColumnOperation{ColumnName: "old"},
				&ast.ExtensionAlterOperation{Payload: &crdbast.AlterRowTTL{Change: crdbdiff.RowTTL{
					After: &crdbschema.DesiredRowTTL{Policy: crdbschema.Policy{ExpirationExpression: "expires_at"}},
				}}},
			}}
			sql, err := builtin.RenderSQLWithCapabilities("cockroachdb", capability.CockroachDB26(), node)
			c.Assert(err, qt.IsNil)
			c.Assert(tableSpellings(sql), qt.HasLen, 1, qt.Commentf("statements: %s", sql))
		})
	}
}

// tableSpellings collects the distinct table names the ALTER TABLE statements
// of a render spell, whatever clause follows the name.
func tableSpellings(sql string) []string {
	seen := make(map[string]bool)
	var result []string
	for line := range strings.SplitSeq(sql, "\n") {
		rest, found := strings.CutPrefix(line, "ALTER TABLE ")
		if !found {
			continue
		}
		name, _, _ := strings.Cut(rest, " SET (")
		name, _, _ = strings.Cut(name, " DROP COLUMN")
		if !seen[name] {
			seen[name] = true
			result = append(result, name)
		}
	}
	return result
}

// TestCockroachDBRowTTL_PlatformPropertiesFollowTheTarget pins the source
// spelling: platform.cockroachdb properties render on CockroachDB and are
// scoped away from every other target, as every platform property is.
func TestCockroachDBRowTTL_PlatformPropertiesFollowTheTarget(t *testing.T) {
	database := must.Must(goschema.ParseSource("sessions.go", `package entities

//ptah:schema:table name="sessions" platform.cockroachdb.ttl_expiration_expression="expires_at + INTERVAL '1 day'" platform.cockroachdb.ttl_select_batch_size="500"
type Session struct {
	//ptah:schema:field name="id" type="INT8" primary="true"
	ID int64
	//ptah:schema:field name="expires_at" type="TIMESTAMPTZ"
	ExpiresAt string
}
`))
	tests := []struct {
		dialect string
		caps    capability.Capabilities
		want    string
	}{
		{dialect: "cockroachdb", caps: capability.CockroachDB26(), want: ` WITH (ttl_expiration_expression = 'expires_at + INTERVAL ''1 day''', ttl_select_batch_size = 500);`},
		{dialect: "postgres", caps: capability.Postgres17(), want: ""},
		{dialect: "yugabytedb", caps: capability.YugabyteDB25(), want: ""},
	}
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(&database, test.dialect, test.caps)
			c.Assert(err, qt.IsNil)
			rendered := strings.Join(statements, "\n")
			c.Assert(strings.Contains(rendered, "ttl_"), qt.Equals, test.want != "")
			c.Assert(rendered, qt.Contains, test.want)
		})
	}
}

// TestCockroachDBRowTTL_AnUnboundPolicyIsRefusedElsewhere pins the other
// half: a policy built in Go without a target binding claims every target, and
// a target without an owner for it refuses rather than drops it.
func TestCockroachDBRowTTL_AnUnboundPolicyIsRefusedElsewhere(t *testing.T) {
	database := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Session", Name: "sessions", Facets: must.Must(schemaext.NewFacets(
			&crdbschema.DesiredRowTTL{Policy: crdbschema.Policy{ExpirationExpression: "expires_at"}}))}},
		Fields: []schemamodel.Field{{StructName: "Session", Name: "id", Type: "INT8", Primary: true}},
	}
	for _, dialect := range []string{"postgres", "yugabytedb", "mysql", "sqlite"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(database, dialect, capability.ForDialect(dialect))
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `(?s).*ptah.run/cockroachdb/row-ttl.*`)
			c.Assert(statements, qt.IsNil)
		})
	}
}
