package builtin_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
)

// bareGeneratedKind is a table whose column c declares kind and no
// expression.
func bareGeneratedKind(kind string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "t", StructName: "T", Engine: "MergeTree"}},
		Fields: []schemamodel.Field{
			{Name: "id", StructName: "T", Type: "Int64", Primary: true},
			{Name: "c", StructName: "T", Type: "Int64", GeneratedKind: kind},
		},
	}
}

// TestRenderBareGeneratedKind_HappyPath renders an EPHEMERAL column declared
// without an expression on ClickHouse, whose grammar lets the kind stand
// alone, with the expression ClickHouse reports for it.
func TestRenderBareGeneratedKind_HappyPath(t *testing.T) {
	c := qt.New(t)

	statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(
		bareGeneratedKind("EPHEMERAL"), platform.ClickHouse, capability.ForDialect(platform.ClickHouse),
	)

	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(statements, "\n"), qt.Contains, "`c` Int64 EPHEMERAL defaultValueOfTypeName('Int64')")
}

// TestRenderBareGeneratedKind_FailurePath refuses a generated kind without an
// expression where the target takes the kind only with one. Rendered, the
// column would be a plain one.
func TestRenderBareGeneratedKind_FailurePath(t *testing.T) {
	tests := []struct {
		name, dialect, kind, want string
	}{
		{
			name: "MATERIALIZED on ClickHouse", dialect: platform.ClickHouse, kind: "MATERIALIZED",
			want: `(?s).*column "c" declares generated kind MATERIALIZED without a generated expression, and clickhouse takes that kind only with one.*`,
		},
		{
			name: "EPHEMERAL on PostgreSQL", dialect: platform.Postgres, kind: "EPHEMERAL",
			want: `(?s).*column "c" declares generated kind EPHEMERAL without a generated expression, and postgres takes that kind only with one.*`,
		},
		{
			name: "STORED on MySQL", dialect: platform.MySQL, kind: "stored",
			want: `(?s).*column "c" declares generated kind STORED without a generated expression, and mysql takes that kind only with one.*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(
				bareGeneratedKind(test.kind), test.dialect, capability.ForDialect(test.dialect),
			)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(statements, qt.IsNil)
		})
	}
}
