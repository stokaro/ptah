package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

// The offline SQL Server comparison has no collation to ask. Two ASCII names
// that differ after ASCII case folding are different under every collation
// SQL Server offers, so they are compared as two names; a pair that folds
// together, or one with a non-ASCII character, may be one name, and the
// comparison refuses rather than guess (stokaro/ptah#2315, stokaro/ptah#4122).

// TestCompareWithDatabaseInfo_SQLServerUnknownSemantics_HappyPath declares two
// tables, and a table with two columns, whose names no collation folds
// together.
func TestCompareWithDatabaseInfo_SQLServerUnknownSemantics_HappyPath(t *testing.T) {
	c := qt.New(t)

	diff, err := schemadiff.CompareWithDatabaseInfo(
		t.Context(), &schemamodel.Database{
			Tables: []schemamodel.Table{
				{StructName: "Order", Schema: "dbo", Name: "orders"},
				{StructName: "User", Schema: "dbo", Name: "users"},
			},
			Fields: []schemamodel.Field{
				{StructName: "User", Name: "email", Type: "NVARCHAR(320)"},
				{StructName: "User", Name: "status", Type: "INT"},
			},
		},
		&catalog.Database{},
		catalog.ServerInfo{Dialect: platform.SQLServer},
		nil, must.Must(builtin.New()),
	)

	c.Assert(err, qt.IsNil)
	c.Assert(diff.TablesAdded.Names(), qt.DeepEquals, []string{"dbo.orders", "dbo.users"})
}

// TestCompareWithDatabaseInfo_SQLServerUnknownTableSemantics_FailurePath covers
// two declared tables whose catalog identity the offline comparison cannot
// separate. The refusal is about the declaration as a whole rather than about
// anything a change set names. A name with a non-ASCII rune has no class
// offline, so the refusal says Ptah cannot tell rather than that the two may
// be one.
func TestCompareWithDatabaseInfo_SQLServerUnknownTableSemantics_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		first  string
		second string
		want   string
	}{
		{name: "ASCII case", first: "Users", second: "users", want: `target tables dbo\.Users and dbo\.users may have the same catalog identity`},
		{
			name: "an accent", first: "orders", second: "\u00f6rders",
			want: "without a connection Ptah cannot tell whether target tables dbo\\.orders and dbo\\.\u00f6rders are one name under the database collation",
		},
		{
			// Two names no collation reads as one are still a pair offline:
			// nothing says what a non-ASCII name is.
			name: "unrelated non-ASCII names", first: "Gr\u00f6\u00dfe", second: "Z\u00e4hler",
			want: "without a connection Ptah cannot tell whether target tables dbo\\.Gr\u00f6\u00dfe and dbo\\.Z\u00e4hler are one name under the database collation",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDatabaseInfo(
				t.Context(), &schemamodel.Database{Tables: []schemamodel.Table{
					{StructName: "First", Schema: "dbo", Name: test.first},
					{StructName: "Second", Schema: "dbo", Name: test.second},
				}},
				&catalog.Database{},
				catalog.ServerInfo{Dialect: platform.SQLServer},
				nil, must.Must(builtin.New()),
			)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, `.*`+test.want+`.*`)
			c.Assert(diff, qt.IsNil)
		})
	}
}

// TestCompareWithDatabaseInfo_SQLServerUnknownColumnSemantics_FailurePath is
// the same question one level down, for two columns of one table.
func TestCompareWithDatabaseInfo_SQLServerUnknownColumnSemantics_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		first  string
		second string
	}{
		{name: "ASCII case", first: "Email", second: "email"},
		{name: "an accent", first: "resume", second: "r\u00e9sum\u00e9"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDatabaseInfo(
				t.Context(), &schemamodel.Database{
					Tables: []schemamodel.Table{
						{StructName: "User", Schema: "dbo", Name: "users"},
					},
					Fields: []schemamodel.Field{
						{StructName: "User", Name: test.first, Type: "NVARCHAR(320)"},
						{StructName: "User", Name: test.second, Type: "INT"},
					},
				},
				&catalog.Database{},
				catalog.ServerInfo{Dialect: platform.SQLServer},
				nil, must.Must(builtin.New()),
			)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches,
				`.*target columns dbo\.users\.`+test.first+` and dbo\.users\.`+test.second+` may have the same catalog identity.*`)
			c.Assert(diff, qt.IsNil)
		})
	}
}
