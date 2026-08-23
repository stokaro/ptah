package oracle_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/renderer"
)

// TestComposite_RendersTheObjectSpellingOracleActuallyCreates pins the one word
// that separates a type from a broken shell.
//
// Measured on 23.26.2.0.0 through go-ora, both spellings return no error:
//
//	CREATE TYPE t AS (a NUMBER, b VARCHAR2(10))         -> err=nil
//	CREATE TYPE t AS OBJECT (a NUMBER, b VARCHAR2(10))  -> err=nil
//
// and only the second creates anything. The first leaves USER_TYPES reporting
// ATTRIBUTES 0 with INCOMPLETE YES and USER_OBJECTS reporting INVALID, so a
// renderer that emitted PostgreSQL's form would report success forever
// (stokaro/ptah#1920).
func TestComposite_RendersTheObjectSpellingOracleActuallyCreates(t *testing.T) {
	tests := []struct {
		name string
		node *ast.CreateTypeNode
		want string
	}{
		{
			name: "two fields",
			node: ast.NewCreateType("ora_point", ast.NewCompositeTypeDef(
				&ast.CompositeField{Name: "x", Type: "NUMBER(10,2)"},
				&ast.CompositeField{Name: "y", Type: "NUMBER(10,2)"},
			)),
			want: "CREATE OR REPLACE TYPE ora_point AS OBJECT (x NUMBER(10,2), y NUMBER(10,2));\n",
		},
		{
			name: "one field",
			node: ast.NewCreateType("ora_one", ast.NewCompositeTypeDef(
				&ast.CompositeField{Name: "a", Type: "NUMBER"},
			)),
			want: "CREATE OR REPLACE TYPE ora_one AS OBJECT (a NUMBER);\n",
		},
		{
			name: "a comment",
			node: ast.NewCreateType("ora_one", ast.NewCompositeTypeDef(
				&ast.CompositeField{Name: "a", Type: "NUMBER"},
			)).SetComment("a point"),
			want: "-- a point\nCREATE OR REPLACE TYPE ora_one AS OBJECT (a NUMBER);\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			out, err := renderer.RenderSQLWithCapabilities(platform.Oracle, capability.Oracle23(), test.node)

			c.Assert(err, qt.IsNil)
			c.Assert(out, qt.Equals, test.want)
			// The spelling PostgreSQL uses is what the server accepts and
			// leaves broken, so its absence is asserted rather than implied.
			c.Assert(out, qt.Not(qt.Contains), "AS (")
		})
	}
}

// TestComposite_BothLinesRenderTheSameStatement states what the two presets do
// NOT differ about.
//
// Object types are not a 23 feature. Measured on 21.3.0.0.0 and 23.26.2.0.0,
// the statement, ALL_TYPES and ALL_TYPE_ATTRS answered identically -- unlike
// CREATE DOMAIN, which is the one type statement where the lines really differ.
func TestComposite_BothLinesRenderTheSameStatement(t *testing.T) {
	c := qt.New(t)
	node := ast.NewCreateType("ora_one", ast.NewCompositeTypeDef(
		&ast.CompositeField{Name: "a", Type: "NUMBER"},
	))
	const want = "CREATE OR REPLACE TYPE ora_one AS OBJECT (a NUMBER);\n"

	on23, err := renderer.RenderSQLWithCapabilities(platform.Oracle, capability.Oracle23(), node)
	c.Assert(err, qt.IsNil)
	c.Assert(on23, qt.Equals, want)

	on21, err := renderer.RenderSQLWithCapabilities(platform.Oracle, capability.Oracle21(), node)
	c.Assert(err, qt.IsNil)
	c.Assert(on21, qt.Equals, want)
}

// TestComposite_RefusedWhereThePresetDeclinesThem keeps the key meaning what it
// says.
//
// The refusal is an error rather than a comment, which is this renderer's rule
// for a type: a comment makes `schema render` exit 0 on a model whose table
// would be left naming a type the server has no definition of.
func TestComposite_RefusedWhereThePresetDeclinesThem(t *testing.T) {
	c := qt.New(t)
	out, err := renderer.RenderSQLWithCapabilities(
		platform.Oracle, capability.Oracle23().With(capability.CompositeTypes, false),
		ast.NewCreateType("ora_one", ast.NewCompositeTypeDef(
			&ast.CompositeField{Name: "a", Type: "NUMBER"},
		)))

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "user types are not rendered for Oracle")
	c.Assert(out, qt.Equals, "")
}

// TestDropComposite_LeavesTheServersRefusalForALiveDependent pins the statement
// and, in its name, the option not taken.
//
// Measured on 23.26.2.0.0: DROP TYPE and CREATE OR REPLACE TYPE both answer
// ORA-02303 while a table column uses the type, and both change nothing. FORCE
// would leave that column naming a shape the server no longer has, so the
// refusal is kept.
func TestDropComposite_LeavesTheServersRefusalForALiveDependent(t *testing.T) {
	tests := []struct {
		name string
		caps capability.Capabilities
		node *ast.DropTypeNode
		want string
	}{
		{
			name: "on 23",
			caps: capability.Oracle23(),
			node: ast.NewDropType("ora_point"),
			want: "DROP TYPE ora_point;\n",
		},
		{
			// `DROP TYPE IF EXISTS` is ORA-00933 on this line, so the drop is
			// written bare -- and it is written bare on 23 too, which keeps one
			// statement rather than two spellings of one drop.
			name: "on 21",
			caps: capability.Oracle21(),
			node: ast.NewDropType("ora_point").SetIfExists(),
			want: "DROP TYPE ora_point;\n",
		},
		{
			name: "a comment",
			caps: capability.Oracle23(),
			node: ast.NewDropType("ora_point").SetComment("no longer declared"),
			want: "-- no longer declared\nDROP TYPE ora_point;\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			out, err := renderer.RenderSQLWithCapabilities(platform.Oracle, test.caps, test.node)

			c.Assert(err, qt.IsNil)
			c.Assert(out, qt.Equals, test.want)
			c.Assert(out, qt.Not(qt.Contains), "FORCE")
		})
	}
}
