package oracle_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer"
)

// TestDomain_RendersTheShapeOracleAccepts pins the statement, and the clause
// order is a measurement rather than a guess.
//
// On 23.26.2.0.0,
//
//	CREATE DOMAIN pgshape_d AS VARCHAR2(50) NOT NULL DEFAULT 'x' CHECK (VALUE <> 'zzz')
//
// is accepted verbatim, so ast.DomainTypeDef's own order needs no rearranging
// for this target (stokaro/ptah#1920).
func TestDomain_RendersTheShapeOracleAccepts(t *testing.T) {
	tests := []struct {
		name string
		def  *ast.DomainTypeDef
		want string
	}{
		{
			name: "a bare domain",
			def:  ast.NewDomainTypeDef("VARCHAR2(50)"),
			want: "CREATE DOMAIN IF NOT EXISTS d AS VARCHAR2(50);",
		},
		{
			name: "not null",
			def:  ast.NewDomainTypeDef("VARCHAR2(50)").SetNotNull(),
			want: "CREATE DOMAIN IF NOT EXISTS d AS VARCHAR2(50) NOT NULL;",
		},
		{
			name: "a default",
			def:  ast.NewDomainTypeDef("VARCHAR2(50)").SetDefault("x"),
			want: "CREATE DOMAIN IF NOT EXISTS d AS VARCHAR2(50) DEFAULT 'x';",
		},
		{
			name: "a check",
			def:  ast.NewDomainTypeDef("NUMBER(5,2)").SetCheck("VALUE BETWEEN 0 AND 100"),
			want: "CREATE DOMAIN IF NOT EXISTS d AS NUMBER(5,2) CHECK (VALUE BETWEEN 0 AND 100);",
		},
		{
			name: "every clause, in the order the server takes them",
			def: ast.NewDomainTypeDef("VARCHAR2(50)").SetNotNull().
				SetDefault("x").SetCheck("VALUE <> 'zzz'"),
			want: "CREATE DOMAIN IF NOT EXISTS d AS VARCHAR2(50) NOT NULL DEFAULT 'x' " +
				"CHECK (VALUE <> 'zzz');",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			out, err := renderer.RenderSQLWithCapabilities(platform.Oracle, capability.Oracle23(),
				ast.NewCreateType("d", test.def))

			c.Assert(err, qt.IsNil)
			c.Assert(out, qt.Contains, test.want)
		})
	}
}

// TestDomain_TheGuardFollowsTheLine holds the one key where the two Oracle
// presets genuinely differ about an object rather than about a spelling.
//
// 23 has CREATE DOMAIN and its IF NOT EXISTS guard; 21 answers ORA-00901 to
// the statement, so the same renderer refuses there rather than writing SQL
// the server cannot parse.
func TestDomain_TheGuardFollowsTheLine(t *testing.T) {
	tests := []struct {
		name string
		caps capability.Capabilities
		// wantStatement is the whole rendered output, so the refusing row
		// asserts an EMPTY one rather than the absence of a substring:
		// qt.Contains with an empty string matches anything.
		wantStatement string
		wantErr       string
	}{
		{
			name:          "23 renders it, guarded",
			caps:          capability.Oracle23(),
			wantStatement: "CREATE DOMAIN IF NOT EXISTS d AS VARCHAR2(50);\n",
		},
		{
			name:    "21 refuses it",
			caps:    capability.Oracle21(),
			wantErr: "user types are not rendered for Oracle",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			out, err := renderer.RenderSQLWithCapabilities(platform.Oracle, test.caps,
				ast.NewCreateType("d", ast.NewDomainTypeDef("VARCHAR2(50)")))

			c.Assert(errorTextOrEmpty(err), qt.Contains, test.wantErr)
			c.Assert(out, qt.Equals, test.wantStatement)
		})
	}
}

// TestDropDomain_KeepsTheServersRefusalForALiveDependent pins the statement
// and, in its name, the option not taken.
//
// DROP DOMAIN ... FORCE was measured on 23.26.2.0.0 and is worse than the
// refusal it would avoid: with a LIVE dependent it succeeds and silently
// untypes the column, so a NOT NULL the domain enforced is gone and nobody
// asked. Plain DROP DOMAIN keeps ORA-11502 for that case.
func TestDropDomain_KeepsTheServersRefusalForALiveDependent(t *testing.T) {
	tests := []struct {
		name string
		node *ast.DropTypeNode
		want string
	}{
		{
			name: "bare",
			node: ast.NewDropType("d").SetDomain(),
			want: "DROP DOMAIN d;\n",
		},
		{
			name: "guarded",
			node: ast.NewDropType("d").SetDomain().SetIfExists(),
			want: "DROP DOMAIN IF EXISTS d;\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			out, err := renderer.RenderSQLWithCapabilities(
				platform.Oracle, capability.Oracle23(), test.node)

			c.Assert(err, qt.IsNil)
			c.Assert(out, qt.Equals, test.want)
			c.Assert(out, qt.Not(qt.Contains), "FORCE")
		})
	}
}

// errorTextOrEmpty renders an error without a branch in a test body.
func errorTextOrEmpty(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
