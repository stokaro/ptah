package renderer_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
)

// constraintMethodSchema is tables p and c, with constraint on c. A foreign
// key refers to p.
func constraintMethodSchema(constraint schemamodel.Constraint) *schemamodel.Database {
	constraint.StructName, constraint.Table, constraint.Name = "C", "c", "c_x"
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "P", Name: "p"}, {StructName: "C", Name: "c"}},
		Fields: []schemamodel.Field{
			{StructName: "P", Name: "id", Type: "INT", Primary: true},
			{StructName: "C", Name: "id", Type: "INT", Primary: true},
			{StructName: "C", Name: "a", Type: "INT"},
		},
		Constraints: []schemamodel.Constraint{constraint},
	}
}

// TestRender_ConstraintMethod_FailurePath refuses `using` on a constraint
// that takes no access method. Built without it, the constraint silently
// becomes one the author did not ask for (stokaro/ptah#3958).
func TestRender_ConstraintMethod_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		constraint schemamodel.Constraint
		dialect    string
		caps       capability.Capabilities
		wantErr    string
	}{
		{
			name:       "UNIQUE on PostgreSQL",
			constraint: schemamodel.Constraint{Type: "UNIQUE", Columns: []string{"a"}, UsingMethod: "gist"},
			dialect:    platform.Postgres, caps: capability.Postgres18(),
			wantErr: `.*: postgres: the UNIQUE constraint "c_x" on "c" asks for USING gist, and a UNIQUE constraint takes no access method`,
		},
		{
			name:       "CHECK on PostgreSQL",
			constraint: schemamodel.Constraint{Type: "CHECK", CheckExpression: "a > 0", UsingMethod: "gist"},
			dialect:    platform.Postgres, caps: capability.Postgres18(),
			wantErr: `.*: postgres: the CHECK constraint "c_x" on "c" asks for USING gist, and a CHECK constraint takes no access method`,
		},
		{
			name: "FOREIGN KEY on MySQL",
			constraint: schemamodel.Constraint{
				Type: "FOREIGN KEY", Columns: []string{"a"}, ForeignTable: "p", ForeignColumns: []string{"id"},
				UsingMethod: "gist",
			},
			dialect: platform.MySQL, caps: capability.MySQL84(),
			wantErr: `.*: mysql: the FOREIGN KEY constraint "c_x" on "c" asks for USING gist, and a FOREIGN KEY constraint takes no access method`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := renderer.GetOrderedCreateStatementsWithCapabilities(
				constraintMethodSchema(test.constraint), test.dialect, test.caps,
			)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// TestRender_ConstraintMethod_HappyPath renders an EXCLUDE constraint's
// method, the one kind besides a primary key that takes one, and a UNIQUE
// constraint whose `using` is blank, which asks for no method.
func TestRender_ConstraintMethod_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		constraint schemamodel.Constraint
		want       string
	}{
		{
			name:       "EXCLUDE keeps its method",
			constraint: schemamodel.Constraint{Type: "EXCLUDE", UsingMethod: "gist", ExcludeElements: "a WITH ="},
			want:       `EXCLUDE USING gist (a WITH =)`,
		},
		{
			name:       "UNIQUE with a blank using",
			constraint: schemamodel.Constraint{Type: "UNIQUE", Columns: []string{"a"}, UsingMethod: "  "},
			want:       `CONSTRAINT "c_x" UNIQUE ("a")`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := renderer.GetOrderedCreateStatementsWithCapabilities(
				constraintMethodSchema(test.constraint), platform.Postgres, capability.Postgres18(),
			)

			c.Assert(err, qt.IsNil)
			c.Assert(strings.Join(statements, "\n"), qt.Contains, test.want)
		})
	}
}
