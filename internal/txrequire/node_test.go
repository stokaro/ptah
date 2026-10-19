package txrequire_test

import (
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/internal/txrequire"
)

// autocommitNodes are the planned nodes NodeRequiresAutocommit decides: one
// of each kind it routes out, and its neighbor that stays in the file.
var autocommitNodes = map[string]ast.Node{
	"concurrent index":  &ast.IndexNode{Name: "idx", Concurrently: true},
	"plain index":       &ast.IndexNode{Name: "idx"},
	"concurrent drop":   &ast.DropIndexNode{Name: "idx", Concurrently: true},
	"plain drop":        &ast.DropIndexNode{Name: "idx"},
	"enum value":        &ast.AlterTypeNode{Name: "status", Operations: []ast.TypeOperation{&ast.AddEnumValueOperation{Value: "archived"}}},
	"other type change": &ast.AlterTypeNode{Name: "status"},
	"validation":        &ast.AlterTableNode{Name: "t", Operations: []ast.AlterOperation{&ast.ValidateConstraintOperation{ConstraintName: "c"}}},
	"other alter":       &ast.AlterTableNode{Name: "t"},
	"table":             &ast.CreateTableNode{Name: "t"},
}

// routedOut are the nodes each built-in dialect sends out of the transactional
// file. They are the answers of the switch the descriptor replaced, which sent
// all four of "concurrent index", "concurrent drop", "enum value" and
// "validation" out on the PostgreSQL family and nothing anywhere else, with one
// deliberate difference: Spanner's PostgreSQL interface has no enum type, its
// descriptor states no enum value rule, and so no ALTER TYPE leaves its file.
// The planner writes a Spanner enum column as TEXT with a CHECK, so no plan for
// Spanner holds one.
var routedOut = map[string][]string{
	platform.Postgres:    {"concurrent index", "concurrent drop", "enum value", "validation"},
	platform.CockroachDB: {"concurrent index", "concurrent drop", "enum value", "validation"},
	platform.YugabyteDB:  {"concurrent index", "concurrent drop", "enum value", "validation"},
	platform.Spanner:     {"concurrent index", "concurrent drop", "validation"},
	platform.MySQL:       nil,
	platform.MariaDB:     nil,
	platform.ClickHouse:  nil,
	platform.SQLite:      nil,
	platform.SQLServer:   nil,
	platform.Oracle:      nil,
	platform.YDB:         nil,
}

// TestNodeRequiresAutocommit_AgreesWithTheRetiredFamilySwitch asks every
// accepted spelling, padded and in upper case too, about every node kind.
func TestNodeRequiresAutocommit_AgreesWithTheRetiredFamilySwitch(t *testing.T) {
	c := qt.New(t)
	c.Assert(platform.Dialects().Dialects(), qt.HasLen, len(routedOut))
	for _, spelling := range platform.DialectSpellings() {
		for _, written := range []string{spelling, " " + spelling + " ", strings.ToUpper(spelling)} {
			for name, node := range autocommitNodes {
				t.Run(written+"/"+name, func(t *testing.T) {
					c := qt.New(t)
					want, found := routedOut[platform.NormalizeDialect(spelling)]
					c.Assert(found, qt.IsTrue)
					c.Assert(txrequire.NodeRequiresAutocommit(written, node), qt.Equals, slices.Contains(want, name))
				})
			}
		}
	}
}

// An empty or unknown dialect states nothing, so every node stays in the file.
func TestNodeRequiresAutocommit_NoDialect(t *testing.T) {
	for _, dialect := range []string{"", "nonesuch"} {
		for name, node := range autocommitNodes {
			t.Run(dialect+"/"+name, func(t *testing.T) {
				c := qt.New(t)
				c.Assert(txrequire.NodeRequiresAutocommit(dialect, node), qt.IsFalse)
			})
		}
	}
}
