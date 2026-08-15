package lintcatalog

// White-box testing required: the drift gate's two failure directions are
// reached by handing the join a registry that disagrees with the catalog, and
// the seam that accepts one is unexported. The real registries are
// process-wide -- migration/lint's Register has no counterpart that removes a
// rule -- so driving a disagreement through them would leave it in place for
// every test that runs afterwards.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/lint"
)

// TestJoinRefusesARuleTheCatalogDoesNotDescribe is the direction that matters
// most: a rule added to the code and to no page.
func TestJoinRefusesARuleTheCatalogDoesNotDescribe(t *testing.T) {
	c := qt.New(t)

	registry := append(lint.Rules(), lint.Rule{
		Code:           "PG112P",
		Title:          "invented for this test",
		Severity:       lint.SeverityWarning,
		CheckStatement: func(*lint.Statement) (bool, string) { return false, "" },
	})

	entries, err := migrationEntriesFrom(registry)
	c.Assert(entries, qt.IsNil)
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "PG112P")
	c.Assert(err.Error(), qt.Contains, "no catalog entry")
}

// TestJoinRefusesACatalogRowForARuleThatIsGone is the other direction: a row
// that outlived the rule it described.
func TestJoinRefusesACatalogRowForARuleThatIsGone(t *testing.T) {
	c := qt.New(t)

	registry := lint.Rules()[1:]
	gone := lint.Rules()[0].Code

	entries, err := migrationEntriesFrom(registry)
	c.Assert(entries, qt.IsNil)
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, gone)
	c.Assert(err.Error(), qt.Contains, "no longer exist")
}

// TestSQLJoinRefusesBothDirections pins the same two failures for the SQL
// linter, whose identifiers come from a declaration rather than a registry.
func TestSQLJoinRefusesBothDirections(t *testing.T) {
	rows := []struct {
		name    string
		ids     []string
		message string
	}{
		{
			name:    "identifier the catalog does not describe",
			ids:     []string{"SQL001", "SQL002", "DDL001", "CAP001", "SQL003"},
			message: "no catalog entry",
		},
		{
			name:    "catalog row for an identifier that is gone",
			ids:     []string{"SQL001", "SQL002", "DDL001"},
			message: "no longer exist",
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			entries, err := sqlEntriesFrom(row.ids)
			c.Assert(entries, qt.IsNil)
			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Contains, row.message)
		})
	}
}
