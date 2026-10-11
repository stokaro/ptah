package spannersource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/sqlext"
	"ptah.run/dialect/spanner/spannerschema"
	"ptah.run/dialect/spanner/spannersource"
	"ptah.run/internal/parser"
)

// parseSpanner parses sql as a Spanner document read with the Spanner owner.
func parseSpanner(sql string) (*ast.StatementList, error) {
	return parser.NewParser(sql, parser.WithDialect(platform.Spanner),
		parser.WithSQL(must.Must(sqlext.NewSet(spannersource.SQL())))).Parse()
}

// A row deletion policy is read whatever the letter case and the comments
// between its words, with its interval and its column unquoted.
func TestSQL_ReadsARowDeletionPolicy(t *testing.T) {
	c := qt.New(t)

	statements, err := parseSpanner("CREATE TABLE t (id bigint PRIMARY KEY, ts timestamptz)\n" +
		"  ttl /* keep a month */ interval '30 days'\n  on -- the column\n  \"ts\";")

	c.Assert(err, qt.IsNil)
	table := statements.Statements[0].(*ast.CreateTableNode)
	policy, found, err := schemaext.FacetAs[*spannerschema.DesiredRowDeletion](table.Facets, spannerschema.RowDeletionKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(policy.Policy, qt.Equals, spannerschema.Policy{Column: "ts", Interval: "30 days"})
}

// A malformed clause is refused at its position in the document, and nothing
// may follow the column.
func TestSQL_RefusesAMalformedClause(t *testing.T) {
	const table = "CREATE TABLE t (id bigint PRIMARY KEY, ts timestamptz) "
	tests := []struct {
		name    string
		clause  string
		wantErr string
	}{
		{name: "no interval keyword", clause: "TTL '30 days' ON ts;", wantErr: `expected INTERVAL after TTL at position 59`},
		{name: "no interval", clause: "TTL INTERVAL ON ts;", wantErr: `expected a quoted interval after TTL INTERVAL at position 68`},
		{name: "no ON", clause: "TTL INTERVAL '30 days' ts;", wantErr: `expected ON after the TTL interval at position 78`},
		{name: "no column", clause: "TTL INTERVAL '30 days' ON;", wantErr: `expected a column name after TTL ... ON at position 80`},
		{name: "an interval Spanner refuses", clause: "TTL INTERVAL '36 hours' ON ts;",
			wantErr: `TTL at position 82: .*interval "36 hours" is not a whole number of days, which Spanner refuses.*`},
		{name: "words after the column", clause: "TTL INTERVAL '30 days' ON ts CASCADE;",
			wantErr: `unexpected "CASCADE" after the row deletion policy at position 84`},
		{name: "a second policy", clause: "TTL INTERVAL '30 days' ON ts, TTL INTERVAL '1 days' ON ts;",
			wantErr: `table t: .*duplicate.*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parseSpanner(table + test.clause)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// A Spanner document claims every table's row deletion policy, so a table
// without one requests none; a document that names no target claims nothing.
func TestSQL_Coverage(t *testing.T) {
	c := qt.New(t)
	set := must.Must(sqlext.NewSet(spannersource.SQL()))

	claim, claimed, err := set.Coverage(platform.Spanner, nil)
	c.Assert(err, qt.IsNil)
	c.Assert(claimed, qt.IsTrue)
	c.Assert(claim.Lookup(spannerschema.RowDeletionKind, objectidentity.ID{}).State, qt.Equals, schemaext.Complete)

	_, claimed, err = set.Coverage("", nil)
	c.Assert(err, qt.IsNil)
	c.Assert(claimed, qt.IsFalse)
}
