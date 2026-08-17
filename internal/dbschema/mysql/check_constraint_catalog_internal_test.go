package mysql

// White-box testing required: what is under test is which of two spellings of
// the same catalog read is attempted FIRST, and both the choice and the queries
// are unexported. Reaching them through ReadSchema would mean scripting every
// other catalog query to observe this one.

import (
	"database/sql/driver"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	mysqldriver "github.com/go-sql-driver/mysql"

	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/internal/dbschema/dbtest"
)

// checkConstraintCatalog records which spellings a reader asked for, in order,
// and answers them the way a MySQL server does: the TABLE_NAME projection is
// error 1054, the name-only one returns a row.
type checkConstraintCatalog struct {
	asked []string
}

func (cat *checkConstraintCatalog) answer(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
	answers := map[bool]func() (dbtest.QueryResult, error){
		true:  cat.refuseTableName,
		false: cat.returnNameOnly,
	}
	cat.asked = append(cat.asked, spelling(query))
	return answers[strings.Contains(query, "TABLE_NAME")]()
}

func (cat *checkConstraintCatalog) refuseTableName() (dbtest.QueryResult, error) {
	return dbtest.QueryResult{}, &mysqldriver.MySQLError{
		Number:  1054,
		Message: "Unknown column 'TABLE_NAME' in 'field list'",
	}
}

func (cat *checkConstraintCatalog) returnNameOnly() (dbtest.QueryResult, error) {
	return dbtest.QueryResult{
		Columns: []string{"CONSTRAINT_NAME", "CHECK_CLAUSE"},
		Rows:    [][]driver.Value{{"chk_qty", "(`qty` > 0)"}},
	}, nil
}

// spelling names which of the two projections a query is, so the assertion
// reads as the sequence of questions rather than as SQL.
func spelling(query string) string {
	names := map[bool]string{true: "table-aware", false: "name-only"}
	return names[strings.Contains(query, "TABLE_NAME")]
}

// TestReadCheckConstraintClauses_CapabilityChoosesTheFirstSpelling pins both
// arms of the fifth ad-hoc gate stokaro/ptah#916 item 3 names, and the reason it
// keeps its error handling.
//
// MySQL's information_schema.CHECK_CONSTRAINTS has no TABLE_NAME column and
// MariaDB's does — measured on 8.4.11 and 11.8.8 — so a reader that always
// asked the richer spelling first paid a failed round trip on every MySQL
// schema read. The set removes that round trip; it does not remove the
// fallback, because the server's own answer about its own catalog outranks a
// preset.
func TestReadCheckConstraintClauses_CapabilityChoosesTheFirstSpelling(t *testing.T) {
	tests := []struct {
		name      string
		caps      capability.Capabilities
		wantAsked []string
	}{
		{
			name:      "a reader with no set sniffs, as it always did",
			caps:      nil,
			wantAsked: []string{"table-aware", "name-only"},
		},
		{
			name:      "a MySQL set asks the spelling MySQL has",
			caps:      capability.MySQL84(),
			wantAsked: []string{"name-only"},
		},
		{
			name:      "a MariaDB set asks the spelling MariaDB has",
			caps:      capability.MariaDB1011(),
			wantAsked: []string{"table-aware", "name-only"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			catalog := &checkConstraintCatalog{}
			db := dbtest.Open(c, catalog.answer)
			reader := NewMySQLReaderWithCapabilities(db.SQL, "app", test.caps)

			clauses, err := reader.readCheckConstraintClauses("app")

			c.Assert(err, qt.IsNil)
			c.Assert(catalog.asked, qt.DeepEquals, test.wantAsked)
			// The clause is read either way: choosing a spelling must not
			// change what the reader comes back with on a server that answers
			// the name-only projection.
			c.Assert(clauses.byName["chk_qty"], qt.Equals, "(`qty` > 0)")
		})
	}
}
