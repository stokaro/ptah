package oracle

// White-box testing required: the column read is package-local and its
// projection is not reachable through an exported API.

import (
	"database/sql/driver"
	"errors"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbschema/dbtest"
)

// A column's comment is read from the catalog -- stokaro/ptah#2132.
//
// The renderer half landed in #2137: a table's comment and its columns' now
// reach the server as COMMENT ON statements instead of SQL line comments the
// server never reads. The reader still asked for neither, so a column's comment
// was not lost in rendering -- nothing carried it at all, and a replay produced
// a table whose columns explained nothing.
func TestReadColumns_CarriesAColumnComment(t *testing.T) {
	c := qt.New(t)

	db := dbtest.Open(t, answeringColumnCatalog)
	reader := &Reader{db: db.SQL, schema: "APP"}

	columns, err := reader.readColumns(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(columns["CUSTOMERS"], qt.HasLen, 2)
	c.Assert(columns["CUSTOMERS"][0].Comment, qt.Equals, "")
	c.Assert(columns["CUSTOMERS"][1].Comment, qt.Equals, "login address")
}

// answeringColumnCatalog answers the column read, and refuses a projection that
// does not ask for what it returns.
//
// A fake that answers a fixed set of columns whatever the query selects cannot
// tell a reader that stopped asking for comments from one that still does: the
// mutation dropping the join survives against it.
func answeringColumnCatalog(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
	if err := requireColumnComments(query); err != nil {
		return dbtest.QueryResult{}, err
	}
	return dbtest.QueryResult{
		Columns: []string{
			"TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "CHAR_LENGTH",
			"DATA_PRECISION", "DATA_SCALE", "NULLABLE", "COLUMN_ID",
			"IDENTITY_COLUMN", "VIRTUAL_COLUMN", "DATA_DEFAULT", "COMMENTS",
		},
		Rows: [][]driver.Value{
			{"CUSTOMERS", "ID", "NUMBER", nil, int64(10), int64(0), "N", int64(1), "YES", "NO", nil, nil},
			{"CUSTOMERS", "EMAIL", "VARCHAR2", int64(255), nil, nil, "N", int64(2), "NO", "NO", nil, "login address"},
		},
	}, nil
}

func requireColumnComments(query string) error {
	joined := contains(query, "all_col_comments") && contains(query, "cc.comments")
	if joined {
		return nil
	}
	return errUnaskedComments
}

var errUnaskedComments = errors.New("column query does not join all_col_comments")

func contains(haystack, needle string) bool {
	return strings.Contains(haystack, needle)
}
