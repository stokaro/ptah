package mysql

// White-box testing required: what the two engines answer for a column with no
// default is a difference in the CATALOG, and the only place it is visible is
// where readColumnsByTable turns a row into a column. Reaching it through
// ReadSchema would mean scripting every other query to observe this one, and
// the rows scripted here are the catalog's own -- so the result is still
// measured against what MariaDB reports rather than against an internal shape.

import (
	"database/sql/driver"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/dbschema/dbtest"
)

// columnsColumns is the projection readColumnsByTable selects, one row per
// column.
var columnsColumns = []string{
	"TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "COLUMN_TYPE", "IS_NULLABLE",
	"COLUMN_DEFAULT", "CHARACTER_MAXIMUM_LENGTH", "NUMERIC_PRECISION",
	"NUMERIC_SCALE", "ORDINAL_POSITION", "CHARACTER_SET_NAME",
	"COLLATION_NAME", "EXTRA", "GENERATION_EXPRESSION", "COLUMN_COMMENT",
}

// TestReadColumns_SeparatesNoDefaultFromADefaultOfNull pins which answers mean
// a column has a default and which mean it has none.
//
// MariaDB and MySQL disagree, and the disagreement is invisible until a
// description is replayed. Measured on MariaDB 12.3 and MySQL 26.7, the same
// table:
//
//	-- MariaDB                            -- MySQL
//	 COLUMN_NAME | COLUMN_DEFAULT          COLUMN_NAME | COLUMN_DEFAULT
//	 bio         | [NULL]  <- the TEXT     bio         | <SQL NULL>
//	 full_name   | [NULL]  <- the TEXT     full_name   | <SQL NULL>
//
// A default recorded from MariaDB's answer is rendered as `DEFAULT NULL`. An
// ordinary column tolerates that; a GENERATED column does not, and the server
// refused the whole statement with `Error 1064 ... near 'DEFAULT NULL'` -- so
// no MariaDB database holding a generated column could be replayed at all,
// while --dry-run against the source reported `Schema is synced`
// (stokaro/ptah#2128).
//
// The quoted row is the one that makes the fold safe rather than lossy, and it
// is a real MariaDB answer rather than an invented one: `DEFAULT 'NULL'` is
// stored WITH its quotes, so a column that genuinely defaults to the string
// keeps it.
func TestReadColumns_SeparatesNoDefaultFromADefaultOfNull(t *testing.T) {
	tests := []struct {
		name string
		// catalog is COLUMN_DEFAULT as the engine answers it. nil is SQL NULL.
		catalog driver.Value
		// want is the default the column ends up with, or "" for none.
		want string
	}{
		{
			name:    "MariaDB's answer for a column with no default",
			catalog: "NULL",
			want:    "",
		},
		{
			name:    "MySQL's answer for the same column",
			catalog: nil,
			want:    "",
		},
		{
			// The control. Without it the fold could swallow a real default and
			// every row above would still pass.
			name:    "a column that defaults to the string",
			catalog: "'NULL'",
			want:    "'NULL'",
		},
		{
			// The other control: an ordinary default is not touched.
			name:    "a column that defaults to something else",
			catalog: "'new'",
			want:    "'new'",
		},
		{
			// Case is the engine's to choose, and folding on an exact match
			// would leave this one recorded.
			name:    "the same answer in another case",
			catalog: "null",
			want:    "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			row := []driver.Value{
				"customers", "bio", "text", "text", "YES",
				test.catalog, nil, nil, nil, int64(1),
				"utf8mb4", "utf8mb4_general_ci", "", nil, "",
			}
			reader := NewMySQLReader(columnsDB(c, [][]driver.Value{row}).SQL, "app")

			columnsByTable, err := reader.readColumnsByTable(t.Context(), "app")

			c.Assert(err, qt.IsNil)
			c.Assert(columnsByTable["customers"], qt.HasLen, 1)

			c.Assert(recordedDefault(columnsByTable["customers"][0]), qt.Equals, test.want)
		})
	}
}

// columnsDB answers the column query with the rows a case supplies.
func columnsDB(c *qt.C, rows [][]driver.Value) *dbtest.DB {
	return dbtest.Open(c, func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		// Every column this fake returns must be one the query asked for.
		// Answering a projection that did not ask for COLUMN_COMMENT is how
		// this fake went one column short of the reader and broke the package
		// on master: the reader gained a column, the other fake for the same
		// query was updated, and this one was not (stokaro/ptah#2129).
		queried := strings.Contains(query, "FROM information_schema.COLUMNS") &&
			strings.Contains(query, "GENERATION_EXPRESSION") &&
			strings.Contains(query, "COLUMN_COMMENT")
		c.Assert(queried, qt.IsTrue, qt.Commentf("query: %s", query))
		return dbtest.QueryResult{Columns: columnsColumns, Rows: rows}, nil
	})
}

// recordedDefault spells a column's default, with "" for a column that has
// none, so a row states one value rather than picking which assertion to make.
//
// The empty string is unambiguous here: a column declared DEFAULT ” is
// reported by the catalog WITH its quotes, so it arrives as "”".
func recordedDefault(column types.DBColumn) string {
	if column.ColumnDefault == nil {
		return ""
	}
	return *column.ColumnDefault
}
