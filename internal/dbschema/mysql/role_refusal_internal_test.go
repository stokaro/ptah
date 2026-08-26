package mysql

// White-box testing required: readRolesInto is unexported, and the record it
// makes is not reachable through ReadSchema without scripting every other
// catalog query to observe this one.
//
// An account may read its own schema and not mysql.user: those need different
// privileges, and failing the whole read over the second would leave an account
// unable to describe the first (stokaro/ptah#1762). The degradation is safe only
// because the description says the role catalog was not read -- and it now says
// WHY, because "the read was refused" is a privilege to grant while a reasonless
// record is a dead end (stokaro/ptah#1346).

import (
	"database/sql/driver"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	mysqldriver "github.com/go-sql-driver/mysql"

	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/dbschema/dbtest"
)

func TestReadRolesInto_RecordsWhyTheRoleCatalogWasNotRead(t *testing.T) {
	tests := []struct {
		name    string
		refused string
		want    []coverage.Object
	}{
		{
			name:    "the role list is refused",
			refused: "mysql.user",
			want:    []coverage.Object{coverage.Refused(coverage.Role)},
		},
		{
			// The grant read no longer names mysql.tables_priv: it reads the
			// privilege views instead, and SCHEMA_PRIVILEGES is the table this
			// query is refused on (stokaro/ptah#2204).
			name:    "the grant list is refused",
			refused: "SCHEMA_PRIVILEGES",
			want:    []coverage.Object{coverage.Refused(coverage.Role)},
		},
		{
			// The control. An account that may read everything claims full
			// authority over roles, so no record appears -- without this row a
			// reader that recorded the limit unconditionally would pass both
			// rows above.
			name:    "nothing is refused",
			refused: "no such table",
			want:    nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := dbtest.Open(t, refusingMySQLCatalog(test.refused))
			reader := NewMySQLReader(db.SQL, "app")

			schema := &types.DBSchema{}
			c.Assert(reader.readRolesInto(t.Context(), schema, "app"), qt.IsNil)

			c.Assert(schema.NotDescribed.Objects, qt.DeepEquals, test.want)
		})
	}
}

// refusingMySQLCatalog answers every catalog query with no rows, except the one
// naming the refused table, which fails the way a MySQL server fails a read the
// account has no privilege for.
//
// The queries wanting a value back are the catalog probes -- which
// discriminator column mysql.user has, and which membership table this server
// keeps -- and they are recognized by being counts. Matching on
// "information_schema" instead answered the grant projection with a one-column
// count, and it then failed on the scan rather than on the refusal this test is
// about (stokaro/ptah#2204).
func refusingMySQLCatalog(refused string) dbtest.QueryHandler {
	return func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		return map[bool]dbtest.QueryResult{
			true:  {Columns: []string{"count"}, Rows: [][]driver.Value{{int64(0)}}},
			false: {},
		}[strings.Contains(query, "SELECT COUNT(*)")], accessDeniedFor(query, refused)
	}
}

// accessDeniedFor is the error MySQL returns for a table the account may not
// read, on the one query that names the refused table.
func accessDeniedFor(query, refused string) error {
	return map[bool]error{
		true:  &mysqldriver.MySQLError{Number: errTableAccessDenied, Message: "command denied"},
		false: nil,
	}[strings.Contains(query, refused)]
}
