package mysql

// White-box testing required: readWideGrants is unexported, and whether it
// returns nil or an empty list is what the grant comparison decides on. Through
// ReadSchema the test would script every other catalog query to observe it.

import (
	"database/sql/driver"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/internal/dbschema/dbtest"
)

// wideGrantMarkers name the queries readWideGrants runs, in order: the role
// predicate, the columns of mysql.user, and the read of mysql.user.
var wideGrantMarkers = []string{"SELECT COUNT(*)", "SELECT column_name", "FROM mysql.user"}

// mysqlUserColumns is information_schema's answer listing names as the columns
// of mysql.user.
func mysqlUserColumns(names ...string) dbtest.QueryResult {
	rows := make([][]driver.Value, 0, len(names))
	for _, name := range names {
		rows = append(rows, []driver.Value{name})
	}
	return dbtest.QueryResult{Columns: []string{"column_name"}, Rows: rows}
}

// mysqlBaseColumns are the privilege columns every supported server has.
var mysqlBaseColumns = []string{
	"Select_priv", "Insert_priv", "Update_priv", "Delete_priv", "Create_priv", "Drop_priv",
	"References_priv", "Index_priv", "Alter_priv", "Create_view_priv", "Show_view_priv", "Trigger_priv",
}

// openWideGrantCatalog is a catalog answering readWideGrants with columns and
// users, and recording each query it receives.
func openWideGrantCatalog(c *qt.C, columns, users dbtest.QueryResult) (*Reader, *[]string) {
	c.Helper()
	answers := []dbtest.QueryResult{
		{Columns: []string{"count"}, Rows: [][]driver.Value{{int64(0)}}},
		columns,
		users,
	}
	var queries []string
	db := dbtest.Open(c, func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		queries = append(queries, query)
		index := slices.IndexFunc(wideGrantMarkers, func(marker string) bool { return strings.Contains(query, marker) })
		return answers[index], nil
	})
	return NewMySQLReader(db.SQL, "app"), &queries
}

// TestReadWideGrants_ReadsEachPrivilegeHeldOnEveryDatabase reads one row per
// privilege a role holds in mysql.user, and an empty list where none does.
// Delete_history_priv is not asked for on a server without it.
func TestReadWideGrants_ReadsEachPrivilegeHeldOnEveryDatabase(t *testing.T) {
	held := func(name string, selects, alters bool) []driver.Value {
		values := []driver.Value{name}
		for _, column := range mysqlBaseColumns {
			values = append(values, map[string]bool{"Select_priv": selects, "Alter_priv": alters}[column])
		}
		return values
	}
	tests := []struct {
		name  string
		users [][]driver.Value
		want  []catalog.WideGrant
	}{
		{name: "none", want: make([]catalog.WideGrant, 0)},
		{
			name:  "two roles",
			users: [][]driver.Value{held("r_all", true, true), held("r_read", true, false)},
			want: []catalog.WideGrant{
				{Role: "r_all", Privilege: "SELECT", Grant: "SELECT ON *.*"},
				{Role: "r_all", Privilege: "ALTER", Grant: "ALTER ON *.*"},
				{Role: "r_read", Privilege: "SELECT", Grant: "SELECT ON *.*"},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			reader, queries := openWideGrantCatalog(c, mysqlUserColumns(mysqlBaseColumns...),
				dbtest.QueryResult{Columns: append([]string{"user"}, mysqlBaseColumns...), Rows: test.users})

			got, err := reader.readWideGrants(t.Context())

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
			c.Assert(*queries, qt.HasLen, 3)
			c.Assert((*queries)[2], qt.Not(qt.Contains), "Delete_history_priv")
		})
	}
}

// TestReadWideGrants_PartOfMySQLUserIsNotKnown returns nil, and does not read
// mysql.user, when information_schema lists only some of its privilege
// columns: the account may read those alone, and the rest could hold a grant.
func TestReadWideGrants_PartOfMySQLUserIsNotKnown(t *testing.T) {
	c := qt.New(t)
	reader, queries := openWideGrantCatalog(c, mysqlUserColumns("Select_priv", "Insert_priv"), dbtest.QueryResult{})

	got, err := reader.readWideGrants(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.IsNil)
	c.Assert(*queries, qt.HasLen, 2)
}
