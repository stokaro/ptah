package mysql

// White-box testing required: readWideGrants, readPatternGrants and
// joinWideGrants are unexported, and whether they return nil or an empty list,
// and whether the join is partial, is what the grant comparison decides on.
// Through ReadSchema the test would script every other catalog query to observe
// it, could not count the queries one of them sends among the rest, and reads
// one database, where readPatternGrants reads several.
// pattern_grants_test.go drives what a read of one database reports.

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

// openPatternGrantCatalog is a catalog answering readPatternGrants with
// partialRevokes as the server's setting and rows as each database's pattern
// grants, and recording each query it receives.
func openPatternGrantCatalog(c *qt.C, partialRevokes string, rows [][]driver.Value) (*Reader, *[]string) {
	c.Helper()
	var queries []string
	db := dbtest.Open(c, func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		queries = append(queries, query)
		switch {
		case strings.Contains(query, "SHOW VARIABLES"):
			return dbtest.QueryResult{Columns: []string{"Variable_name", "Value"}, Rows: [][]driver.Value{{"partial_revokes", partialRevokes}}}, nil
		case strings.Contains(query, "SELECT COUNT(*)"):
			return dbtest.QueryResult{Columns: []string{"count"}, Rows: [][]driver.Value{{int64(1)}}}, nil
		default:
			return dbtest.QueryResult{Columns: []string{"role_name", "TABLE_SCHEMA", "PRIVILEGE_TYPE"}, Rows: rows}, nil
		}
	})
	return NewMySQLReader(db.SQL, "app"), &queries
}

// TestReadPatternGrants_ReadsEachDatabase reads the pattern grants of each
// database a read describes, under that database: a read of a server names
// several.
func TestReadPatternGrants_ReadsEachDatabase(t *testing.T) {
	c := qt.New(t)
	reader, _ := openPatternGrantCatalog(c, "OFF", [][]driver.Value{{"rvc", "cs%", "SELECT"}})

	got, err := reader.readPatternGrants(t.Context(), []string{"cs1", "cs2"})

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, []catalog.WideGrant{
		{Role: "rvc", Privilege: "SELECT", Grant: "SELECT ON `cs%`.*", Schema: "cs1"},
		{Role: "rvc", Privilege: "SELECT", Grant: "SELECT ON `cs%`.*", Schema: "cs2"},
	})
}

// TestReadPatternGrants_PartialRevokesReadNamesLiterally returns an empty list,
// and asks for no pattern grant, where MySQL's partial_revokes is ON: the
// server then reads "%" and "_" in a database name as the characters
// themselves, and a pattern grant reaches no other database.
func TestReadPatternGrants_PartialRevokesReadNamesLiterally(t *testing.T) {
	c := qt.New(t)
	reader, queries := openPatternGrantCatalog(c, "ON", [][]driver.Value{{"rvc", "cs%", "SELECT"}})

	got, err := reader.readPatternGrants(t.Context(), []string{"cs1"})

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, make([]catalog.WideGrant, 0))
	c.Assert(*queries, qt.HasLen, 1)
}

// TestJoinWideGrants is nil where neither read established its grants, and
// partial where only one did: a revoke under a grant the missing read would
// have found is then planned, as it is with no read at all.
func TestJoinWideGrants(t *testing.T) {
	everywhere := []catalog.WideGrant{{Role: "r", Privilege: "SELECT", Grant: "SELECT ON *.*"}}
	patterns := []catalog.WideGrant{{Role: "r", Privilege: "SELECT", Grant: "SELECT ON `cs%`.*", Schema: "cs1"}}
	tests := []struct {
		name                 string
		everywhere, patterns []catalog.WideGrant
		want                 []catalog.WideGrant
		partial              bool
	}{
		{name: "both read", everywhere: everywhere, patterns: patterns, want: append(slices.Clone(everywhere), patterns...)},
		{name: "both read, none held", everywhere: make([]catalog.WideGrant, 0), patterns: make([]catalog.WideGrant, 0), want: make([]catalog.WideGrant, 0)},
		{name: "no grants on every database", patterns: patterns, want: patterns, partial: true},
		{name: "no pattern grants", everywhere: make([]catalog.WideGrant, 0), want: make([]catalog.WideGrant, 0), partial: true},
		{name: "neither"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, partial := joinWideGrants(test.everywhere, test.patterns)

			c.Assert(got, qt.DeepEquals, test.want)
			c.Assert(partial, qt.Equals, test.partial)
		})
	}
}
