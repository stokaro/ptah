package mysql

// White-box testing required: readWideGrants, readDatabaseRows, databaseRows,
// joinWideGrants and wideAccessOf are unexported, and whether they return nil
// or an empty list, and whether the join is partial, is what the grant
// comparison and the security analysis decide on. Through ReadSchema the test
// would script every other catalog query to observe it, could not count the
// queries one of them sends among the rest, reads one database, where
// readDatabaseRows serves several, and would script the role and membership
// reads for each subject the pattern access is decided for.
// pattern_grants_test.go drives what a read of one database reports.

import (
	"cmp"
	"database/sql/driver"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/mysqlgrants"
)

// wideGrantMarkers name the queries readWideGrants runs, in order: the
// columns of mysql.user, and the read of mysql.user.
var wideGrantMarkers = []string{"SELECT column_name", "FROM mysql.user"}

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
	answers := []dbtest.QueryResult{columns, users}
	var queries []string
	db := dbtest.Open(c, func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		queries = append(queries, query)
		index := slices.IndexFunc(wideGrantMarkers, func(marker string) bool { return strings.Contains(query, marker) })
		return answers[index], nil
	})
	return NewMySQLReader(db.SQL, "app"), &queries
}

// TestReadWideGrants_ReadsEachPrivilegeHeldOnEveryDatabase reads one row per
// privilege a role or a login on host % holds in mysql.user, and an empty
// list where none does. Delete_history_priv is not asked for on a server
// without it.
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

			got, err := reader.readWideGrants(t.Context(), grantSource{})

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
			c.Assert(*queries, qt.HasLen, 2)
			c.Assert((*queries)[1], qt.Not(qt.Contains), "Delete_history_priv")
			c.Assert((*queries)[1], qt.Contains, "WHERE u.host = '%'")
		})
	}
}

// TestReadWideGrants_PartOfMySQLUserIsNotKnown returns nil, and does not read
// mysql.user, when information_schema lists only some of its privilege
// columns: the account may read those alone, and the rest could hold a grant.
func TestReadWideGrants_PartOfMySQLUserIsNotKnown(t *testing.T) {
	c := qt.New(t)
	reader, queries := openWideGrantCatalog(c, mysqlUserColumns("Select_priv", "Insert_priv"), dbtest.QueryResult{})

	got, err := reader.readWideGrants(t.Context(), grantSource{})

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.IsNil)
	c.Assert(*queries, qt.HasLen, 1)
}

// openDatabaseRowCatalog is a catalog answering readDatabaseRows with
// partialRevokes as the server's setting and rows as the roles'
// database-level rows, and recording each query it receives.
func openDatabaseRowCatalog(c *qt.C, partialRevokes string, rows [][]driver.Value) (*Reader, *[]string) {
	c.Helper()
	var queries []string
	db := dbtest.Open(c, func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		queries = append(queries, query)
		switch {
		case strings.Contains(query, "SHOW VARIABLES"):
			return dbtest.QueryResult{Columns: []string{"Variable_name", "Value"}, Rows: [][]driver.Value{{"partial_revokes", partialRevokes}}}, nil
		default:
			return dbtest.QueryResult{Columns: []string{"role_name", "TABLE_SCHEMA", "PRIVILEGE_TYPE"}, Rows: rows}, nil
		}
	})
	return NewMySQLReader(db.SQL, "app"), &queries
}

// TestReadDatabaseRows_ReadsEachDatabase reports the pattern grants of each
// database a read describes, under that database, from one read of the rows:
// a read of a server names several.
func TestReadDatabaseRows_ReadsEachDatabase(t *testing.T) {
	c := qt.New(t)
	reader, queries := openDatabaseRowCatalog(c, "OFF", [][]driver.Value{{"rvc", "cs%", "SELECT"}})

	rows, err := reader.readDatabaseRows(t.Context(), grantSource{})

	c.Assert(err, qt.IsNil)
	c.Assert(rows.patternGrants([]string{"cs1", "cs2"}), qt.DeepEquals, []catalog.WideGrant{
		{Role: "rvc", Privilege: "SELECT", Grant: "SELECT ON `cs%`.*", Schema: "cs1"},
		{Role: "rvc", Privilege: "SELECT", Grant: "SELECT ON `cs%`.*", Schema: "cs2"},
	})
	c.Assert(*queries, qt.HasLen, 2)
	c.Assert((*queries)[1], qt.Contains, "WHERE u.host = '%'")
}

// TestReadDatabaseRows_PartialRevokesReadNamesLiterally reports no pattern
// grant and no access where MySQL's partial_revokes is ON: the server then
// reads "%" and "_" in a database name as the characters themselves, and a
// pattern grant reaches no other database. Both lists are empty rather than
// nil, since the read established them, and the read asks for the partial
// revokes beside the rows.
func TestReadDatabaseRows_PartialRevokesReadNamesLiterally(t *testing.T) {
	c := qt.New(t)
	reader, queries := openDatabaseRowCatalog(c, "ON", [][]driver.Value{{"rvc", "cs%", "SELECT"}})

	rows, err := reader.readDatabaseRows(t.Context(), grantSource{})

	c.Assert(err, qt.IsNil)
	c.Assert(rows.patternGrants([]string{"cs1"}), qt.DeepEquals, make([]catalog.WideGrant, 0))
	c.Assert(rows.databaseAccess([]string{"cs1"}, nil, nil), qt.DeepEquals, make([]catalog.WideAccess, 0))
	c.Assert(*queries, qt.HasLen, 3)
	c.Assert((*queries)[1], qt.Contains, "JSON_TABLE(u.User_attributes")
	c.Assert((*queries)[1], qt.Contains, "WHERE u.host = '%'")
}

// TestDatabaseRows_NotRead is nil for both lists where the read did not
// establish the rows, which the security analysis and the revoke refusal each
// read as unknown.
func TestDatabaseRows_NotRead(t *testing.T) {
	c := qt.New(t)
	var rows *databaseRows

	c.Assert(rows.patternGrants([]string{"cs1"}), qt.IsNil)
	c.Assert(rows.databaseAccess([]string{"cs1"}, nil, nil), qt.IsNil)
}

// TestDatabaseRows_DatabaseAccess reports, for each role, the database-level
// rows the server applies to a session with that role active, the database's
// own among them, which is a measurement:
// each shape ran on MySQL 8.4.13 and 26.10.0 and on MariaDB 11.8.9 and 12.3.3
// with a session that activated the role. The rows are those of the role and
// of the roles it inherits, so a row of one role shadows a pattern another
// holds, and the revoke refusal still counts the shadowed pattern; the e2e
// test integration/schema_security_reach_e2e_test.go checks each shape against
// a session. A row on the database's own name names no source, and where the
// order is unknown every privilege of each tied row counts, under its row.
func TestDatabaseRows_DatabaseAccess(t *testing.T) {
	type held = map[string]map[string][]string
	mysql := mysqlgrants.Options{}
	mariaDB := mysqlgrants.Options{MariaDB: true}
	tests := []struct {
		name        string
		options     mysqlgrants.Options
		held        held
		roles       []string
		memberships []catalog.RoleMembership
		want        []catalog.WideAccess
		database    string
	}{
		{
			name: "a pattern alone", options: mysql, held: held{"qr": {"qa%": {"SELECT"}}},
			want: []catalog.WideAccess{{Role: "qr", Subject: "qr", Read: true, Source: "SELECT ON `qa%`.*", Schema: "qabc"}},
		},
		{
			name: "the database's own row shadows the pattern", options: mysql,
			held: held{"qs1": {"qa%": {"SELECT"}, "qabc": {"INSERT"}}},
			want: []catalog.WideAccess{{Role: "qs1", Subject: "qs1", Write: true, Schema: "qabc"}},
		},
		{
			name: "an own row holding EVENT alone shadows the pattern", options: mariaDB,
			held: held{"qe": {"qa%": {"SELECT"}, "qabc": {"EVENT"}}},
		},
		{
			name: "an own row of another role shadows the pattern for their member", options: mysql,
			held:        held{"qr1": {"qa%": {"SELECT"}}, "qr2": {"qabc": {"INSERT"}}},
			roles:       []string{"qm"},
			memberships: []catalog.RoleMembership{{Role: "qr1", Member: "qm"}, {Role: "qr2", Member: "qm"}},
			want: []catalog.WideAccess{
				{Role: "qr2", Subject: "qm", Write: true, Schema: "qabc"},
				{Role: "qr1", Subject: "qr1", Read: true, Source: "SELECT ON `qa%`.*", Schema: "qabc"},
				{Role: "qr2", Subject: "qr2", Write: true, Schema: "qabc"},
			},
		},
		{
			// The database's own name holds "_", so the server reads it as a
			// pattern, and on MySQL `q_%` sorts before it.
			name: "MySQL applies a pattern over an own name holding an underscore", options: mysql,
			held:     held{"qu": {"q_%": {"SELECT"}, "q_bc": {"INSERT"}}},
			want:     []catalog.WideAccess{{Role: "qu", Subject: "qu", Read: true, Source: "SELECT ON `q_%`.*", Schema: "q_bc"}},
			database: "q_bc",
		},
		{
			name: "MariaDB applies an own name holding an underscore over a pattern", options: mariaDB,
			held:     held{"qu": {"q_%": {"SELECT"}, "q_bc": {"INSERT"}}},
			want:     []catalog.WideAccess{{Role: "qu", Subject: "qu", Write: true, Schema: "q_bc"}},
			database: "q_bc",
		},
		{
			name: "a pattern through a chain of roles", options: mysql,
			held:        held{"qr1": {"qa%": {"SELECT"}}},
			roles:       []string{"qmid", "qm2"},
			memberships: []catalog.RoleMembership{{Role: "qmid", Member: "qm2"}, {Role: "qr1", Member: "qmid"}},
			want: []catalog.WideAccess{
				{Role: "qr1", Subject: "qm2", Read: true, Source: "SELECT ON `qa%`.*", Schema: "qabc"},
				{Role: "qr1", Subject: "qmid", Read: true, Source: "SELECT ON `qa%`.*", Schema: "qabc"},
				{Role: "qr1", Subject: "qr1", Read: true, Source: "SELECT ON `qa%`.*", Schema: "qabc"},
			},
		},
		{
			name: "MySQL applies the pattern first in byte order", options: mysql,
			held: held{"qp": {"q%": {"SELECT"}, "qa%": {"INSERT"}}},
			want: []catalog.WideAccess{{Role: "qp", Subject: "qp", Read: true, Source: "SELECT ON `q%`.*", Schema: "qabc"}},
		},
		{
			name: "MariaDB applies the pattern that matches fewer names", options: mariaDB,
			held: held{"qp": {"q%": {"SELECT"}, "qa%": {"INSERT"}}},
			want: []catalog.WideAccess{{Role: "qp", Subject: "qp", Write: true, Source: "INSERT ON `qa%`.*", Schema: "qabc"}},
		},
		{
			name: "rows that tie count what each holds", options: mariaDB,
			held: held{"uo": {"%_bc": {"SELECT"}, "_%bc": {"INSERT"}}},
			want: []catalog.WideAccess{
				{Role: "uo", Subject: "uo", Read: true, Source: "SELECT ON `%_bc`.*", Schema: "qabc"},
				{Role: "uo", Subject: "uo", Write: true, Source: "INSERT ON `_%bc`.*", Schema: "qabc"},
			},
		},
		{
			name: "a privilege two tied rows hold, named under each", options: mariaDB,
			held: held{"qt": {"%_bc": {"SELECT", "INSERT"}, "_%bc": {"SELECT"}}},
			want: []catalog.WideAccess{
				{Role: "qt", Subject: "qt", Read: true, Source: "SELECT ON `%_bc`.*", Schema: "qabc"},
				{Role: "qt", Subject: "qt", Write: true, Source: "INSERT ON `%_bc`.*", Schema: "qabc"},
				{Role: "qt", Subject: "qt", Read: true, Source: "SELECT ON `_%bc`.*", Schema: "qabc"},
			},
		},
		{
			name: "MariaDB looks PUBLIC up apart", options: mariaDB,
			held: held{"PUBLIC": {"qa%": {"SELECT"}}, "qr": {"qabc": {"INSERT"}}},
			want: []catalog.WideAccess{
				{Role: "PUBLIC", Subject: "PUBLIC", Read: true, Source: "SELECT ON `qa%`.*", Schema: "qabc"},
				{Role: "qr", Subject: "qr", Write: true, Schema: "qabc"},
			},
		},
		{
			name: "one row's read and write access, named apart", options: mysql,
			held: held{"qw": {"qa%": {"EVENT", "DELETE", "INSERT", "SELECT"}}},
			want: []catalog.WideAccess{
				{Role: "qw", Subject: "qw", Read: true, Source: "SELECT ON `qa%`.*", Schema: "qabc"},
				{Role: "qw", Subject: "qw", Write: true, Source: "INSERT, DELETE ON `qa%`.*", Schema: "qabc"},
			},
		},
		{name: "a row that gives no access to rows", options: mysql, held: held{"qx": {"qa%": {"EVENT", "CREATE"}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			roles := make([]catalog.Role, 0, len(test.roles))
			for _, name := range test.roles {
				roles = append(roles, catalog.Role{Name: name})
			}
			rows := &databaseRows{options: test.options, held: test.held}

			got := rows.databaseAccess([]string{cmp.Or(test.database, "qabc")}, roles, test.memberships)

			c.Assert(got, qt.DeepEquals, append(make([]catalog.WideAccess, 0), test.want...))
		})
	}
}

// TestWideAccessOf names one read and one write entry for each role holding
// privileges on every database, each by the privileges that give it, and
// keeps the pattern access after them. Either read missing leaves no access.
func TestWideAccessOf(t *testing.T) {
	everywhere := []catalog.WideGrant{
		{Role: "r1", Privilege: "SELECT"}, {Role: "r1", Privilege: "INSERT"}, {Role: "r1", Privilege: "ALTER"},
		{Role: "r1", Privilege: "DELETE"}, {Role: "r2", Privilege: "UPDATE"},
	}
	patterns := []catalog.WideAccess{{Role: "r3", Subject: "r3", Read: true, Source: "SELECT ON `cs%`.*", Schema: "cs1"}}
	tests := []struct {
		name           string
		everywhere     []catalog.WideGrant
		patterns, want []catalog.WideAccess
	}{
		{
			name: "both read", everywhere: everywhere, patterns: patterns,
			want: []catalog.WideAccess{
				{Role: "r1", Read: true, Source: "SELECT ON *.*"},
				{Role: "r1", Write: true, Source: "INSERT, DELETE ON *.*"},
				{Role: "r2", Write: true, Source: "UPDATE ON *.*"},
				patterns[0],
			},
		},
		{name: "no grants on every database", patterns: patterns},
		{name: "no pattern access", everywhere: everywhere},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(wideAccessOf(test.everywhere, test.patterns), qt.DeepEquals, test.want)
		})
	}
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
