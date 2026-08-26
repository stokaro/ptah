package mysql

// White-box testing required: readRoleMemberships and the catalog discovery it
// depends on are unexported, and the fact under test is which table each engine
// records the role graph in. Reaching them through ReadSchema would mean
// scripting every other catalog query to observe this one.

import (
	"database/sql/driver"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/dbschema/dbtest"
)

// TestReadRoleMemberships_ReadsTheTableThisEngineHas pins the discovery and the
// projection together.
//
// The two engines record the same GRANT in different tables, and a query
// written for one does not degrade on the other -- MariaDB has no
// mysql.role_edges at all. The admin flag is part of the projection rather than
// a detail: MariaDB inserts an admin edge from a role's creator by itself, so
// an analysis that could not tell an admin edge from a membership would report
// every MariaDB role as held (stokaro/ptah#1950).
func TestReadRoleMemberships_ReadsTheTableThisEngineHas(t *testing.T) {
	tests := []struct {
		name string
		// hasRoleEdges is what information_schema.tables answers for
		// mysql.role_edges; the second lookup answers for roles_mapping.
		hasRoleEdges bool
		hasRolesMap  bool
		rows         [][]driver.Value
		want         []types.DBRoleMembership
	}{
		{
			name:         "MySQL, through role_edges",
			hasRoleEdges: true,
			rows: [][]driver.Value{
				{"reader", "alice", true},
				{"reader", "root", false},
			},
			want: []types.DBRoleMembership{
				{Role: "reader", Member: "alice", AdminOption: true},
				{Role: "reader", Member: "root", AdminOption: false},
			},
		},
		{
			name:        "MariaDB, through roles_mapping",
			hasRolesMap: true,
			rows: [][]driver.Value{
				{"reader", "root", true},
				{"reader", "alice", false},
			},
			want: []types.DBRoleMembership{
				{Role: "reader", Member: "root", AdminOption: true},
				{Role: "reader", Member: "alice", AdminOption: false},
			},
		},
		{
			// Roles arrived in MySQL 8.0 and MariaDB 10.0.5. A server with
			// neither table is describing a world where the question does not
			// exist, and answers an empty list rather than an error.
			name: "a server with neither table",
			want: make([]types.DBRoleMembership, 0),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := membershipDB(c, test.hasRoleEdges, test.hasRolesMap, test.rows)
			reader := NewMySQLReader(db.SQL, "app")

			memberships, err := reader.readRoleMemberships(t.Context())

			c.Assert(err, qt.IsNil)
			c.Assert(memberships, qt.DeepEquals, test.want)
		})
	}
}

// membershipDB answers the two existence lookups and then the membership query,
// and refuses anything else, so a projection that stopped selecting the admin
// flag fails here rather than reading something else.
func membershipDB(c *qt.C, hasRoleEdges, hasRolesMap bool, rows [][]driver.Value) *dbtest.DB {
	return dbtest.Open(c, func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		existence := strings.Contains(query, "FROM information_schema.tables")
		countRow := func(present bool) dbtest.QueryResult {
			count := int64(0)
			if present {
				count = 1
			}
			return dbtest.QueryResult{Columns: []string{"count"}, Rows: [][]driver.Value{{count}}}
		}
		switch {
		case existence && strings.Contains(query, "'role_edges'"):
			return countRow(hasRoleEdges), nil
		case existence && strings.Contains(query, "'roles_mapping'"):
			return countRow(hasRolesMap), nil
		case strings.Contains(query, "FROM mysql.role_edges"):
			// The admin flag has to be READ, not defaulted: a scripted result
			// answers whatever the stub was given, so the projection is what
			// this asserts. MariaDB writes an admin edge by itself, and an
			// analysis that lost the flag would report every role as held.
			c.Assert(query, qt.Contains, "WITH_ADMIN_OPTION")
			return dbtest.QueryResult{
				Columns: []string{"role_name", "member_name", "admin_option"},
				Rows:    rows,
			}, nil
		case strings.Contains(query, "FROM mysql.roles_mapping"):
			c.Assert(query, qt.Contains, "Admin_option")
			return dbtest.QueryResult{
				Columns: []string{"role_name", "member_name", "admin_option"},
				Rows:    rows,
			}, nil
		}
		c.Fatalf("unexpected query: %s", query)
		return dbtest.QueryResult{}, nil
	})
}
