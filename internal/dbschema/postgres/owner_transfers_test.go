package postgres_test

import (
	"database/sql/driver"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/dbschema/postgres"
	"ptah.run/internal/objectowner"
)

// ownerTransferServer answers the questions an owner transfer check asks: the
// connected role app, a superuser where superuser says so, on a server whose
// server_version_num is version, and the roles rv_b and rv_a, unsorted, as
// the ones app lacks the privilege on. It records the privilege each role read asked
// about, and answers the relation named "public"."v" as owned by rv_a in
// schema public, where rv_a holds CREATE and no other role does, and no other
// relation.
type ownerTransferServer struct {
	version   int64
	superuser bool
	asked     []string
	looked    [][2]string
}

func (s *ownerTransferServer) open(c *qt.C, dialect string) *postgres.PostgreSQLWriter {
	c.Helper()
	db := dbtest.Open(c, func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
		switch {
		case strings.Contains(query, "server_version_num"):
			return dbtest.QueryResult{Columns: []string{"rolname", "rolsuper", "version"}, Rows: [][]driver.Value{{"app", s.superuser, s.version}}}, nil
		case strings.Contains(query, "pg_has_role"):
			s.asked = append(s.asked, args[0].Value.(string))
			return dbtest.QueryResult{Columns: []string{"rolname"}, Rows: [][]driver.Value{{"rv_b"}, {"rv_a"}}}, nil
		case strings.Contains(query, "to_regclass"):
			written, role := args[0].Value.(string), args[1].Value.(string)
			s.looked = append(s.looked, [2]string{written, role})
			columns := []string{"owner", "schema", "creates"}
			if written == `"public"."v"` {
				return dbtest.QueryResult{Columns: columns, Rows: [][]driver.Value{{"rv_a", "public", role == "rv_a"}}}, nil
			}
			return dbtest.QueryResult{Columns: columns}, nil
		}
		return dbtest.QueryResult{}, nil
	})
	return postgres.NewPostgreSQLWriterForRunnerWithCapabilities(db.SQL, "public", dialect, capability.ForDialect(dialect))
}

// TestOwnerTransfers_HappyPath pins the server's answer about the roles a
// connection may give an object to: SET from PostgreSQL 16.0, whose
// server_version_num is 160000, and membership before that; the roles the
// connection lacks it on, sorted; the connected role; and whether it is
// exempt from the schema check, which a PostgreSQL superuser is and a
// CockroachDB admin is not. CockroachDB
// reports a version that calls for SET and rejects SET as a privilege name,
// so it is asked about membership whatever the version says. YugabyteDB,
// whose requirement is not measured, and a writer built without a dialect ask
// nothing.
func TestOwnerTransfers_HappyPath(t *testing.T) {
	tests := []struct {
		name, dialect string
		version       int64
		superuser     bool
		wantAsked     []string
		want          objectowner.Transfers
	}{
		{name: "PostgreSQL 16.0", dialect: "postgres", version: 160000, wantAsked: []string{"SET"},
			want: objectowner.Transfers{Connected: "app", Privilege: "SET", Refused: []string{"rv_a", "rv_b"}}},
		{name: "PostgreSQL 18", dialect: "postgres", version: 180001, wantAsked: []string{"SET"},
			want: objectowner.Transfers{Connected: "app", Privilege: "SET", Refused: []string{"rv_a", "rv_b"}}},
		{name: "the last PostgreSQL 15 release", dialect: "postgres", version: 150099, wantAsked: []string{"MEMBER"},
			want: objectowner.Transfers{Connected: "app", Privilege: "MEMBER", Refused: []string{"rv_a", "rv_b"}}},
		{name: "CockroachDB", dialect: "cockroachdb", version: 180000, wantAsked: []string{"MEMBER"},
			want: objectowner.Transfers{Connected: "app", Privilege: "MEMBER", Refused: []string{"rv_a", "rv_b"}}},
		{name: "a CockroachDB admin", dialect: "cockroachdb", version: 180000, superuser: true, wantAsked: []string{"MEMBER"},
			want: objectowner.Transfers{Connected: "app", Privilege: "MEMBER", Refused: []string{"rv_a", "rv_b"}}},
		{name: "a superuser", dialect: "postgres", version: 180001, superuser: true, wantAsked: []string{"SET"},
			want: objectowner.Transfers{Connected: "app", SchemaExempt: true, Privilege: "SET", Refused: []string{"rv_a", "rv_b"}}},
		{name: "YugabyteDB", dialect: "yugabytedb", version: 110009},
		{name: "no dialect", dialect: "", version: 180001},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			server := &ownerTransferServer{version: test.version, superuser: test.superuser}

			got, err := server.open(c, test.dialect).OwnerTransfers(c.Context())

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
			c.Assert(server.asked, qt.DeepEquals, test.wantAsked)
		})
	}
}

// TestRelation_HappyPath pins the lookup of what the server holds of one
// relation: the name goes to the server as the statement writes it, with the
// role the statement names, and the answer is the owner, the schema, and
// whether that role holds CREATE there. A relation the server does not hold
// answers nothing rather than an error.
func TestRelation_HappyPath(t *testing.T) {
	tests := []struct {
		name, written, role string
		want                objectowner.Relation
	}{
		{name: "a role that may own an object there", written: `"public"."v"`, role: "rv_a",
			want: objectowner.Relation{Owner: "rv_a", Schema: "public", Creates: true}},
		{name: "a role that may not", written: `"public"."v"`, role: "rv_b",
			want: objectowner.Relation{Owner: "rv_a", Schema: "public"}},
		{name: "a relation the server does not hold", written: `"public"."new_view"`, role: "rv_a"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			server := &ownerTransferServer{}

			got, err := server.open(c, "postgres").Relation(c.Context(), test.written, test.role)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
			c.Assert(server.looked, qt.DeepEquals, [][2]string{{test.written, test.role}})
		})
	}
}

// TestHeldRoles_HappyPath pins the question about the roles a role holds the
// privilege on: the role and the privilege go to the server as given, and the
// answer is the roles it reports, sorted.
func TestHeldRoles_HappyPath(t *testing.T) {
	c := qt.New(t)
	server := &ownerTransferServer{}

	got, err := server.open(c, "postgres").HeldRoles(c.Context(), "rv_g", "SET")

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, []string{"rv_a", "rv_b"})
	c.Assert(server.asked, qt.DeepEquals, []string{"rv_g"})
}
