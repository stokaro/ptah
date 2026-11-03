package postgres_test

import (
	"database/sql/driver"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5/pgconn"

	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/dbschema/postgres"
)

// serverRoles is a server whose role catalog answers roles, and which records
// every statement it is sent. A statement named in refused is answered once
// with that SQLSTATE, as the server answers a DROP ROLE for a role that still
// holds a privilege.
type serverRoles struct {
	roles    []string
	refused  map[string]string
	executed []string
}

func (s *serverRoles) open(c *qt.C) *postgres.PostgreSQLWriter {
	c.Helper()
	db := dbtest.OpenWithExec(c,
		func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
			rows := make([][]driver.Value, 0, len(s.roles))
			for _, role := range s.roles {
				rows = append(rows, []driver.Value{role})
			}
			return dbtest.QueryResult{Columns: []string{"rolname"}, Rows: rows}, nil
		},
		func(query string, _ []driver.NamedValue) (driver.Result, error) {
			s.executed = append(s.executed, query)
			if code, refused := s.refused[query]; refused {
				delete(s.refused, query)
				return nil, &pgconn.PgError{Code: code, Message: "refused"}
			}
			return driver.RowsAffected(0), nil
		},
	)
	return postgres.NewPostgreSQLWriter(db.SQL, "public")
}

// TestServerRoles_HappyPath reads every role on the server, which a dev
// database's claim keeps so the reset can tell the roles the run created.
func TestServerRoles_HappyPath(t *testing.T) {
	c := qt.New(t)
	server := &serverRoles{roles: []string{"app_reader", "pg_monitor", "ptah_user"}}

	roles, err := server.open(c).ServerRoles(c.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(roles, qt.DeepEquals, []string{"app_reader", "pg_monitor", "ptah_user"})
}

// TestDropRolesAddedSince_HappyPath drops the roles a replay created on a
// server the run owns, so the next replay of the same directory does not meet
// them: without the drop its CREATE ROLE is refused with `role "app_reader"
// already exists` (SQLSTATE 42710). A role the claim found is never dropped.
func TestDropRolesAddedSince_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		claimed []string
		current []string
		refused map[string]string
		want    []string
	}{
		{
			name:    "a role the run created",
			claimed: []string{"pg_monitor", "ptah_user"},
			current: []string{"app_reader", "pg_monitor", "ptah_user"},
			want:    []string{`DROP ROLE "app_reader"`},
		},
		{
			name:    "roles the server held at the claim",
			claimed: []string{"app_reader", "pg_monitor", "ptah_user"},
			current: []string{"app_reader", "pg_monitor", "ptah_user"},
		},
		{
			name:    "a role that still holds a privilege",
			claimed: []string{"ptah_user"},
			current: []string{"App Writer", "ptah_user"},
			refused: map[string]string{`DROP ROLE "App Writer"`: "2BP01"},
			want: []string{
				`DROP ROLE "App Writer"`,
				`REASSIGN OWNED BY "App Writer" TO CURRENT_USER`,
				`DROP OWNED BY "App Writer"`,
				`DROP ROLE "App Writer"`,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			server := &serverRoles{roles: test.current, refused: test.refused}

			err := server.open(c).DropRolesAddedSince(c.Context(), test.claimed)

			c.Assert(err, qt.IsNil)
			c.Assert(server.executed, qt.DeepEquals, test.want)
		})
	}
}

// TestDropRolesAddedSince_DryRunDropsNothing holds a dry run to sending no
// statement, as a dry run of the realm reset sends none.
func TestDropRolesAddedSince_DryRunDropsNothing(t *testing.T) {
	c := qt.New(t)
	server := &serverRoles{roles: []string{"app_reader", "ptah_user"}}
	writer := server.open(c)
	writer.SetDryRun(true)

	err := writer.DropRolesAddedSince(c.Context(), []string{"ptah_user"})

	c.Assert(err, qt.IsNil)
	c.Assert(server.executed, qt.HasLen, 0)
}

// TestDropRolesAddedSince_FailurePath names the role a refused drop left, and
// hands nothing the role owns to the session's user when the refusal is not
// about what depends on the role.
func TestDropRolesAddedSince_FailurePath(t *testing.T) {
	c := qt.New(t)
	server := &serverRoles{
		roles:   []string{"app_reader", "ptah_user"},
		refused: map[string]string{`DROP ROLE "app_reader"`: "42501"},
	}

	err := server.open(c).DropRolesAddedSince(c.Context(), []string{"ptah_user"})

	c.Assert(err, qt.ErrorMatches, `drop role "app_reader", which the run created: .*refused.*`)
	c.Assert(strings.Join(server.executed, "\n"), qt.Equals, `DROP ROLE "app_reader"`)
}
