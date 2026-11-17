package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/objectowner"
	"ptah.run/internal/sqlschema"
)

// TestRead_ViewOwner_HappyPath pins that `ALTER [MATERIALIZED] VIEW name OWNER
// TO role` in a PostgreSQL schema file declares the owner of the view an
// earlier statement creates, under the name the server stores for the role.
// The last statement for a view is the owner it declares, as it is the owner
// the server leaves. A view no statement gives away declares none, and the
// OWNER TO of a table, which pg_dump writes after every table, declares
// nothing.
func TestRead_ViewOwner_HappyPath(t *testing.T) {
	tests := []struct {
		name                       string
		sql                        string
		wantView, wantMaterialized string
	}{
		{name: "a view", sql: "CREATE VIEW v AS SELECT 1 AS x;\nALTER VIEW v OWNER TO reporting;", wantView: "reporting"},
		{name: "a folded role name", sql: "CREATE VIEW v AS SELECT 1 AS x;\nALTER VIEW v OWNER TO Reporting;", wantView: "reporting"},
		{
			name:     "quoted names",
			sql:      "CREATE VIEW \"Order View\" AS SELECT 1 AS x;\nALTER VIEW \"Order View\" OWNER TO \"Report Owner\";",
			wantView: "Report Owner",
		},
		{name: "a name qualified with the default schema", sql: "CREATE VIEW public.v AS SELECT 1 AS x;\nALTER VIEW v OWNER TO reporting;", wantView: "reporting"},
		{
			name:             "a materialized view",
			sql:              "CREATE MATERIALIZED VIEW m AS SELECT 1 AS x;\nALTER MATERIALIZED VIEW public.m OWNER TO reporting;",
			wantMaterialized: "reporting",
		},
		{
			name:     "the last statement for a view",
			sql:      "CREATE VIEW v AS SELECT 1 AS x;\nALTER VIEW v OWNER TO first;\nALTER VIEW v OWNER TO second;",
			wantView: "second",
		},
		{name: "no statement", sql: "CREATE VIEW v AS SELECT 1 AS x;"},
		{name: "the owner of a table", sql: "CREATE TABLE t (id int);\nALTER TABLE t OWNER TO postgres;\nCREATE VIEW v AS SELECT id FROM t;"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := readWithOwners([]byte(test.sql), platform.Postgres)

			c.Assert(err, qt.IsNil)
			var view, materialized string
			for _, declared := range database.Views {
				view += declared.Owner
			}
			for _, declared := range database.MaterializedViews {
				materialized += declared.Owner
			}
			c.Assert(view, qt.Equals, test.wantView)
			c.Assert(materialized, qt.Equals, test.wantMaterialized)
		})
	}
}

// TestRead_ViewOwner_UnmodeledFailurePath pins the ownership statements a
// schema file cannot declare an owner with: one that names a view the file
// does not declare, and one that names a view as the other kind.
func TestRead_ViewOwner_UnmodeledFailurePath(t *testing.T) {
	tests := []struct {
		name, sql, wantErr string
	}{
		{
			name:    "a view the file does not declare",
			sql:     "CREATE VIEW v AS SELECT 1 AS x;\nALTER VIEW other OWNER TO reporting;",
			wantErr: `(?s).*ALTER VIEW other OWNER TO reporting names a view this schema does not declare.*`,
		},
		{
			name:    "a view named as a materialized view",
			sql:     "CREATE VIEW v AS SELECT 1 AS x;\nALTER MATERIALIZED VIEW v OWNER TO reporting;",
			wantErr: `(?s).*ALTER MATERIALIZED VIEW v OWNER TO reporting: the schema declares v as the other kind of view.*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, _, err := readWithOwners([]byte(test.sql), platform.Postgres)

			c.Assert(err, qt.ErrorIs, sqlschema.ErrUnmodeledStatement)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

// TestRead_ViewOwner_RoleFailurePath pins the roles no declaration can give a
// view to: the session's role, which the server resolves as the statement
// runs, and PUBLIC, which is every role.
func TestRead_ViewOwner_RoleFailurePath(t *testing.T) {
	tests := []struct {
		name, sql, wantErr string
	}{
		{
			name:    "the session's role",
			sql:     "CREATE VIEW v AS SELECT 1 AS x;\nALTER VIEW v OWNER TO CURRENT_USER;",
			wantErr: `(?s).*ALTER VIEW v OWNER TO CURRENT_USER: invalid owner CURRENT_USER: it names the role of the session.*`,
		},
		{
			name:    "every role",
			sql:     "CREATE MATERIALIZED VIEW m AS SELECT 1 AS x;\nALTER MATERIALIZED VIEW m OWNER TO public;",
			wantErr: `(?s).*invalid owner "public": PUBLIC is every role.*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, _, err := readWithOwners([]byte(test.sql), platform.Postgres)

			c.Assert(err, qt.ErrorIs, objectowner.ErrInvalidOwner)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

// TestRead_ViewOwner_CockroachDB_HappyPath pins that CockroachDB, which
// records a role name folded whether it is quoted or not, declares the owner
// under that name.
func TestRead_ViewOwner_CockroachDB_HappyPath(t *testing.T) {
	c := qt.New(t)

	database, _, err := readWithOwners([]byte("CREATE VIEW v AS SELECT 1 AS x;\nALTER VIEW v OWNER TO \"Reporting\";"), platform.CockroachDB)

	c.Assert(err, qt.IsNil)
	c.Assert(database.Views, qt.HasLen, 1)
	c.Assert(database.Views[0].Owner, qt.Equals, "reporting")
}
