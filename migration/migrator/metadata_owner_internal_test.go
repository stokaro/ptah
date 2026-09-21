package migrator

// White-box testing required: metadataTableOwnerQuery is deliberately
// internal, and which dialects answer the ownership question at all cannot be
// read through the public API without a live server of each engine.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
)

// Which dialects answer is the scope of the refusal, so it is pinned rather
// than left to whichever engine a live test happens to run against.
//
// The engines that answer no are not an oversight. The MySQL family has no
// table owner, and a trigger there runs as its definer rather than as the
// connected account, so the same squat gains the squatter a hook on every
// migration and not the migration role's privileges -- measured on MySQL
// 8.4.11 and MariaDB 12.3.3.
//
// SQL Server is out for a different reason: `sys.objects.principal_id` is NULL
// for an ordinary object, which the catalog reports as the schema's owner
// rather than the creator. Measured on SQL Server 2022, a `db_ddladmin`
// account created a table in `dbo` and the catalog named `dbo` as its owner,
// so the answer says nothing about who created it and a refusal built on it
// would reject the table Ptah itself had just created.
//
// SQLite has no roles, and ClickHouse and Spanner no per-table owner to
// compare against (stokaro/ptah#3474).
func Test_metadataTableOwnerQuery_Coverage(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		wantOK  bool
	}{
		{name: "postgres answers", dialect: platform.Postgres, wantOK: true},
		{name: "cockroachdb answers", dialect: platform.CockroachDB, wantOK: true},
		{name: "yugabytedb answers", dialect: platform.YugabyteDB, wantOK: true},
		{name: "sqlserver reports the schema owner, not the creator", dialect: platform.SQLServer, wantOK: false},
		{name: "oracle answers", dialect: platform.Oracle, wantOK: true},
		{name: "mysql has no table owner", dialect: platform.MySQL, wantOK: false},
		{name: "mariadb has no table owner", dialect: platform.MariaDB, wantOK: false},
		{name: "sqlite has no roles", dialect: platform.SQLite, wantOK: false},
		{name: "clickhouse has no table owner", dialect: platform.ClickHouse, wantOK: false},
		{name: "spanner has no table owner", dialect: platform.Spanner, wantOK: false},
		{name: "an unknown dialect answers nothing", dialect: "nonesuch", wantOK: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			query, args, ok := metadataTableOwnerQuery(test.dialect, "public", "public", "schema_migrations")
			c.Assert(ok, qt.Equals, test.wantOK)
			c.Assert(query != "", qt.Equals, test.wantOK)
			c.Assert(len(args) == 2, qt.Equals, test.wantOK)
		})
	}
}

// Every query that answers binds the schema and the table rather than pasting
// them, so a name carrying a quote cannot reshape the statement.
func Test_metadataTableOwnerQuery_BindsItsNames(t *testing.T) {
	dialects := []string{
		platform.Postgres,
		platform.CockroachDB,
		platform.YugabyteDB,
		platform.Oracle,
	}

	for _, dialect := range dialects {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			query, args, ok := metadataTableOwnerQuery(dialect, "app", "app", "schema_migrations")
			c.Assert(ok, qt.IsTrue)
			c.Assert(args, qt.DeepEquals, []any{"app", "schema_migrations"})
			c.Assert(query, qt.Not(qt.Contains), "app")
			c.Assert(query, qt.Not(qt.Contains), "schema_migrations")
		})
	}
}
