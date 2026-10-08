//go:build integration

package ydb_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// uniqueSchema is the directory the UNIQUE constraint tests write into.
const uniqueSchema = "ptah_ydb_unique"

var uniqueSchemas = []string{uniqueSchema}

// uniqueDeclaration is a table with a named table-level UNIQUE, a column's own
// UNIQUE, and a UNIQUE over its key, which folds into the key.
func uniqueDeclaration() *schemamodel.Database {
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Account", Name: "accounts", Schema: uniqueSchema}},
		Fields: []schemamodel.Field{
			{StructName: "Account", Name: "id", Type: "BIGINT", Primary: true, Unique: true},
			{StructName: "Account", Name: "email", Type: "TEXT", Nullable: true, Unique: true},
			{StructName: "Account", Name: "tenant", Type: "BIGINT", Nullable: true},
			{StructName: "Account", Name: "login", Type: "TEXT", Nullable: true},
		},
		Constraints: []schemamodel.Constraint{
			{StructName: "Account", Name: "uq_accounts_tenant_login", Type: "UNIQUE", Columns: []string{"tenant", "login"}},
		},
	}
	schemamodel.Finalize(db)
	return db
}

// TestYDBUniqueConstraint_RoundTrip applies a declaration whose UNIQUE
// constraints YDB holds as global unique indexes, reads the indexes back under
// the names the renderer gave them, and plans nothing after, applied once or
// twice. The indexes hold rows the way the constraints would: a second row with
// the same value is refused, and two rows whose value is NULL are not.
func TestYDBUniqueConstraint_RoundTrip(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropTables(c, conn, uniqueSchemas)
			c.Cleanup(func() { dropTables(c, conn, uniqueSchemas) })

			declared := uniqueDeclaration()
			apply(c, conn, planAgainst(c, conn, declared, uniqueSchemas))
			c.Assert(planAgainst(c, conn, declared, uniqueSchemas), qt.HasLen, 0)
			apply(c, conn, planAgainst(c, conn, declared, uniqueSchemas))
			c.Assert(planAgainst(c, conn, declared, uniqueSchemas), qt.HasLen, 0)

			live := readScoped(c, conn, uniqueSchemas)
			c.Assert(indexNamesOf(live), qt.DeepEquals, []string{"accounts_email_key", "uq_accounts_tenant_login"})
			email := indexNamed(c, live, "accounts_email_key")
			c.Assert(email.IsUnique, qt.IsTrue)
			c.Assert(email.Columns, qt.DeepEquals, []string{"email"})
			tenantLogin := indexNamed(c, live, "uq_accounts_tenant_login")
			c.Assert(tenantLogin.IsUnique, qt.IsTrue)
			c.Assert(tenantLogin.Columns, qt.DeepEquals, []string{"tenant", "login"})

			writer := conn.Writer()
			c.Assert(writer.ExecuteSQL(c.Context(),
				"INSERT INTO `ptah_ydb_unique/accounts` (id, email) VALUES (1, 'a@example.com'u)"), qt.IsNil)
			c.Assert(writer.ExecuteSQL(c.Context(),
				"INSERT INTO `ptah_ydb_unique/accounts` (id, email) VALUES (2, 'a@example.com'u)"), qt.IsNotNil)
			c.Assert(writer.ExecuteSQL(c.Context(),
				"INSERT INTO `ptah_ydb_unique/accounts` (id, email) VALUES (3, NULL), (4, NULL)"), qt.IsNil)
			var count int64
			c.Assert(conn.QueryRowContext(c.Context(),
				"SELECT COUNT(*) FROM `ptah_ydb_unique/accounts` WHERE email IS NULL").Scan(&count), qt.IsNil)
			c.Assert(count, qt.Equals, int64(2))
		})
	}
}

// TestYDBUniqueConstraint_MatchesAUniqueIndex compares a declared UNIQUE
// constraint with a database that holds the unique index of the same name,
// built by hand, and plans nothing: the two are one object on YDB.
func TestYDBUniqueConstraint_MatchesAUniqueIndex(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropTables(c, conn, uniqueSchemas)
			c.Cleanup(func() { dropTables(c, conn, uniqueSchemas) })

			apply(c, conn, []string{"CREATE TABLE `ptah_ydb_unique/accounts` (" +
				"`id` Int64 NOT NULL, `email` Utf8, `tenant` Int64, `login` Utf8, PRIMARY KEY (`id`), " +
				"INDEX `uq_accounts_tenant_login` GLOBAL UNIQUE SYNC ON (`tenant`, `login`), " +
				"INDEX `accounts_email_key` GLOBAL UNIQUE SYNC ON (`email`))"})

			c.Assert(planAgainst(c, conn, uniqueDeclaration(), uniqueSchemas), qt.HasLen, 0)
		})
	}
}

// TestYDBUniqueConstraint_AddedToATableThatExists_FailurePath holds a UNIQUE
// constraint added to a table that exists to the key any unique index added
// there needs: the server refuses the index on such a table unless its flag is
// on, so the plan refuses first, naming the key.
func TestYDBUniqueConstraint_AddedToATableThatExists_FailurePath(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropTables(c, conn, uniqueSchemas)
			c.Cleanup(func() { dropTables(c, conn, uniqueSchemas) })

			before := uniqueDeclaration()
			before.Constraints = nil
			apply(c, conn, planAgainst(c, conn, before, uniqueSchemas))

			info := conn.Info()
			diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), uniqueDeclaration(), readScoped(c, conn, uniqueSchemas), info, nil, must.Must(builtin.New()))
			c.Assert(err, qt.IsNil)
			statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(
				context.Background(), must.Must(builtin.New()),
				diff, info.Dialect, planner.Options{Capabilities: info.Capabilities},
			)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `.*adding unique index "uq_accounts_tenant_login" to table "ptah_ydb_unique.accounts", `+
				`which exists already, which requires target capability unique_index_on_existing_table.*`)
			c.Assert(statements, qt.IsNil)
		})
	}
}
