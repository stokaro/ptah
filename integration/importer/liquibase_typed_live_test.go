//go:build integration

package importer_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/importer"
	"ptah.run/migration/migrator"
)

// liquibaseTypedChangelog is a Liquibase history written only in typed changes:
// two tables, an index, a reference between them, and a later column added and
// one renamed. None of it is SQL until a dialect is chosen.
const liquibaseTypedChangelog = `<databaseChangeLog>
  <changeSet id="accounts" author="simon">
    <createTable tableName="lq_typed_accounts">
      <column name="id" type="bigint" autoIncrement="true"><constraints primaryKey="true"/></column>
      <column name="name" type="varchar(40)"><constraints nullable="false"/></column>
    </createTable>
  </changeSet>
  <changeSet id="subscriptions" author="simon">
    <createTable tableName="lq_typed_subscriptions">
      <column name="id" type="bigint" autoIncrement="true"><constraints primaryKey="true"/></column>
      <column name="account_id" type="bigint"><constraints nullable="false"/></column>
      <column name="plan" type="varchar(40)" defaultValue="starter"/>
    </createTable>
    <createIndex indexName="lq_typed_subscriptions_account_idx" tableName="lq_typed_subscriptions">
      <column name="account_id"/>
    </createIndex>
    <addForeignKeyConstraint constraintName="lq_typed_subscriptions_account_fk"
      baseTableName="lq_typed_subscriptions" baseColumnNames="account_id"
      referencedTableName="lq_typed_accounts" referencedColumnNames="id" onDelete="CASCADE"/>
  </changeSet>
  <changeSet id="cancellation" author="simon">
    <addColumn tableName="lq_typed_subscriptions">
      <column name="cancel_at_period_end" type="boolean" defaultValueBoolean="false">
        <constraints nullable="false"/>
      </column>
    </addColumn>
    <renameColumn tableName="lq_typed_subscriptions" oldColumnName="plan" newColumnName="tier" columnDataType="varchar(40)"/>
  </changeSet>
</databaseChangeLog>
`

// TestLiquibaseTypedImport_AppliesReadsBackAndRollsBack imports a changelog of
// typed changes for each engine, applies it, and asks the engine what it built.
//
// The rendered SQL is pinned offline; what only a server can say is whether it
// accepts that SQL and whether the objects behave as the changelog declared.
// So the evidence is behavior read back: an insert that names no id gets one,
// an omitted tier takes its default under its new name, the new flag defaults
// to false, and deleting an account removes its subscription through the
// reference. Rolling back to version 0 runs every derived rollback, in reverse,
// and must leave neither table behind.
func TestLiquibaseTypedImport_AppliesReadsBackAndRollsBack(t *testing.T) {
	tests := []struct {
		name    string
		engine  dbtarget.Engine
		dialect string
		// tablesQuery counts the two tables in the schema the connection uses.
		tablesQuery string
	}{
		{
			name: "postgres", engine: dbtarget.PostgreSQL, dialect: "postgres",
			tablesQuery: "SELECT count(*) FROM information_schema.tables WHERE table_schema = current_schema() " +
				"AND table_name IN ('lq_typed_accounts', 'lq_typed_subscriptions')",
		},
		{
			name: "mysql", engine: dbtarget.MySQL, dialect: "mysql",
			tablesQuery: "SELECT count(*) FROM information_schema.tables WHERE table_schema = DATABASE() " +
				"AND table_name IN ('lq_typed_accounts', 'lq_typed_subscriptions')",
		},
		{
			name: "mariadb", engine: dbtarget.MariaDB, dialect: "mariadb",
			tablesQuery: "SELECT count(*) FROM information_schema.tables WHERE table_schema = DATABASE() " +
				"AND table_name IN ('lq_typed_accounts', 'lq_typed_subscriptions')",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dbURL := dbtarget.URL(t, test.engine)
			c := qt.New(t)
			ctx := t.Context()

			conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
			dropLiquibaseTypedObjects(c, conn)
			c.Cleanup(func() { dropLiquibaseTypedObjects(c, conn) })

			source := c.TempDir()
			c.Assert(os.WriteFile(filepath.Join(source, "changelog.xml"), []byte(liquibaseTypedChangelog), 0o600), qt.IsNil)
			out := c.TempDir()
			parser, err := importer.ParserByName("liquibase")
			c.Assert(err, qt.IsNil)
			parser, err = importer.WithDialect(parser, test.dialect)
			c.Assert(err, qt.IsNil)
			result, err := importer.Import(os.DirFS(source), parser, out, importer.Options{})
			c.Assert(err, qt.IsNil)
			c.Assert(result.Files, qt.HasLen, 6)

			migrations, err := migrator.NewFSMigrator(conn, os.DirFS(out))
			c.Assert(err, qt.IsNil)
			migrations = migrations.WithMigrationsTable("", "lq_typed_migrations")
			c.Assert(migrations.MigrateUp(ctx), qt.IsNil)

			_, err = conn.ExecContext(ctx, "INSERT INTO lq_typed_accounts (name) VALUES ('acme')")
			c.Assert(err, qt.IsNil)
			_, err = conn.ExecContext(ctx,
				"INSERT INTO lq_typed_subscriptions (account_id) SELECT id FROM lq_typed_accounts WHERE name = 'acme'")
			c.Assert(err, qt.IsNil)
			var tier string
			var cancelAtPeriodEnd bool
			var generatedID int64
			err = conn.QueryRowContext(ctx,
				"SELECT id, tier, cancel_at_period_end FROM lq_typed_subscriptions").Scan(&generatedID, &tier, &cancelAtPeriodEnd)
			c.Assert(err, qt.IsNil)
			c.Assert(generatedID > 0, qt.IsTrue, qt.Commentf("the server generated no id: %d", generatedID))
			c.Assert(tier, qt.Equals, "starter")
			c.Assert(cancelAtPeriodEnd, qt.IsFalse)

			_, err = conn.ExecContext(ctx, "DELETE FROM lq_typed_accounts")
			c.Assert(err, qt.IsNil)
			var remaining int
			c.Assert(conn.QueryRowContext(ctx, "SELECT count(*) FROM lq_typed_subscriptions").Scan(&remaining), qt.IsNil)
			c.Assert(remaining, qt.Equals, 0)

			c.Assert(migrations.MigrateDownTo(ctx, 0), qt.IsNil)
			var tables int
			c.Assert(conn.QueryRowContext(ctx, test.tablesQuery).Scan(&tables), qt.IsNil)
			c.Assert(tables, qt.Equals, 0)
		})
	}
}

// dropLiquibaseTypedObjects removes what the test creates, in the order the
// reference allows, so a failed run does not leave a table the next run
// collides with. The migrations table goes with them: its rows describe
// migrations this test wrote into a directory that no longer exists.
//
// It runs on a background context because it also runs as a cleanup, and the
// test's own context is canceled before cleanups run.
func dropLiquibaseTypedObjects(c *qt.C, conn *dbschema.DatabaseConnection) {
	c.Helper()
	for _, table := range []string{
		"lq_typed_subscriptions", "lq_typed_accounts", "lq_typed_migrations", "lq_typed_migrations_log",
	} {
		_, err := conn.ExecContext(context.Background(), "DROP TABLE IF EXISTS "+table)
		c.Assert(err, qt.IsNil)
	}
}
