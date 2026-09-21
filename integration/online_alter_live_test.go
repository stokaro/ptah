//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff/difftypes"
)

// What the ALGORITHM clause buys is the server's own answer rather than Ptah's
// model of it, so the property is measured on a server: the statement the
// planner writes is accepted for a change that runs in place and refused for
// one that copies the table (stokaro/ptah#3421).
//
// The refusal is the half worth having. Without the clause the copy proceeds,
// and the migration that was supposed to run online blocks writes for as long
// as the table takes.
func TestOnlineAlterMySQLFamilyServerDecidesLive(t *testing.T) {
	tests := []struct {
		name   string
		engine dbtarget.Engine
		caps   capability.Capabilities
	}{
		{name: "mysql", engine: dbtarget.MySQLAdmin, caps: capability.MySQL84()},
		{name: "mariadb", engine: dbtarget.MariaDBAdmin, caps: capability.MariaDB1011()},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx := context.Background()
			database := "ptah_online_alter_" + test.name
			admin, err := sql.Open("mysql", dbtarget.DriverDSN(c, test.engine))
			c.Assert(err, qt.IsNil)
			t.Cleanup(func() { c.Check(admin.Close(), qt.IsNil) })
			dropMySQLDatabase(c, ctx, admin, database)
			createMySQLDatabase(c, ctx, admin, database)
			t.Cleanup(func() { dropMySQLDatabase(c, context.Background(), admin, database) })

			scoped, err := sql.Open("mysql", mySQLDSNForDatabase(c, dbtarget.DriverDSN(c, test.engine), database))
			c.Assert(err, qt.IsNil)
			t.Cleanup(func() { c.Check(scoped.Close(), qt.IsNil) })
			_, err = scoped.ExecContext(ctx,
				"CREATE TABLE online_alter (id BIGINT PRIMARY KEY, c VARCHAR(10)) ENGINE=InnoDB")
			c.Assert(err, qt.IsNil)

			planned := onlineAlterAddColumnStatement(c, test.caps)
			c.Assert(planned, qt.Contains, "ALGORITHM=INPLACE, LOCK=NONE")

			_, err = scoped.ExecContext(ctx, planned)
			c.Assert(err, qt.IsNil, qt.Commentf("%s", planned))

			// Adding AUTO_INCREMENT copies the table on both engines, so the
			// same clause is refused rather than downgraded.
			_, err = scoped.ExecContext(ctx,
				"ALTER TABLE online_alter MODIFY id BIGINT NOT NULL AUTO_INCREMENT, ALGORITHM=INPLACE, LOCK=NONE")
			c.Assert(err, qt.ErrorMatches, `(?s).*ALGORITHM=INPLACE is not supported.*`)
		})
	}
}

// onlineAlterAddColumnStatement plans one column addition with the online
// request and returns the single statement it produced.
func onlineAlterAddColumnStatement(c *qt.C, caps capability.Capabilities) string {
	c.Helper()
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(
		&difftypes.SchemaDiff{
			TablesModified: []difftypes.TableDiff{{
				TableName: "online_alter",
				ColumnsAdded: difftypes.ColumnChanges{{
					StructName: "OnlineAlter", Name: "nickname", Type: "VARCHAR(64)", Nullable: true,
				}},
			}},
			DeclaredTables: []schemamodel.Table{{StructName: "OnlineAlter", Name: "online_alter"}},
		},
		"mysql",
		planner.Options{Capabilities: caps, OnlineAlter: true},
	)
	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.HasLen, 1)
	return statements[0]
}

// mySQLDSNForDatabase renames the database a driver DSN addresses, keeping the
// credentials and the host the registry handed over.
func mySQLDSNForDatabase(c *qt.C, dsn, database string) string {
	c.Helper()
	cut := strings.LastIndex(dsn, "/")
	c.Assert(cut > 0, qt.IsTrue)
	return dsn[:cut+1] + database
}

// A PostgreSQL constraint added NOT VALID leaves the rows already in the table
// unchecked, and the statement that validates it takes a weaker lock. Both
// halves are the server's, so both are asked here.
func TestOnlineAlterPostgresAddsAndValidatesSeparatelyLive(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn, err := sql.Open("pgx", dbtarget.URL(c, dbtarget.PostgreSQL))
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })

	table := "ptah_online_constraint"
	_, err = conn.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() {
		_, dropErr := conn.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
		c.Check(dropErr, qt.IsNil)
	})
	_, err = conn.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id BIGINT PRIMARY KEY, amount BIGINT)", table))
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (id, amount) VALUES (1, 5)", table))
	c.Assert(err, qt.IsNil)

	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(
		&difftypes.SchemaDiff{ConstraintsAdded: difftypes.ConstraintAdditions{{
			Name: "ck_amount", TableName: table, Type: "CHECK", CheckExpression: "amount > 0",
		}}},
		"postgres",
		planner.Options{Capabilities: capability.Postgres18(), OnlineAlter: true},
	)
	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.HasLen, 2)

	// The addition alone leaves the constraint unvalidated, which is what
	// makes it cheap; the second statement is what completes it.
	_, err = conn.ExecContext(ctx, statements[0])
	c.Assert(err, qt.IsNil, qt.Commentf("%s", statements[0]))
	c.Assert(postgresConstraintValidated(c, ctx, conn, "ck_amount"), qt.IsFalse)

	_, err = conn.ExecContext(ctx, statements[1])
	c.Assert(err, qt.IsNil, qt.Commentf("%s", statements[1]))
	c.Assert(postgresConstraintValidated(c, ctx, conn, "ck_amount"), qt.IsTrue)
}

func postgresConstraintValidated(c *qt.C, ctx context.Context, conn *sql.DB, name string) bool {
	c.Helper()
	var validated bool
	c.Assert(conn.QueryRowContext(
		ctx, "SELECT convalidated FROM pg_constraint WHERE conname = $1", name,
	).Scan(&validated), qt.IsNil)
	return validated
}
