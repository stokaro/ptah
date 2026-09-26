//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
)

// The reader reports the comment each kind of table constraint carries, on
// every engine of the family that stores one (stokaro/ptah#3678). The
// comments are written with raw COMMENT ON CONSTRAINT, not by Ptah, so the
// read is measured against what the server holds.
func TestReaderConstraintComments_LiveReadsEveryKind(t *testing.T) {
	engines := []struct {
		name   string
		engine dbtarget.Engine
	}{
		{name: "PostgreSQL", engine: dbtarget.PostgreSQL},
		{name: "CockroachDB", engine: dbtarget.CockroachDB},
		{name: "YugabyteDB", engine: dbtarget.YugabyteDB},
	}
	for _, test := range engines {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(c.Context(), time.Minute)
			defer cancel()
			conn, schemaName := prepareConstraintCommentFixture(c, ctx, test.engine)

			schema, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, []string{schemaName})

			c.Assert(err, qt.IsNil)
			comments := make(map[string]string)
			for _, constraint := range schema.Constraints {
				comments[constraint.Name] = constraint.Comment
			}
			c.Assert(comments["cc_orders_pk"], qt.Equals, "the key")
			c.Assert(comments["cc_orders_code_uq"], qt.Equals, "one order per code")
			c.Assert(comments["cc_orders_total_positive"], qt.Equals, "a total is positive")
			c.Assert(comments["cc_orders_customer_fk"], qt.Equals, "an order has a customer")
			c.Assert(comments["cc_customers_pk"], qt.Equals, "")
		})
	}
}

// prepareConstraintCommentFixture creates a schema of its own holding two
// tables whose constraints carry comments, and drops it when the test ends.
func prepareConstraintCommentFixture(c *qt.C, ctx context.Context, engine dbtarget.Engine) (*dbschema.DatabaseConnection, string) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(ctx, dbtarget.URL(c, engine))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		c.Check(conn.Close(), qt.IsNil)
	})
	schemaName := fmt.Sprintf("ptah_constraint_comment_%d", time.Now().UnixNano())
	schema := pgx.Identifier{schemaName}.Sanitize()
	c.Cleanup(func() {
		_, err := conn.ExecContext(context.Background(), "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
		c.Check(err, qt.IsNil)
	})
	for _, statement := range []string{
		"CREATE SCHEMA " + schema,
		"CREATE TABLE " + schema + ".customers (id bigint, CONSTRAINT cc_customers_pk PRIMARY KEY (id))",
		"CREATE TABLE " + schema + `.orders (
			id bigint, code text, total bigint, customer_id bigint,
			CONSTRAINT cc_orders_pk PRIMARY KEY (id),
			CONSTRAINT cc_orders_code_uq UNIQUE (code),
			CONSTRAINT cc_orders_total_positive CHECK (total > 0),
			CONSTRAINT cc_orders_customer_fk FOREIGN KEY (customer_id) REFERENCES ` + schema + `.customers (id)
		)`,
		"COMMENT ON CONSTRAINT cc_orders_pk ON " + schema + ".orders IS 'the key'",
		"COMMENT ON CONSTRAINT cc_orders_code_uq ON " + schema + ".orders IS 'one order per code'",
		"COMMENT ON CONSTRAINT cc_orders_total_positive ON " + schema + ".orders IS 'a total is positive'",
		"COMMENT ON CONSTRAINT cc_orders_customer_fk ON " + schema + ".orders IS 'an order has a customer'",
	} {
		_, err := conn.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}
	return conn, schemaName
}
