package dbschema_test

import (
	"cmp"
	"context"
	"database/sql"
	"fmt"
	"slices"

	"github.com/go-extras/go-kit/must"

	"go.5x5.cz/ptah/dbschema"
)

// ExampleConnectToDatabase is the loop every embedder starts with: open a
// connection from a URL, defer CloseAndWarn so a close failure is logged
// rather than dropped, and read the server metadata back. The URL scheme
// picks the dialect; an in-memory SQLite database keeps the example
// self-contained and lives exactly as long as the connection.
func ExampleConnectToDatabase() {
	ctx := context.Background()

	conn, err := dbschema.ConnectToDatabase(ctx, "sqlite:///:memory:")
	if err != nil {
		fmt.Println("connect:", err)
		return
	}
	defer dbschema.CloseAndWarn(conn)

	info := conn.Info()
	fmt.Printf("dialect=%s schema=%s\n", info.Dialect, info.Schema)

	// Output:
	// dialect=sqlite schema=main
}

// ExampleReadSchemaWithSchemasContext reads the live schema into the catalog
// model, the CURRENT side of a schema comparison. This entry point is the
// safe scoped read: the schema allow-list goes on a private reader built for
// the call, so two concurrent reads at different scopes cannot see each
// other's list. A dialect whose reader has no schema scoping -- SQLite here
// -- reads its single schema regardless of the list.
func ExampleReadSchemaWithSchemasContext() {
	ctx := context.Background()

	conn := must.Must(dbschema.ConnectToDatabase(ctx, "sqlite:///:memory:"))
	defer dbschema.CloseAndWarn(conn)

	must.Must(conn.ExecContext(ctx,
		"CREATE TABLE products (id INTEGER PRIMARY KEY, code VARCHAR(50) NOT NULL)"))

	db, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, []string{"main"})
	if err != nil {
		fmt.Println("read:", err)
		return
	}

	for _, table := range db.Tables {
		fmt.Println("table:", table.QualifiedName())
		for _, column := range table.Columns {
			fmt.Printf("  %s %s nullable=%s\n", column.Name, column.RawType(), column.IsNullable)
		}
	}

	// Output:
	// table: products
	//   id INTEGER nullable=YES
	//   code VARCHAR(50) nullable=NO
}

// ExampleReadTableRows reads current rows projected onto named columns, the
// row-level read migration/datadiff builds on. Each row is a map keyed by the
// caller's exact column spellings, and driver []byte values arrive as string
// so values compare stably across drivers. No ORDER BY is issued -- row order
// is unspecified -- so the example sorts before printing.
func ExampleReadTableRows() {
	ctx := context.Background()

	conn := must.Must(dbschema.ConnectToDatabase(ctx, "sqlite:///:memory:"))
	defer dbschema.CloseAndWarn(conn)

	must.Must(conn.ExecContext(ctx,
		"CREATE TABLE regions (code TEXT PRIMARY KEY, name TEXT NOT NULL, population INTEGER NOT NULL)"))
	must.Must(conn.ExecContext(ctx,
		"INSERT INTO regions (code, name, population) VALUES ('CZ', 'Czechia', 10), ('US', 'United States', 331)"))

	rows, err := dbschema.ReadTableRows(ctx, conn, "", "regions",
		[]string{"code", "population"})
	if err != nil {
		fmt.Println("read rows:", err)
		return
	}

	slices.SortFunc(rows, func(a, b map[string]any) int {
		return cmp.Compare(a["code"].(string), b["code"].(string))
	})
	for _, row := range rows {
		fmt.Printf("%s %d\n", row["code"], row["population"])
	}

	// Output:
	// CZ 10
	// US 331
}

// ExampleDatabaseConnection_WithIsolatedQuerySession runs exploratory queries
// without letting them change anything: the callback gets a query-only handle
// on one physical session, and the transaction the queries run in is always
// rolled back -- even the DELETE below is undone, as the row count after the
// call shows. The physical session is discarded afterward so session state
// cannot leak back into the pool; in-memory SQLite's sole connection is
// instead rolled back and returned, which is what keeps the database alive
// and lets this example run on one.
func ExampleDatabaseConnection_WithIsolatedQuerySession() {
	ctx := context.Background()

	conn := must.Must(dbschema.ConnectToDatabase(ctx, "sqlite:///:memory:"))
	defer dbschema.CloseAndWarn(conn)

	must.Must(conn.ExecContext(ctx, "CREATE TABLE users (id INTEGER PRIMARY KEY)"))
	must.Must(conn.ExecContext(ctx, "INSERT INTO users (id) VALUES (1)"))

	err := conn.WithIsolatedQuerySession(ctx, new(sql.TxOptions),
		func(queryer dbschema.IsolatedQueryer) error {
			rows, err := queryer.QueryContext(ctx, "DELETE FROM users RETURNING id")
			if err != nil {
				return err
			}
			defer rows.Close()
			for rows.Next() {
				var id int
				if err := rows.Scan(&id); err != nil {
					return err
				}
				fmt.Println("deleted inside the session:", id)
			}
			return rows.Err()
		})
	if err != nil {
		fmt.Println("session:", err)
		return
	}

	var count int
	must.Assert(conn.QueryRowContext(ctx, "SELECT count(*) FROM users").Scan(&count))
	fmt.Println("rows after the session:", count)

	// Output:
	// deleted inside the session: 1
	// rows after the session: 1
}

// ExampleDatabaseConnection_SchemaWriter drives the transactional write path:
// begin a transaction-scoped executor, run DDL through it, commit, and read
// the schema back to see the table. Schema changes go through the writer
// rather than through [DatabaseConnection.Exec] because the writer is where
// dialect handling, dry-run, and the safety gates live.
func ExampleDatabaseConnection_SchemaWriter() {
	ctx := context.Background()

	conn := must.Must(dbschema.ConnectToDatabase(ctx, "sqlite:///:memory:"))
	defer dbschema.CloseAndWarn(conn)

	writer := conn.SchemaWriter()
	tx := must.Must(writer.BeginTransaction(ctx))
	if err := tx.ExecuteSQL(ctx,
		"CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL)"); err != nil {
		_ = tx.Rollback()
		fmt.Println("create:", err)
		return
	}
	must.Assert(tx.Commit())

	db := must.Must(conn.Reader().ReadSchemaContext(ctx))
	for _, table := range db.Tables {
		fmt.Println("table:", table.QualifiedName(), "columns:", len(table.Columns))
	}

	// Output:
	// table: users columns: 2
}
