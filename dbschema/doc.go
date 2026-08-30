// Package dbschema provides database connection management plus schema reading
// and writing against a live database. It is the boundary where Ptah's
// dialect-agnostic schema model meets a concrete server: [ConnectToDatabase]
// picks the dialect from the URL scheme, and the returned [DatabaseConnection]
// carries a schema reader, a schema writer, and direct SQL access bound to
// that dialect.
//
// # Core Components
//
// The package provides these main types:
//
//   - [DatabaseConnection]: the connection every operation hangs off.
//   - catalog.SchemaReader: reads the live schema into a *catalog.Database.
//   - catalog.SchemaWriter: executes schema changes, transactionally on
//     request.
//   - catalog.ServerInfo: dialect, version, schema, capability and identifier
//     metadata, returned by [DatabaseConnection.Info].
//
// The per-dialect reader and writer implementations live under
// internal/dbschema and are not importable from another module;
// [ConnectToDatabase] selects the pair matching the URL and hands them out
// behind the catalog interfaces.
//
// # Supported Databases
//
// The package connects to these database platforms:
//
//   - PostgreSQL, with CockroachDB, YugabyteDB, and Spanner reached over the
//     same wire protocol; the server's own version banner can override the
//     scheme: a postgres:// URL to a server announcing CockroachDB, YugabyteDB,
//     or Spanner resolves that product's dialect, while a banner naming only
//     PostgreSQL leaves the scheme's dialect in place.
//   - MySQL and MariaDB, one driver with the product told apart by version.
//   - SQLite: local file, URI, and in-memory databases with PRAGMA-backed
//     introspection -- plus a remote libsql (Turso) server through libsql://
//     and libsql+ws://, which serves the same SQLite schema surface over a
//     different transport.
//   - ClickHouse: MergeTree-family subset with system catalog introspection.
//   - SQL Server: T-SQL subset with schemas, IDENTITY, indexes, and core
//     constraints.
//   - Oracle: the connected user's schema, since in Oracle a schema is a
//     user; ?schema= on the URL selects another user's objects.
//
// A URL scheme outside that set is refused rather than guessed at.
//
// # Connection Management
//
// Database connections are established using standard database URLs:
//
//	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
//	defer cancel()
//
//	// PostgreSQL
//	conn, err := dbschema.ConnectToDatabase(ctx, "postgres://user:pass@localhost:5432/database")
//
//	// MySQL
//	conn, err := dbschema.ConnectToDatabase(ctx, "mysql://user:pass@tcp(localhost:3306)/database")
//
//	// MariaDB
//	conn, err := dbschema.ConnectToDatabase(ctx, "mariadb://user:pass@tcp(localhost:3307)/database")
//
//	// SQLite
//	conn, err := dbschema.ConnectToDatabase(ctx, "sqlite:///tmp/app.db")
//
//	// SQL Server
//	conn, err := dbschema.ConnectToDatabase(ctx, "sqlserver://sa:pass@localhost:1433?database=app&encrypt=disable")
//
// # Schema Reading
//
// The package provides comprehensive schema introspection:
//
//	schema, err := conn.Reader().ReadSchemaContext(ctx)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Access schema components
//	for _, table := range schema.Tables {
//		fmt.Printf("Table: %s\n", table.Name)
//		for _, column := range table.Columns {
//			fmt.Printf("  Column: %s (%s)\n", column.Name, column.RawType())
//		}
//	}
//
// Every schema read comes in two forms, the pairing this package offers for
// every other database call and the one database/sql itself uses: a reader's
// ReadSchemaContext beside its ReadSchema, and [ReadSchemaWithSchemasContext]
// beside [ReadSchemaWithSchemas], just as [DatabaseConnection.ExecContext] sits
// beside [DatabaseConnection.Exec]. The context-free form is the same read
// under context.Background(), for a caller with none to hand.
//
// Prefer the Context form wherever a context exists. A schema read is dozens of
// round trips against a server that may be slow or unreachable, and only the
// Context form can be told to stop: canceling the context, or letting its
// deadline pass, makes the read return promptly with an error rather than
// running the remaining queries.
//
// # Schema Writing
//
// The package supports transactional schema modifications:
//
//	writer := conn.SchemaWriter()
//
//	// Begin a transaction-scoped writer.
//	tx, err := writer.BeginTransaction(ctx)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Execute schema changes
//	if err := tx.ExecuteSQL(ctx, "CREATE TABLE users (id SERIAL PRIMARY KEY)"); err != nil {
//		tx.Rollback()
//		log.Fatal(err)
//	}
//
//	// Commit changes
//	if err := tx.Commit(); err != nil {
//		log.Fatal(err)
//	}
//
// Destructive administrative operations are context-aware as well. Call
// writer.DropAllTables(ctx) with a context whose cancellation and deadline
// should govern object discovery and destructive DDL. A dialect may briefly
// outlive cancellation to restore connection-local safety settings.
//
// # Database Information
//
// Connection metadata is available through the Info() method:
//
//	info := conn.Info()
//	fmt.Printf("Dialect: %s\n", info.Dialect)
//	fmt.Printf("Version: %s\n", info.Version)
//	fmt.Printf("Schema: %s\n", info.Schema)
//
// # URL Format Support
//
// The package handles various database URL formats:
//
//   - Standard URLs: postgres://user:pass@host:port/database
//   - MySQL TCP URLs: mysql://user:pass@tcp(host:port)/database
//   - SQLite URLs: sqlite:///absolute/path.db, sqlite://relative.db, sqlite:///:memory:
//   - Remote libsql URLs: libsql://host and libsql+ws://host
//   - SQL Server URLs: sqlserver://user:pass@host:1433?database=name&encrypt=disable
//   - Connection parameters: URLs with query parameters for SSL, charset, etc.
//
// # Integration with Ptah
//
// This package integrates with other Ptah components:
//
//   - migration/migrator: uses connections for migration execution
//   - migration/generator: uses schema reading for migration generation
//   - migration/schemadiff: consumes the read schema for comparison
//   - core/goschema: provides the desired schema the comparison runs against
//
// # Thread Safety
//
// A DatabaseConnection may be used from several goroutines for reads:
// [ReadSchemaWithSchemasContext] and its context-free form give each call a
// reader of its own, the query methods go straight to database/sql's pool, and
// [DatabaseConnection.Info] returns a copy. Two concurrent schema reads at different schema scopes are
// two independent reads. That is a property of where the schema allow-list is
// held rather than a free one: the allow-list is reader state, so a read that
// scoped a reader shared with another read would let each see the other's
// scope (stokaro/ptah#2246).
//
// Writes are not covered by that. A schema writer, and the transaction a writer
// opens, belong to one goroutine at a time: [DatabaseConnection.WithSession]
// and [DatabaseConnection.WithExecutor] hand out connection copies for exactly
// that reason. Open a second connection instead of sharing a writer.
package dbschema
