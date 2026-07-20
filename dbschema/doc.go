// Package dbschema provides database schema operations and connection management for the Ptah schema management system.
//
// This package implements comprehensive database connectivity and schema manipulation capabilities
// across multiple database platforms. It provides unified interfaces for reading database schemas,
// writing schema changes, and managing database connections with proper abstraction over
// platform-specific implementations.
//
// # Overview
//
// The dbschema package serves as the primary interface between Ptah and actual databases.
// It abstracts away database-specific details while providing consistent APIs for schema
// operations across PostgreSQL-family, MySQL, MariaDB, SQLite, ClickHouse, and
// SQL Server platforms.
//
// # Key Features
//
//   - Unified database connection management across multiple platforms
//   - Schema reading and introspection with detailed metadata extraction
//   - Schema writing and modification with transaction support
//   - Database URL parsing and connection string handling
//   - Platform-specific optimizations and feature support
//   - Comprehensive error handling and connection management
//
// # Core Components
//
// The package provides these main types:
//
//   - DatabaseConnection: Main connection wrapper with unified interface
//   - SchemaReader: Interface for reading database schemas
//   - SchemaWriter: Interface for writing schema changes
//   - DBInfo: Database connection and metadata information
//
// # Supported Databases
//
// The package supports these database platforms:
//
//   - PostgreSQL: Full support with enum types, SERIAL columns, and advanced constraints
//   - MySQL: Complete support with AUTO_INCREMENT, ENGINE specifications, and charset handling
//   - MariaDB: Full compatibility using MySQL driver with MariaDB-specific optimizations
//   - SQLite: Local file, URI, and in-memory databases with PRAGMA-backed introspection
//   - ClickHouse: MergeTree-family subset with system catalog introspection
//   - SQL Server: T-SQL subset with schemas, IDENTITY, indexes, and core constraints
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
//	schema, err := conn.Reader().ReadSchema()
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Access schema components
//	for _, table := range schema.Tables {
//		fmt.Printf("Table: %s\n", table.Name)
//		for _, column := range table.Columns {
//			fmt.Printf("  Column: %s (%s)\n", column.Name, column.Type)
//		}
//	}
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
// # Database Information
//
// Connection metadata is available through the Info() method:
//
//	info := conn.Info()
//	fmt.Printf("Dialect: %s\n", info.Dialect)
//	fmt.Printf("Version: %s\n", info.Version)
//	fmt.Printf("Schema: %s\n", info.Schema)
//
// # Platform-Specific Implementations
//
// The package includes platform-specific implementations:
//
//   - postgres/: PostgreSQL-specific reader and writer implementations
//   - mysql/: MySQL/MariaDB-specific reader and writer implementations
//   - mssql/: SQL Server-specific reader and writer implementations
//   - clickhouse/: ClickHouse-specific reader and writer implementations
//   - sqlite/: SQLite-specific reader and writer implementations
//   - types/: Common type definitions and interfaces
//
// # URL Format Support
//
// The package handles various database URL formats:
//
//   - Standard URLs: postgres://user:pass@host:port/database
//   - MySQL TCP URLs: mysql://user:pass@tcp(host:port)/database
//   - SQLite URLs: sqlite:///absolute/path.db, sqlite://relative.db, sqlite:///:memory:
//   - SQL Server URLs: sqlserver://user:pass@host:1433?database=name&encrypt=disable
//   - Connection parameters: URLs with query parameters for SSL, charset, etc.
//
// # Transaction Safety
//
// All schema writing operations support transactions:
//
//   - Automatic transaction management for complex operations
//   - Rollback support for failed operations
//   - Proper resource cleanup and connection management
//   - Dry-run capabilities for testing schema changes
//
// # Error Handling
//
// The package provides comprehensive error handling:
//
//   - Database connection errors with detailed context
//   - SQL execution errors with statement information
//   - Transaction management errors with rollback support
//   - Schema parsing errors with object-specific details
//
// # Integration with Ptah
//
// This package integrates with other Ptah components:
//
//   - ptah/migration/migrator: Uses connections for migration execution
//   - ptah/migration/generator: Uses schema reading for migration generation
//   - ptah/migration/schemadiff: Consumes database schema for comparison
//   - ptah/core/goschema: Provides target schema for comparison
//
// # Performance Considerations
//
// The package is optimized for:
//
//   - Efficient database connection pooling and management
//   - Fast schema introspection with minimal queries
//   - Batch SQL execution for complex schema changes
//   - Memory-efficient handling of large schema objects
//
// # Security Features
//
// The package includes security considerations:
//
//   - Password masking in connection string formatting
//   - Proper SQL parameter binding to prevent injection
//   - Connection validation and health checking
//   - Secure handling of database credentials
//
// # Thread Safety
//
// DatabaseConnection instances are thread-safe for read operations but should
// not be used concurrently for write operations that involve transactions.
// Multiple connections can be created for concurrent access patterns.
package dbschema
