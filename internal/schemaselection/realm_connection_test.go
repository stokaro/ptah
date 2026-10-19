package schemaselection_test

import (
	"context"
	"database/sql"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/dbreset"
	"ptah.run/internal/dbschema/mysql"
	"ptah.run/internal/dbschema/postgres"
)

// realmConnection is the connection a realm read runs on: a scripted database
// and the writer whose catalog lists the realm.
type realmConnection struct {
	*sql.DB
	writer catalog.SchemaWriter
}

// SchemaWriter returns the writer the connection was given.
func (c realmConnection) SchemaWriter() catalog.SchemaWriter { return c.writer }

// postgresConnection reads the realm of a PostgreSQL-family dialect through
// db, as the family's writer reads it.
func postgresConnection(db *sql.DB, dialect string) realmConnection {
	return realmConnection{
		DB:     db,
		writer: postgres.NewPostgreSQLWriterForRunnerWithCapabilities(db, "", dialect, capability.ForDialect(dialect)),
	}
}

// mysqlConnection reads the realm of a MySQL-family dialect through db, as the
// family's writer reads it.
func mysqlConnection(db *sql.DB, dialect string) realmConnection {
	return realmConnection{DB: db, writer: mysql.NewMySQLWriter(db, "", dialect)}
}

// writerlessConnection reads through db with a writer that offers no catalog.
func writerlessConnection(db *sql.DB) realmConnection {
	return realmConnection{DB: db, writer: struct{ catalog.SchemaWriter }{}}
}

// acmeScope is the scope catalog of an engine nobody ships, which lists the
// realm it was given.
type acmeScope struct {
	schemas []string
}

func (a acmeScope) Tables(context.Context, dbreset.Querier, capability.Capabilities, string) ([]dbreset.ScopeTable, error) {
	return nil, nil
}

func (a acmeScope) Realm(context.Context, dbreset.Querier, capability.Capabilities) ([]dbreset.ScopeTable, error) {
	return nil, nil
}

func (a acmeScope) RealmSchemas(context.Context, dbreset.Querier, capability.Capabilities) ([]string, error) {
	return a.schemas, nil
}

// acmeWriter is that engine's writer. The embedded writer is never called.
type acmeWriter struct {
	catalog.SchemaWriter
	scope dbreset.ScopeCatalog
}

func (w acmeWriter) ScopeCatalog() dbreset.ScopeCatalog { return w.scope }
