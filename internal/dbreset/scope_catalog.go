package dbreset

import (
	"context"

	"ptah.run/core/platform/capability"
)

// ScopeCatalog is what an engine's catalog answers about the schemas and the
// base tables a connection reaches, on an engine whose URL selects a schema:
// the tables of the one schema a URL pins, and every schema of the realm a URL
// that pins none reaches. The clean check of internal/migrateclean judges a
// database with it before Ptah adopts the database or resets it as a dev
// database, and internal/schemaselection lists a realm with it. A writer
// offers one for the engines whose catalog it reads; neither consumer names an
// engine.
//
// Each read runs through the querier the caller hands it, so it runs on the
// caller's session, and caps are the capabilities that session resolved.
type ScopeCatalog interface {
	// Tables lists the base tables of schema that the clean check counts,
	// in the order the catalog returns them, each with the schema the
	// catalog read it from. The empty schema is the one the session lands
	// in.
	Tables(ctx context.Context, q Querier, caps capability.Capabilities, schema string) ([]ScopeTable, error)
	// Realm lists every schema of the realm with each base table it holds
	// that the clean check counts, in the order the catalog returns them. A
	// schema that holds no such table is listed once, with an empty Name,
	// since an empty schema is part of the realm too.
	Realm(ctx context.Context, q Querier, caps capability.Capabilities) ([]ScopeTable, error)
	// RealmSchemas lists the name of every schema of the realm, in the
	// order the catalog returns them. It reads the schemas Realm reads,
	// through a query of its own that reads no table.
	RealmSchemas(ctx context.Context, q Querier, caps capability.Capabilities) ([]string, error)
}

// ScopeTable is a base table a [ScopeCatalog] read reports, or a schema that
// holds none.
type ScopeTable struct {
	// Schema is the schema the catalog read the table from.
	Schema string
	// Name is the table's name, empty for a schema that holds no table.
	Name string
}

// QueryScopeTables runs query through q and reads each row as the schema and
// the name of a [ScopeTable], in the order the query returns them. It is the
// row reading a [ScopeCatalog] shares; an error is the driver's, unwrapped.
func QueryScopeTables(ctx context.Context, q Querier, query string, args ...any) ([]ScopeTable, error) {
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var tables []ScopeTable
	for rows.Next() {
		var table ScopeTable
		if err := rows.Scan(&table.Schema, &table.Name); err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return tables, nil
}

// QueryNames runs query through q and reads the one column of each row as a
// name, in the order the query returns them. An error is the driver's,
// unwrapped.
func QueryNames(ctx context.Context, q Querier, query string) ([]string, error) {
	rows, err := q.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return names, nil
}
