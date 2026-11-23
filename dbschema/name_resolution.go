package dbschema

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/sqlident"
)

// ResolveNamesAs returns the pinned session dc with its unqualified names
// resolving as they resolve on the session like describes. The server's
// search path is set to like's, and the returned connection's reader and
// writer describe the database relative to like's default schema, so a name
// the reader leaves unqualified is one the session resolves to that schema.
//
// A rehearsal needs it when the dev database and the target resolve one
// unqualified name to two schemas. Measured on PostgreSQL 18 with a role
// `rebase` whose search_path is the default `"$user", public`:
// current_schema() is `rebase` in a database that holds schema `rebase`, and
// `public` in a sibling database that does not. A plan for the first names an
// object of `rebase` without a schema, and the second session would create
// it in `public`.
//
// Only an engine that resolves names along a search path can move the
// resolution, and only on a session pinned by [DatabaseConnection.WithSession]:
// the setting belongs to the physical session, which WithSession discards when
// its callback returns, so it reaches no other user of the pool. Any other
// connection is refused, and so is a like that names no schema. dc itself is
// not changed, but the setting is its session's, so dc resolves names as the
// returned connection does once this returns without an error.
func (dc *DatabaseConnection) ResolveNamesAs(ctx context.Context, like catalog.ServerInfo) (*DatabaseConnection, error) {
	if dc == nil || !dc.pinned || dc.session == nil {
		return nil, errors.New("moving name resolution requires a pinned database session")
	}
	described, _ := platform.Lookup(dc.info.Dialect)
	if described.Schemas.Selection != descriptor.SchemaSelectionSearchPath {
		return nil, fmt.Errorf("dialect %q does not resolve names along a search path", dc.info.Dialect)
	}
	if dc.factoriesFor == nil {
		return nil, errors.New("database connection does not support session rebinding")
	}
	schema := like.Schema
	if schema == "" {
		return nil, errors.New("moving name resolution requires a schema to resolve names in")
	}
	path := slices.Clone(like.IdentifierSemantics.SearchPath)
	if !slices.Contains(path, schema) {
		path = append([]string{schema}, path...)
	}
	quoted := make([]string, 0, len(path))
	for _, name := range path {
		quoted = append(quoted, sqlident.Ident(dc.info.Dialect, name))
	}
	if _, err := dc.session.ExecContext(ctx, "SET search_path TO "+strings.Join(quoted, ", ")); err != nil {
		return nil, fmt.Errorf("set the session's search_path: %w", err)
	}
	moved := *dc
	moved.info.Schema = schema
	moved.info.IdentifierSemantics = dc.info.IdentifierSemantics.Clone()
	moved.info.IdentifierSemantics.DefaultSchema = schema
	moved.info.IdentifierSemantics.SearchPath = path
	moved.newReader, moved.newWriter = dc.factoriesFor(moved.info)
	moved.reader = moved.newReader(dc.runner)
	moved.writer = moved.newWriter(dc.runner, dc.session)
	moved.executor = nil
	if dc.writer != nil && dc.writer.IsDryRun() {
		moved.writer.SetDryRun(true)
	}
	return &moved, nil
}
