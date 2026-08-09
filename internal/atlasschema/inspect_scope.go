package atlasschema

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/dbschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/schemascope"
)

// readInspectSchema reads the schemas one inspection describes.
//
// It exists because "which schemas" has three answers and the reader defaults
// to the narrowest of them. `--schema` names them outright; otherwise the URL
// decides, and a URL that pins no schema puts the whole realm under inspection.
// Reading only the connection's own schema there described one schema of a
// multi-schema database and said nothing about the rest — a database holding
// `public.a` and `extra.b` lost `extra` and its table entirely, where the
// pinned community binary v1.3.0 lists both (stokaro/ptah#1264, measured on
// PostgreSQL 17).
//
// The schema names are always passed explicitly, even when they resolve to the
// one schema the reader would have defaulted to. That is what makes the reader
// report the schemas themselves rather than only their tables, which is the
// other half of the same issue: an empty database rendered as `{}` where the
// binary renders its schema.
func readInspectSchema(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	requested []string,
) (*dbschematypes.DBSchema, error) {
	names, err := inspectSchemaNames(ctx, conn, requested)
	if err != nil {
		return nil, err
	}
	schema, err := dbschema.ReadSchemaWithSchemas(conn, names)
	if err != nil {
		return nil, err
	}
	schema = withConnectedSchemaRow(schema, conn.Info(), requested)
	if err := inspectSchemaAttributes(ctx, conn, schema); err != nil {
		return nil, err
	}
	return schema, nil
}

// inspectSchemaNames resolves the schema names to read.
//
// The decision itself belongs to [schemascope.ReadNames], which every read
// feeding a comparison consumes; this wrapper only labels the failure. An
// explicit `--schema` wins over the URL's scope: it is the operator naming what
// they want described, and a name that turns out not to exist stays absent from
// the result rather than being replaced by a schema they did not ask for.
// Measured on PostgreSQL 17, `--schema nope` renders `{}` on both
// implementations.
func inspectSchemaNames(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	requested []string,
) ([]string, error) {
	names, err := schemascope.ReadNames(ctx, conn.Info(), requested, conn)
	if err != nil {
		return nil, fmt.Errorf("read database schema: %w", err)
	}
	return names, nil
}

// withConnectedSchemaRow makes sure a run that named no schema describes the
// one it is connected to, even on a dialect whose reader reports no schema rows
// of its own.
//
// Only the PostgreSQL-family reader answers DBSchema.Schemas today, so on
// MySQL, MariaDB, SQLite and ClickHouse an empty database still arrived here
// with nothing in it and rendered as an empty document. The connection is the
// proof the schema exists — it was opened against it.
//
// It does not fire when `--schema` named something, because there the empty
// result is the answer: the named schema is not there.
func withConnectedSchemaRow(
	schema *dbschematypes.DBSchema,
	info dbschematypes.DBInfo,
	requested []string,
) *dbschematypes.DBSchema {
	if schema == nil || len(SplitSchemaNames(requested)) > 0 {
		return schema
	}
	name := connectedSchemaName(info)
	if name == "" {
		return schema
	}
	if slices.ContainsFunc(schema.Schemas, func(row dbschematypes.DBSchemaInfo) bool {
		return row.Name == name
	}) {
		return schema
	}
	schema.Schemas = append(schema.Schemas, dbschematypes.DBSchemaInfo{Name: name})
	return schema
}

// connectedSchemaName is the schema an unqualified object of this connection
// belongs to: `current_schema()` on PostgreSQL, the database on MySQL-family
// and ClickHouse connections, and `main` on SQLite.
//
// dbschema resolves all of those into DBInfo.Schema at connect time, so this
// only has to refuse an empty one rather than re-derive it.
func connectedSchemaName(info dbschematypes.DBInfo) string {
	return strings.TrimSpace(info.Schema)
}

// inspectSchemaAttributes fills in the attributes of a schema row the reader
// did not describe, so a schema-only document is not thinner than the one the
// pinned binary prints.
//
// MySQL-family schemas carry a character set and a collation, and the pinned
// binary prints both for an empty database: measured on MySQL 9.7,
// `{"name":"…","charset":"utf8mb4","collate":"utf8mb4_0900_ai_ci"}`. Nothing
// else reaches this: the PostgreSQL-family reader fills its own rows including
// the comment, and SQLite's single namespace carries no attributes on either
// implementation.
func inspectSchemaAttributes(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	schema *dbschematypes.DBSchema,
) error {
	if schema == nil {
		return nil
	}
	info := conn.Info()
	switch platform.NormalizeDialect(info.Dialect) {
	case platform.MySQL, platform.MariaDB:
	default:
		return nil
	}
	for i := range schema.Schemas {
		row := &schema.Schemas[i]
		if row.Charset != "" || row.Collate != "" {
			continue
		}
		var charset, collate string
		err := conn.QueryRowContext(ctx, `
			SELECT default_character_set_name, default_collation_name
			FROM information_schema.schemata
			WHERE schema_name = ?`, row.Name).Scan(&charset, &collate)
		if errors.Is(err, sql.ErrNoRows) {
			// A schema that is not in the catalog has no attributes to carry.
			// Inspection is read-only, so it describes what it found rather
			// than failing on a schema that went away between the two reads.
			continue
		}
		if err != nil {
			return fmt.Errorf("read schema %q attributes: %w", row.Name, err)
		}
		row.Charset = charset
		row.Collate = collate
	}
	return nil
}
