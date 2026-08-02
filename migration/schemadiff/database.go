package schemadiff

import (
	"context"
	"fmt"

	"go.5x5.cz/ptah/config"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/tableref"
	"go.5x5.cz/ptah/migration/internal/generatedschema"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

// CompareWithDatabase resolves live catalog identifier equivalence and compares
// the target schema with the connected database schema.
func CompareWithDatabase(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	generated *goschema.Database,
	database *dbschematypes.DBSchema,
	opts *config.CompareOptions,
) (*difftypes.SchemaDiff, error) {
	if conn == nil {
		return nil, fmt.Errorf("compare schemas: database connection is nil")
	}
	info := conn.Info()
	names := collectIdentifierNames(generated, database, info.Schema)
	semantics, err := conn.ResolveIdentifierSemantics(ctx, names)
	if err != nil {
		return nil, fmt.Errorf("compare schemas: %w", err)
	}
	info.IdentifierSemantics = semantics
	return CompareWithDatabaseInfo(generated, database, info, opts)
}

func collectIdentifierNames(
	generated *goschema.Database,
	database *dbschematypes.DBSchema,
	defaultSchema string,
) []string {
	names := []string{defaultSchema}
	names = appendGeneratedIdentifierNames(names, generated)
	return appendDatabaseIdentifierNames(names, database)
}

func appendGeneratedIdentifierNames(
	names []string,
	generated *goschema.Database,
) []string {
	if generated == nil {
		return names
	}
	for _, field := range generated.Fields {
		names = append(names, field.Name)
	}
	for _, table := range generated.Tables {
		names = append(names, table.Schema, table.Name)
		for _, field := range generatedschema.FieldsForTable(generated, table) {
			names = append(names, field.Name)
		}
	}
	for _, index := range generated.Indexes {
		names = append(names, index.Name)
		names = appendQualifiedIdentifier(names, index.TableName)
		names = append(names, index.Fields...)
		names = append(names, index.IncludeColumns...)
		for _, part := range index.Parts {
			names = append(names, part.Name)
		}
	}
	return names
}

func appendDatabaseIdentifierNames(
	names []string,
	database *dbschematypes.DBSchema,
) []string {
	if database == nil {
		return names
	}
	for _, table := range database.Tables {
		names = append(names, table.Schema, table.Name)
		for _, column := range table.Columns {
			names = append(names, column.Name)
		}
	}
	for _, index := range database.Indexes {
		names = append(names, index.Schema, index.TableName, index.Name)
		names = append(names, index.Columns...)
		for _, part := range index.Parts {
			names = append(names, part.Name)
		}
	}
	return names
}

func appendQualifiedIdentifier(names []string, value string) []string {
	ref, ok := tableref.Parse(value)
	if !ok {
		return append(names, value)
	}
	if !ref.Qualified {
		return append(names, ref.Name)
	}
	return append(names, ref.Schema, ref.Name)
}
