// Package schemaops contains shared command-line schema comparison helpers.
package schemaops

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/stokaro/ptah/cmd/internal/dbcli"
	"github.com/stokaro/ptah/config"
	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/dbschema"
	dbschematypes "github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/migration/schemadiff"
	difftypes "github.com/stokaro/ptah/migration/schemadiff/types"
)

// CompareOptions configures a live schema comparison.
type CompareOptions struct {
	RootDir        string
	DatabaseURL    string
	ConnectTimeout time.Duration
	IgnoredTables  []string
	Schemas        []string
}

// CompareResult is the output of a live schema comparison.
type CompareResult struct {
	RootDir     string
	DatabaseURL string
	Dialect     string
	Generated   *goschema.Database
	Database    *dbschematypes.DBSchema
	Diff        *difftypes.SchemaDiff
}

// Compare parses Go entities, reads the live database schema, applies command
// filters, and returns a dialect-aware schema diff.
func Compare(ctx context.Context, opts CompareOptions) (*CompareResult, error) {
	if opts.RootDir == "" {
		opts.RootDir = "."
	}
	if opts.DatabaseURL == "" {
		return nil, fmt.Errorf("database URL is required")
	}

	absPath, err := filepath.Abs(opts.RootDir)
	if err != nil {
		return nil, fmt.Errorf("error resolving path: %w", err)
	}

	generated, err := goschema.ParseDir(absPath)
	if err != nil {
		return nil, fmt.Errorf("error parsing Go entities: %w", err)
	}

	connectCtx, cancelConnect := dbcli.ConnectContext(ctx, opts.ConnectTimeout)
	conn, err := dbschema.ConnectToDatabase(connectCtx, opts.DatabaseURL)
	cancelConnect()
	if err != nil {
		return nil, fmt.Errorf("error connecting to database: %w", err)
	}
	defer dbschema.CloseAndWarn(conn)

	dbSchema, err := dbschema.ReadSchemaWithSchemas(conn, opts.Schemas)
	if err != nil {
		return nil, fmt.Errorf("error reading database schema: %w", err)
	}

	if len(opts.IgnoredTables) > 0 {
		generated = FilterGeneratedTables(generated, opts.IgnoredTables)
		dbSchema = FilterDatabaseTables(dbSchema, opts.IgnoredTables)
	}

	compareOpts := config.DefaultCompareOptions()
	compareOpts.Dialect = conn.Info().Dialect
	diff := schemadiff.CompareWithOptions(generated, dbSchema, compareOpts)

	return &CompareResult{
		RootDir:     absPath,
		DatabaseURL: dbschema.FormatDatabaseURL(opts.DatabaseURL),
		Dialect:     conn.Info().Dialect,
		Generated:   generated,
		Database:    dbSchema,
		Diff:        diff,
	}, nil
}

// FilterGeneratedTables returns a shallow copy of db without ignored tables and
// their table-scoped schema objects.
func FilterGeneratedTables(db *goschema.Database, ignoredTables []string) *goschema.Database {
	if db == nil {
		return nil
	}
	ignored := tableSet(ignoredTables)
	if len(ignored) == 0 {
		return db
	}

	filtered := *db
	ignoredStructs := make(map[string]struct{})
	filtered.Tables = keep(db.Tables, func(table goschema.Table) bool {
		if isIgnoredTable(ignored, table.QualifiedName(), table.Name) {
			ignoredStructs[table.StructName] = struct{}{}
			return false
		}
		return true
	})
	ignoredEnumRefs := make(map[string]struct{})
	filtered.Fields = keep(db.Fields, func(field goschema.Field) bool {
		_, ignore := ignoredStructs[field.StructName]
		if ignore && strings.HasPrefix(field.Type, "enum_") {
			ignoredEnumRefs[field.Type] = struct{}{}
		}
		return !ignore
	})
	filtered.Indexes = keep(db.Indexes, func(index goschema.Index) bool {
		if _, ignore := ignoredStructs[index.StructName]; ignore {
			return false
		}
		if index.TableName != "" {
			return !isIgnoredTable(ignored, index.TableName)
		}
		return true
	})
	filtered.Constraints = keep(db.Constraints, func(constraint goschema.Constraint) bool {
		if _, ignore := ignoredStructs[constraint.StructName]; ignore {
			return false
		}
		if constraint.Table != "" {
			return !isIgnoredTable(ignored, constraint.Table)
		}
		return true
	})
	filtered.EmbeddedFields = keep(db.EmbeddedFields, func(field goschema.EmbeddedField) bool {
		_, ignore := ignoredStructs[field.StructName]
		return !ignore
	})
	filtered.RLSPolicies = keep(db.RLSPolicies, func(policy goschema.RLSPolicy) bool {
		return !isIgnoredTable(ignored, policy.Table)
	})
	filtered.RLSEnabledTables = keep(db.RLSEnabledTables, func(table goschema.RLSEnabledTable) bool {
		return !isIgnoredTable(ignored, table.Table)
	})
	filtered.Dependencies = filterDependencies(db.Dependencies, ignored)
	filtered.SelfReferencingForeignKeys = filterSelfReferencingForeignKeys(db.SelfReferencingForeignKeys, ignored)
	filtered.Enums = keepGeneratedEnums(db.Enums, filtered.Fields, ignoredEnumRefs)

	return &filtered
}

// FilterDatabaseTables returns a shallow copy of db without ignored tables and
// their table-scoped schema objects.
func FilterDatabaseTables(db *dbschematypes.DBSchema, ignoredTables []string) *dbschematypes.DBSchema {
	if db == nil {
		return nil
	}
	ignored := tableSet(ignoredTables)
	if len(ignored) == 0 {
		return db
	}

	filtered := *db
	ignoredEnumRefs := make(map[string]struct{})
	filtered.Tables = keep(db.Tables, func(table dbschematypes.DBTable) bool {
		ignore := isIgnoredTable(ignored, table.QualifiedName(), table.Name)
		if ignore {
			addDatabaseEnumRefs(ignoredEnumRefs, table.Columns)
		}
		return !ignore
	})
	filtered.Indexes = keep(db.Indexes, func(index dbschematypes.DBIndex) bool {
		return !isIgnoredTable(ignored, index.QualifiedTableName(), index.TableName)
	})
	filtered.Constraints = keep(db.Constraints, func(constraint dbschematypes.DBConstraint) bool {
		return !isIgnoredTable(ignored, constraint.QualifiedTableName(), constraint.TableName)
	})
	filtered.RLSPolicies = keep(db.RLSPolicies, func(policy dbschematypes.DBRLSPolicy) bool {
		return !isIgnoredTable(ignored, policy.Table)
	})
	filtered.Enums = keepDatabaseEnums(db.Enums, filtered.Tables, ignoredEnumRefs)

	return &filtered
}

func tableSet(names []string) map[string]struct{} {
	set := make(map[string]struct{})
	for _, name := range names {
		name = strings.TrimSpace(name)
		if name != "" {
			set[name] = struct{}{}
		}
	}
	return set
}

func isIgnoredTable(ignored map[string]struct{}, names ...string) bool {
	for _, name := range names {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		if _, ok := ignored[name]; ok {
			return true
		}
		if idx := strings.LastIndex(name, "."); idx >= 0 {
			if _, ok := ignored[name[idx+1:]]; ok {
				return true
			}
		}
	}
	return false
}

func keep[T any](items []T, shouldKeep func(T) bool) []T {
	out := make([]T, 0, len(items))
	for _, item := range items {
		if shouldKeep(item) {
			out = append(out, item)
		}
	}
	return out
}

func filterDependencies(in map[string][]string, ignored map[string]struct{}) map[string][]string {
	if in == nil {
		return nil
	}
	out := make(map[string][]string, len(in))
	for table, deps := range in {
		if isIgnoredTable(ignored, table) {
			continue
		}
		out[table] = keep(deps, func(dep string) bool {
			return !isIgnoredTable(ignored, dep)
		})
	}
	return out
}

func filterSelfReferencingForeignKeys(
	in map[string][]goschema.SelfReferencingFK,
	ignored map[string]struct{},
) map[string][]goschema.SelfReferencingFK {
	if in == nil {
		return nil
	}
	out := make(map[string][]goschema.SelfReferencingFK, len(in))
	for table, refs := range in {
		if isIgnoredTable(ignored, table) {
			continue
		}
		out[table] = refs
	}
	return out
}

func keepGeneratedEnums(enums []goschema.Enum, fields []goschema.Field, ignoredEnumRefs map[string]struct{}) []goschema.Enum {
	referenced := make(map[string]struct{})
	for _, field := range fields {
		if strings.HasPrefix(field.Type, "enum_") {
			referenced[field.Type] = struct{}{}
		}
	}
	return keep(enums, func(enum goschema.Enum) bool {
		if _, stillReferenced := referenced[enum.Name]; stillReferenced {
			return true
		}
		_, wasIgnored := ignoredEnumRefs[enum.Name]
		return !wasIgnored
	})
}

func keepDatabaseEnums(
	enums []dbschematypes.DBEnum,
	tables []dbschematypes.DBTable,
	ignoredEnumRefs map[string]struct{},
) []dbschematypes.DBEnum {
	referenced := make(map[string]struct{})
	for _, table := range tables {
		addDatabaseEnumRefs(referenced, table.Columns)
	}
	return keep(enums, func(enum dbschematypes.DBEnum) bool {
		if _, stillReferenced := referenced[enum.Name]; stillReferenced {
			return true
		}
		_, wasIgnored := ignoredEnumRefs[enum.Name]
		return !wasIgnored
	})
}

func addDatabaseEnumRefs(out map[string]struct{}, columns []dbschematypes.DBColumn) {
	for _, column := range columns {
		ref, ok := databaseEnumRef(column)
		if ok {
			out[ref] = struct{}{}
		}
	}
}

func databaseEnumRef(column dbschematypes.DBColumn) (string, bool) {
	if column.UDTName == "" {
		return "", false
	}

	switch strings.ToUpper(column.DataType) {
	case "USER-DEFINED":
		return column.UDTName, true
	case "ARRAY":
		ref := strings.TrimPrefix(column.UDTName, "_")
		return ref, ref != ""
	case "":
		return column.UDTName, true
	default:
		return "", false
	}
}
