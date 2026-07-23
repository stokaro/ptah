// Package schemaclean plans and executes destructive schema cleanup.
package schemaclean

import (
	"fmt"
	"slices"
	"strings"

	"github.com/stokaro/ptah/dbschema"
	dbschematypes "github.com/stokaro/ptah/dbschema/types"
)

const (
	ObjectTypeEnum       = "enum"
	ObjectTypeForeignKey = "foreign_key"
	ObjectTypeSequence   = "sequence"
	ObjectTypeTable      = "table"
)

type Options struct {
	DryRun bool
}

type Plan struct {
	Objects []Object
	Changes []Change
}

type Object struct {
	Type   string
	Schema string
	Table  string
	Name   string
}

type Change struct {
	Type   string
	Schema string
	Table  string
	Name   string
	Cmd    string
}

func Inspect(conn *dbschema.DatabaseConnection) (Plan, error) {
	schema, err := conn.Reader().ReadSchema()
	if err != nil {
		return Plan{}, fmt.Errorf("inspect schema before cleanup: %w", err)
	}
	dialect := conn.Info().Dialect
	objects := cleanupObjects(schema, dialect)
	runtimeObjects, err := inspectRuntimeObjects(conn)
	if err != nil {
		return Plan{}, err
	}
	objects = append(objects, runtimeObjects...)
	return PlanFromObjects(objects, dialect), nil
}

func Execute(conn *dbschema.DatabaseConnection, opts Options) (Plan, error) {
	plan, err := Inspect(conn)
	if err != nil {
		return Plan{}, err
	}
	if opts.DryRun {
		return plan, nil
	}
	if err := Apply(conn); err != nil {
		return Plan{}, err
	}
	return plan, nil
}

func Apply(conn *dbschema.DatabaseConnection) error {
	conn.SchemaWriter().SetDryRun(false)
	if err := conn.SchemaWriter().DropAllTables(); err != nil {
		return fmt.Errorf("drop schema objects: %w", err)
	}
	return nil
}

func PlanFromSchema(schema *dbschematypes.DBSchema, dialect string) Plan {
	if schema == nil {
		return Plan{}
	}
	return PlanFromObjects(cleanupObjects(schema, dialect), dialect)
}

func PlanFromObjects(objects []Object, dialect string) Plan {
	objects = append([]Object(nil), objects...)
	sortObjects(objects)
	changes := make([]Change, 0, len(objects))
	for _, object := range objects {
		changes = append(changes, Change{
			Type:   object.Type,
			Schema: object.Schema,
			Table:  object.Table,
			Name:   object.Name,
			Cmd:    dropCommand(dialect, object),
		})
	}
	return Plan{
		Objects: objects,
		Changes: changes,
	}
}

func cleanupObjects(schema *dbschematypes.DBSchema, dialect string) []Object {
	if schema == nil {
		return nil
	}
	objects := make([]Object, 0, len(schema.Tables)+len(schema.Enums)+len(schema.Constraints))
	if supportsExplicitForeignKeyCleanup(dialect) {
		for _, constraint := range schema.Constraints {
			if !isForeignKeyConstraint(constraint) {
				continue
			}
			objects = append(objects, Object{
				Type:   ObjectTypeForeignKey,
				Schema: constraint.Schema,
				Table:  constraint.TableName,
				Name:   constraint.Name,
			})
		}
	}
	for _, table := range schema.Tables {
		if !isCleanupTableType(table.Type) {
			continue
		}
		objects = append(objects, Object{
			Type:   ObjectTypeTable,
			Schema: table.Schema,
			Name:   table.Name,
		})
	}
	if !supportsStandaloneEnums(dialect) {
		return objects
	}
	for _, enum := range schema.Enums {
		objects = append(objects, Object{
			Type: ObjectTypeEnum,
			Name: enum.Name,
		})
	}
	return objects
}

func inspectRuntimeObjects(conn *dbschema.DatabaseConnection) ([]Object, error) {
	if !supportsStandaloneSequences(conn.Info().Dialect) {
		return nil, nil
	}
	schema := strings.TrimSpace(conn.Info().Schema)
	if schema == "" {
		schema = "public"
	}
	rows, err := conn.Query(`
		SELECT sequence_name
		FROM information_schema.sequences
		WHERE sequence_schema = $1
		ORDER BY sequence_name`, schema)
	if err != nil {
		return nil, fmt.Errorf("inspect cleanup sequences: %w", err)
	}
	defer rows.Close()

	objects := []Object{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan cleanup sequence: %w", err)
		}
		objects = append(objects, Object{
			Type:   ObjectTypeSequence,
			Schema: schema,
			Name:   name,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate cleanup sequences: %w", err)
	}
	return objects, nil
}

func sortObjects(objects []Object) {
	slices.SortFunc(objects, func(a, b Object) int {
		if cmp := strings.Compare(a.Type, b.Type); cmp != 0 {
			return cmp
		}
		if cmp := strings.Compare(a.Schema, b.Schema); cmp != 0 {
			return cmp
		}
		if cmp := strings.Compare(a.Table, b.Table); cmp != 0 {
			return cmp
		}
		return strings.Compare(a.Name, b.Name)
	})
}

func dropCommand(dialect string, object Object) string {
	name := qualifiedName(dialect, object.Schema, object.Name)
	switch object.Type {
	case ObjectTypeEnum:
		return "DROP TYPE IF EXISTS " + name + " CASCADE"
	case ObjectTypeForeignKey:
		return "ALTER TABLE " + qualifiedName(dialect, object.Schema, object.Table) +
			" DROP CONSTRAINT " + quoteIdent(dialect, object.Name)
	case ObjectTypeSequence:
		return "DROP SEQUENCE IF EXISTS " + name + " CASCADE"
	case ObjectTypeTable:
		return dropTableCommand(dialect, name)
	default:
		return ""
	}
}

func dropTableCommand(dialect, name string) string {
	switch strings.ToLower(strings.TrimSpace(dialect)) {
	case "postgres", "postgresql", "cockroachdb", "yugabytedb":
		return "DROP TABLE IF EXISTS " + name + " CASCADE"
	case "clickhouse":
		return "DROP TABLE IF EXISTS " + name + " SYNC"
	default:
		return "DROP TABLE IF EXISTS " + name
	}
}

func isCleanupTableType(tableType string) bool {
	switch strings.ToUpper(strings.TrimSpace(tableType)) {
	case "", "TABLE", "BASE TABLE":
		return true
	default:
		return false
	}
}

func isForeignKeyConstraint(constraint dbschematypes.DBConstraint) bool {
	return strings.EqualFold(strings.TrimSpace(constraint.Type), "FOREIGN KEY")
}

func supportsExplicitForeignKeyCleanup(dialect string) bool {
	switch strings.ToLower(strings.TrimSpace(dialect)) {
	case "sqlserver", "mssql":
		return true
	default:
		return false
	}
}

func supportsStandaloneEnums(dialect string) bool {
	switch strings.ToLower(strings.TrimSpace(dialect)) {
	case "postgres", "postgresql", "cockroachdb", "yugabytedb":
		return true
	default:
		return false
	}
}

func supportsStandaloneSequences(dialect string) bool {
	switch strings.ToLower(strings.TrimSpace(dialect)) {
	case "postgres", "postgresql":
		return true
	default:
		return false
	}
}

func qualifiedName(dialect, schema, name string) string {
	schema = strings.TrimSpace(schema)
	name = strings.TrimSpace(name)
	if schema == "" {
		return quoteIdent(dialect, name)
	}
	return quoteIdent(dialect, schema) + "." + quoteIdent(dialect, name)
}

func quoteIdent(dialect, name string) string {
	switch strings.ToLower(strings.TrimSpace(dialect)) {
	case "mysql", "mariadb", "clickhouse":
		return "`" + strings.ReplaceAll(name, "`", "``") + "`"
	case "sqlserver", "mssql":
		return "[" + strings.ReplaceAll(name, "]", "]]") + "]"
	default:
		return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
	}
}
