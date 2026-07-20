package compare

import (
	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/internal/convert/fromschema"
)

// synthesizeFieldLevelCheckConstraints turns each field-level `check=`
// annotation on an existing database column into a synthetic
// goschema.Constraint of type CHECK so the standard Constraints() diff path
// can compare it against the introspected CHECK from pg_constraint.
//
// The constraint name follows the user-provided `check_name=` value when set,
// otherwise it falls back to the PostgreSQL convention
// "<table>_<column>_check" — which is what PostgreSQL itself uses for
// unnamed inline column-level CHECKs, so the name lines up with whatever the
// reader sees on the DB side.
//
// Columns that do not yet exist in the database are deliberately skipped:
// those CHECKs ship inline as part of CREATE TABLE / ALTER TABLE ADD COLUMN,
// and emitting an ADD CONSTRAINT alongside would attempt to create the same
// constraint twice in the same migration.
//
// Precedence: an explicit table-level constraint declared via
// `//migrator:schema:constraint` that happens to share the synthesized
// name wins — synthesis never clobbers it. See the guard in
// Constraints() where genConstraints is populated.
func synthesizeFieldLevelCheckConstraints(generated *goschema.Database, database *types.DBSchema) []goschema.Constraint {
	if generated == nil || database == nil {
		return nil
	}

	structToTable := make(map[string]goschema.Table, len(generated.Tables))
	for _, t := range generated.Tables {
		structToTable[t.StructName] = t
	}

	dbColumns := make(map[string]struct{}, 16)
	for _, t := range database.Tables {
		for _, c := range t.Columns {
			dbColumns[t.QualifiedName()+"."+c.Name] = struct{}{}
		}
	}

	var synthesized []goschema.Constraint
	for _, f := range generated.Fields {
		if f.Check == "" {
			continue
		}
		table, ok := structToTable[f.StructName]
		tableName := table.QualifiedName()
		tableLeafName := table.Name
		if !ok || tableName == "" {
			tableName = f.StructName
			tableLeafName = f.StructName
		}
		if _, exists := dbColumns[tableName+"."+f.Name]; !exists {
			continue
		}
		name := f.CheckName
		if name == "" {
			name = tableLeafName + "_" + f.Name + "_check"
		}
		synthesized = append(synthesized, goschema.Constraint{
			StructName:      f.StructName,
			Name:            name,
			Type:            "CHECK",
			Table:           tableName,
			CheckExpression: f.Check,
		})
	}
	return synthesized
}

func synthesizeTablePrimaryKeyConstraints(
	generated *goschema.Database,
	database *types.DBSchema,
	dialect string,
) []goschema.Constraint {
	if generated == nil || database == nil {
		return nil
	}

	dbTables := make(map[string]struct{}, len(database.Tables))
	for _, table := range database.Tables {
		dbTables[table.QualifiedName()] = struct{}{}
	}

	var synthesized []goschema.Constraint
	for _, table := range generated.Tables {
		columns := tablePrimaryKeyColumns(table)
		if len(columns) == 0 {
			continue
		}
		if _, exists := dbTables[table.QualifiedName()]; !exists {
			continue
		}

		name := tablePrimaryKeyConstraintName(table, database.Constraints, dialect)
		synthesized = append(synthesized, goschema.Constraint{
			StructName: table.StructName,
			Name:       name,
			Type:       "PRIMARY KEY",
			Table:      table.QualifiedName(),
			Columns:    append([]string(nil), columns...),
		})
	}
	return synthesized
}

func tablePrimaryKeyConstraintName(table goschema.Table, dbConstraints []types.DBConstraint, dialect string) string {
	for _, constraint := range dbConstraints {
		if constraint.Type == "PRIMARY KEY" && constraint.QualifiedTableName() == table.QualifiedName() {
			return constraint.Name
		}
	}

	if isMySQLFamily(dialect) {
		return "PRIMARY"
	}
	return table.Name + "_pkey"
}

// synthesizeFieldLevelForeignKeyConstraints turns each field-level `foreign=`
// annotation on an existing database column into a synthetic
// goschema.Constraint of type FOREIGN KEY so the standard Constraints() diff
// path can compare it against the introspected FK from
// information_schema.referential_constraints. This is what makes on_delete /
// on_update drift on a pre-existing field-level FK observable (issue #189).
//
// The constraint name follows the user-provided `foreign_key_name=` value when
// set, otherwise it falls back to the conventional generated name from
// fromschema.GenerateForeignKeyName ("fk_<table>_<column>"), which is the name
// the planner emits when it creates the FK, so the synthesized name lines up
// with whatever the reader sees on the DB side.
//
// Columns that do not yet exist in the database are deliberately skipped: those
// FKs ship inline as part of CREATE TABLE / ALTER TABLE ADD COLUMN, and emitting
// an ADD CONSTRAINT alongside would attempt to create the same constraint twice
// in the same migration. This mirrors synthesizeFieldLevelCheckConstraints
// exactly and is what keeps added-table generation untouched.
//
// Precedence: an explicit table-level constraint declared via
// `//migrator:schema:constraint` that happens to share the synthesized name
// wins — synthesis never clobbers it (see the guard in Constraints()).
func synthesizeFieldLevelForeignKeyConstraints(generated *goschema.Database, database *types.DBSchema) []goschema.Constraint {
	if generated == nil || database == nil {
		return nil
	}

	dbColumns := make(map[string]struct{}, 16)
	for _, t := range database.Tables {
		for _, c := range t.Columns {
			dbColumns[t.QualifiedName()+"."+c.Name] = struct{}{}
		}
	}

	// Iterate the fields that actually materialize on each concrete table,
	// not the raw parse result. A `foreign=` annotation declared on an
	// embedded inline-relation mixin (e.g. a TenantGroupAwareEntityID base
	// struct carrying tenant_id/group_id/created_by_user_id FKs) lives on the
	// mixin's StructName, which is NOT a table. Synthesizing against
	// f.StructName therefore produced a constraint targeting the Go struct
	// name (ALTER TABLE TenantGroupAwareEntityID ...) once per embedding host,
	// all collapsed onto the same bogus name (issue #197). Resolving via the
	// same CREATE-path embedded expansion that TableColumns uses gives one
	// field per real host table with the host's StructName, so each embedding
	// table gets its own correctly-targeted FK.
	//
	// dedupe guards against a host that both declares a field directly and
	// inherits one of the same (table, constraint name) from a mixin.
	dedupe := make(map[string]struct{})
	var synthesized []goschema.Constraint
	for _, f := range resolveTableFields(generated) {
		if f.Foreign == "" {
			continue
		}
		// resolveTableFields only returns fields that belong to a real table,
		// tagged with that table's name. An empty tableName would mean the
		// field is not part of any table, so skip it rather than synthesize
		// against a struct name.
		tableName := f.qualifiedTableName
		if tableName == "" {
			continue
		}
		if _, exists := dbColumns[tableName+"."+f.Name]; !exists {
			continue
		}
		name := f.ForeignKeyName
		if name == "" {
			name = fromschema.GenerateForeignKeyName(f.tableName, f.Name)
		}
		// Reuse the canonical generate-path parser so the synthesized table /
		// column always match exactly what the planner emits (issue #189
		// follow-up: a single source of truth removes the latent two-parser
		// divergence). A malformed foreign= reference yields nil and is skipped
		// rather than synthesizing a garbage constraint.
		fkRef := fromschema.ParseForeignKeyReference(f.Foreign)
		if fkRef == nil {
			continue
		}
		dedupeKey := tableName + "." + name
		if _, seen := dedupe[dedupeKey]; seen {
			continue
		}
		dedupe[dedupeKey] = struct{}{}
		synthesized = append(synthesized, goschema.Constraint{
			StructName:     f.StructName,
			Name:           name,
			Type:           "FOREIGN KEY",
			Table:          tableName,
			Columns:        []string{f.Name},
			ForeignTable:   fkRef.Table,
			ForeignColumn:  fkRef.Column,
			ForeignColumns: fkRef.ReferencedColumns(),
			OnDelete:       f.OnDelete,
			OnUpdate:       f.OnUpdate,
		})
	}
	return synthesized
}

// resolvedField is a goschema.Field paired with the concrete database table it
// materializes on. Fields declared directly on a table struct carry that
// table's name; fields contributed by an embedded inline / inline-relation
// mixin are expanded once per embedding host and carry the host table's name.
type resolvedField struct {
	goschema.Field
	tableName          string
	qualifiedTableName string
}

// resolveTableFields expands every table's field set the same way the CREATE
// and column-diff paths do (processEmbeddedFieldsForStruct), tagging each
// resulting field with its concrete host table name. This is the single source
// of truth for "which fields end up as columns on which real table", so any
// field-level synthesis (FK drift, and any future field-level constraint
// synthesis) targets host tables rather than mixin struct names (issue #197).
//
// Only fields whose owning struct is a declared table are returned: a
// `foreign=` annotation on a mixin that is never embedded, or on a struct that
// is not a //migrator:schema:table, has no concrete table and must not be
// synthesized.
func resolveTableFields(generated *goschema.Database) []resolvedField {
	if generated == nil {
		return nil
	}

	var resolved []resolvedField
	for _, table := range generated.Tables {
		// Direct fields declared on the table struct itself.
		for _, f := range generated.Fields {
			if f.StructName == table.StructName {
				resolved = append(resolved, resolvedField{Field: f, tableName: table.Name, qualifiedTableName: table.QualifiedName()})
			}
		}
		// Fields contributed by embedded mixins (inline + inline-relation),
		// each already rewritten to the host struct name by the expansion.
		for _, f := range processEmbeddedFieldsForStruct(generated.EmbeddedFields, generated.Fields, table.StructName) {
			resolved = append(resolved, resolvedField{Field: f, tableName: table.Name, qualifiedTableName: table.QualifiedName()})
		}
	}
	return resolved
}
