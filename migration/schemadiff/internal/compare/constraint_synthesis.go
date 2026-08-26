package compare

import (
	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/internal/convert/fromschema"
	"go.5x5.cz/ptah/migration/internal/generatedschema"
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
// `//ptah:schema:constraint` that happens to share the synthesized
// name wins — synthesis never clobbers it. See the guard in
// Constraints() where genConstraints is populated.
func synthesizeFieldLevelCheckConstraints(
	generated *goschema.Database,
	database *catalog.Database,
	semantics identifier.Semantics,
) []goschema.Constraint {
	if generated == nil || database == nil {
		return nil
	}

	structToTable := make(map[string]goschema.Table, len(generated.Tables))
	for _, t := range generated.Tables {
		structToTable[t.StructName] = t
	}

	dbColumns := make(map[tableMemberKey]struct{}, 16)
	for _, t := range database.Tables {
		for _, c := range t.Columns {
			dbColumns[newTableMemberKey(t.QualifiedName(), c.Name, semantics)] = struct{}{}
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
		if _, exists := dbColumns[newTableMemberKey(tableName, f.Name, semantics)]; !exists {
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
	database *catalog.Database,
	dialect string,
	semantics identifier.Semantics,
) []goschema.Constraint {
	if generated == nil || database == nil {
		return nil
	}

	dbTables := make(map[tableIdentity]struct{}, len(database.Tables))
	for _, table := range database.Tables {
		dbTables[newQualifiedTableIdentity(table.QualifiedName(), semantics)] = struct{}{}
	}

	var synthesized []goschema.Constraint
	for _, table := range generated.Tables {
		columns := tablePrimaryKeyColumns(table)
		if len(columns) == 0 {
			continue
		}
		identity := newQualifiedTableIdentity(table.QualifiedName(), semantics)
		if _, exists := dbTables[identity]; !exists {
			continue
		}
		if livePrimaryKeyIsOnTheColumns(database, identity, columns, semantics) {
			// The key is already there and the read carries it on the columns
			// rather than as a constraint row, so there is nothing to compare a
			// synthesized constraint against and an addition would be planned
			// for a key that exists.
			//
			// Oracle is the engine that does this. Its reader drops a
			// system-named PRIMARY KEY after copying the fact onto the columns
			// (stokaro/ptah#1890), which is right for a key declared on a field
			// and left a table-level `primary_key` block with no counterpart:
			// measured on Oracle Free 23, applying one HCL document twice
			// answered `ORA-02260: table can have only one primary key`
			// (stokaro/ptah#2057).
			continue
		}

		name := tablePrimaryKeyConstraintName(table, database.Constraints, dialect, semantics)
		synthesized = append(synthesized, goschema.Constraint{
			StructName: table.StructName,
			Name:       name,
			Type:       "PRIMARY KEY",
			Table:      table.QualifiedName(),
			Columns:    append([]string(nil), columns...),
			// primaryKeyConstraintChanged compares the payload, so a synthesized
			// constraint without it is unequal to every covering key the catalog
			// reports -- and the difference is a DROP and an ADD that removes the
			// payload from the live index, after which the schema reads as synced
			// (stokaro/ptah#2199).
			IncludeColumns: append([]string(nil), table.PrimaryKeyInclude...),
		})
	}
	return synthesized
}

// livePrimaryKeyIsOnTheColumns reports that the live table already has exactly
// this primary key, recorded on its columns rather than as a constraint row.
//
// Both halves are required. A table whose read carries a PRIMARY KEY row is
// compared through that row, as every engine but Oracle does. A key on the
// columns that does not MATCH the declared one is a real change and has to be
// planned, so the column set is compared and not merely counted.
func livePrimaryKeyIsOnTheColumns(
	database *catalog.Database,
	identity tableIdentity,
	declared []string,
	semantics identifier.Semantics,
) bool {
	for _, constraint := range database.Constraints {
		if constraint.Type != "PRIMARY KEY" {
			continue
		}
		if newQualifiedTableIdentity(constraint.QualifiedTableName(), semantics) == identity {
			return false
		}
	}
	live := livePrimaryKeyColumns(database, identity, semantics)
	if len(live) == 0 {
		return false
	}
	return columnSetsMatch(declared, live, semantics)
}

// livePrimaryKeyColumns names the columns the read marked as this table's
// primary key.
func livePrimaryKeyColumns(
	database *catalog.Database,
	identity tableIdentity,
	semantics identifier.Semantics,
) []string {
	for _, table := range database.Tables {
		if newQualifiedTableIdentity(table.QualifiedName(), semantics) != identity {
			continue
		}
		columns := make([]string, 0, len(table.Columns))
		for _, column := range table.Columns {
			if column.IsPrimaryKey {
				columns = append(columns, column.Name)
			}
		}
		return columns
	}
	return nil
}

// columnSetsMatch compares two column lists under the target's identifier
// rules, which is what makes an upper-cased catalog and a lower-cased
// declaration one key.
func columnSetsMatch(declared, live []string, semantics identifier.Semantics) bool {
	if len(declared) != len(live) {
		return false
	}
	folded := make(map[string]int, len(live))
	for _, column := range live {
		folded[semantics.ColumnIdentityKey(column)]++
	}
	for _, column := range declared {
		key := semantics.ColumnIdentityKey(column)
		if folded[key] == 0 {
			return false
		}
		folded[key]--
	}
	return true
}

// tablePrimaryKeyConstraintName adopts the name the database already uses for
// this table's primary key, so the synthesized constraint compares equal to it
// instead of being reported as a rename.
//
// The lookup normalizes both sides for the same reason the keys around it do:
// the database reports the schema as empty wherever the engine treats it as
// implicit, and the desired side carries it. Comparing the two spellings
// directly meant the lookup never matched and the name always came from the
// fallback below. That was invisible wherever the fallback happens to be right
// -- MySQL always names it PRIMARY, PostgreSQL usually names it <table>_pkey --
// and wrong the moment a schema names its primary key itself, which surfaced as
// dropping the real constraint and adding a differently named one
// (stokaro/ptah#1244).
func tablePrimaryKeyConstraintName(
	table goschema.Table,
	dbConstraints []catalog.Constraint,
	dialect string,
	semantics identifier.Semantics,
) string {
	wanted := newQualifiedTableIdentity(table.QualifiedName(), semantics)
	for _, constraint := range dbConstraints {
		if constraint.Type != "PRIMARY KEY" {
			continue
		}
		if newQualifiedTableIdentity(constraint.QualifiedTableName(), semantics) == wanted {
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
// `//ptah:schema:constraint` that happens to share the synthesized name
// wins — synthesis never clobbers it (see the guard in Constraints()).
func synthesizeFieldLevelForeignKeyConstraints(
	generated *goschema.Database,
	database *catalog.Database,
	semantics identifier.Semantics,
) []goschema.Constraint {
	if generated == nil || database == nil {
		return nil
	}

	dbColumns := make(map[tableMemberKey]struct{}, 16)
	for _, t := range database.Tables {
		for _, c := range t.Columns {
			dbColumns[newTableMemberKey(t.QualifiedName(), c.Name, semantics)] = struct{}{}
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
	dedupe := make(map[tableMemberKey]struct{})
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
		if _, exists := dbColumns[newTableMemberKey(tableName, f.Name, semantics)]; !exists {
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
		dedupeKey := newTableMemberKey(tableName, name, semantics)
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
			// foreignKeyConstraintChanged compares the deferral, so a
			// synthesized constraint without it is unequal to every deferrable
			// key the catalog reports -- and a single-column foreign key is
			// ALWAYS synthesized, because a declaration carries it on the field
			// rather than as a constraint. It went unnoticed only while no
			// reader reported the property (stokaro/ptah#2202).
			Deferrable: f.Deferrable,
			Initially:  f.Initially,
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

// resolveTableFields uses the shared generated-schema expansion and tags each
// resulting field with its concrete host table name. This keeps field-level
// synthesis aligned with CREATE and column-diff paths, so constraints target
// host tables rather than mixin struct names (issue #197).
//
// Only fields whose owning struct is a declared table are returned: a
// `foreign=` annotation on a mixin that is never embedded, or on a struct that
// is not a //ptah:schema:table, has no concrete table and must not be
// synthesized.
func resolveTableFields(generated *goschema.Database) []resolvedField {
	if generated == nil {
		return nil
	}

	var resolved []resolvedField
	for _, table := range generated.Tables {
		for _, f := range generatedschema.FieldsForTable(generated, table) {
			resolved = append(resolved, resolvedField{Field: f, tableName: table.Name, qualifiedTableName: table.QualifiedName()})
		}
	}
	return resolved
}
