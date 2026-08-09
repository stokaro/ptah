package generator

import (
	"strings"

	"go.5x5.cz/ptah/core/goschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/convert/dbschematogo"
	"go.5x5.cz/ptah/internal/convert/fromschema"
	"go.5x5.cz/ptah/internal/deporder"
	"go.5x5.cz/ptah/internal/planner/tablelookup"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

// dropReverseConstraintsRestoredByTableCreation removes, from a reverse plan,
// the constraint additions that the plan's own CREATE TABLE already restores.
//
// # Why the reverse needs this and the forward direction does not
//
// The comparator applies one rule going forward: a table it is CREATING carries
// its own constraints, so it never also lists them as constraint additions —
// "new tables/columns get their FK inline via CREATE TABLE / ALTER TABLE ADD
// CONSTRAINT, so emitting an ADD CONSTRAINT here would double-create it in the
// same migration step" (migration/schemadiff/internal/compare/constraints.go).
// A table it is DROPPING gets no such treatment: the constraints go into
// ConstraintsRemoved alongside the table, which is harmless going forward
// because every drop is IF EXISTS and the DROP TABLE follows.
//
// Reversing that diff swaps both lists at once, and the asymmetry turns into a
// rollback that says the same thing twice: the re-created table is rendered
// from the pre-change schema WITH its primary key inline and its foreign keys
// re-added by the planner's new-table pass, and then the swapped constraint
// lists ADD both a second time. PostgreSQL 17.10 answers the second primary key
// with `multiple primary keys for table "gadgets" are not allowed` (SQLSTATE
// 42P16) and the second foreign key with `constraint … already exists` (42710),
// so the rollback half aborts part-applied — measured on both writers of down
// files, `ptah migrations generate` and the Atlas-compatible `migrate diff`
// (stokaro/ptah#1013).
//
// # What is dropped, and what deliberately is not
//
// Only what the re-created table demonstrably brings back on its own:
//
//   - PRIMARY KEY, when the restored table body carries one — single-column via
//     the field, composite via [goschema.Table.PrimaryKey]. Both render inline,
//     and neither renders the constraint's NAME, so the separate ALTER carried
//     no information the CREATE TABLE loses.
//   - FOREIGN KEY, when the restored table has a field-level reference the
//     planner's new-table pass re-adds under the same constraint name. A
//     SELF-referencing reference is excluded because that pass skips it, and a
//     multi-column foreign key is excluded because the introspected schema
//     cannot express it field-level; in both cases the ALTER is the only
//     emission there is.
//
// CHECK and UNIQUE additions are left alone. The CREATE TABLE restores neither
// by name — a CHECK is not rendered at all, and a UNIQUE comes back as the
// column's anonymous inline uniqueness — so dropping them here would trade a
// rollback that fails loudly for one that silently restores less than it
// should. Those two remain as they were before this change.
//
// Nothing is dropped when there is no introspected pre-change schema to read
// the restored body from: the caller then has no table bodies to compare
// against and the conservative answer is the behavior that was there before.
func dropReverseConstraintsRestoredByTableCreation(
	reversed *types.SchemaDiff,
	removedWithTables []types.ConstraintRemovalInfo,
	dbSchema *dbschematypes.DBSchema,
) {
	if reversed == nil || dbSchema == nil || len(reversed.TablesAdded) == 0 {
		return
	}
	restored := tableCreationRestores(dbschematogo.ConvertDBSchemaToGoSchema(dbSchema), reversed.TablesAdded)
	if len(restored) == 0 {
		return
	}

	keptNames := make(map[string]struct{}, len(reversed.ConstraintsAddedWithTables))
	keptAdditions := make([]types.ConstraintAdditionInfo, 0, len(reversed.ConstraintsAddedWithTables))
	for _, addition := range reversed.ConstraintsAddedWithTables {
		if restored.covers(addition.TableName, addition.Name, addition.Type) {
			continue
		}
		keptAdditions = append(keptAdditions, addition)
		keptNames[addition.Name] = struct{}{}
	}
	if len(keptAdditions) != len(reversed.ConstraintsAddedWithTables) {
		reversed.ConstraintsAddedWithTables = keptAdditions
	}

	// The bare name list has to lose the same entries. Left behind, a name whose
	// table-qualified addition was dropped falls through to the planners'
	// name-only add path and re-emits exactly the statement this function set
	// out to remove.
	//
	// A name is only dropped when EVERY host the comparator recorded for it is
	// one of the re-created tables that restores it. A constraint name shared
	// across host tables — the mixin case ConstraintsAddedWithTables exists for
	// — keeps its name whenever any host still needs it.
	hostsByName := make(map[string][]types.ConstraintRemovalInfo, len(removedWithTables))
	for _, removal := range removedWithTables {
		hostsByName[removal.Name] = append(hostsByName[removal.Name], removal)
	}
	keptBareNames := make([]string, 0, len(reversed.ConstraintsAdded))
	for _, name := range reversed.ConstraintsAdded {
		if restored.coversEveryHost(name, keptNames, hostsByName[name]) {
			continue
		}
		keptBareNames = append(keptBareNames, name)
	}
	if len(keptBareNames) != len(reversed.ConstraintsAdded) {
		// Freshly allocated on purpose: ConstraintsAdded is the caller's
		// ConstraintsRemoved slice, and reverseSchemaDiffWithSchema promises to
		// leave the forward diff untouched.
		reversed.ConstraintsAdded = keptBareNames
	}
}

// recreatedTableRestores answers, per table the reverse re-creates, which of
// its constraints the CREATE TABLE step puts back without help.
type recreatedTableRestores []recreatedTableRestore

type recreatedTableRestore struct {
	table goschema.Table
	// hasPrimaryKey is true when the rendered table body declares a primary
	// key, in either spelling the renderer accepts.
	hasPrimaryKey bool
	// foreignKeyNames holds the constraint names the planner's new-table
	// foreign-key pass emits for this table.
	foreignKeyNames map[string]struct{}
}

// tableCreationRestores resolves the re-created tables exactly as the planners
// do — through [deporder.TablesForCreate], the same call
// `addNewTables`/`addForeignKeyConstraintsForNewTables` make — so this
// function's idea of "this plan creates that table" cannot drift from theirs.
func tableCreationRestores(prior *goschema.Database, tablesAdded []string) recreatedTableRestores {
	created := deporder.TablesForCreate(prior, tablesAdded)
	if len(created) == 0 {
		return nil
	}
	restores := make(recreatedTableRestores, 0, len(created))
	for _, table := range created {
		restore := recreatedTableRestore{
			table:           table,
			hasPrimaryKey:   len(table.PrimaryKey) > 0,
			foreignKeyNames: make(map[string]struct{}),
		}
		for _, field := range prior.Fields {
			if field.StructName != table.StructName {
				continue
			}
			if field.Primary {
				restore.hasPrimaryKey = true
			}
			if name, ok := recreatedForeignKeyName(prior, table, field); ok {
				restore.foreignKeyNames[name] = struct{}{}
			}
		}
		restores = append(restores, restore)
	}
	return restores
}

// recreatedForeignKeyName mirrors the planners' new-table foreign-key pass:
// which field-level references it emits, and under which constraint name.
//
// A reference that resolves back to its own table is not emitted there — the
// planners route self-references through a separate list the introspected
// schema does not populate — so it is reported as not restored, and its ALTER
// stays.
func recreatedForeignKeyName(prior *goschema.Database, table goschema.Table, field goschema.Field) (string, bool) {
	if strings.TrimSpace(field.Foreign) == "" {
		return "", false
	}
	ref := fromschema.ParseForeignKeyReference(field.Foreign)
	if ref == nil {
		return "", false
	}
	if tablelookup.ResolveReference(prior.Tables, table, ref.Table) == table.QualifiedName() {
		return "", false
	}
	if field.ForeignKeyName != "" {
		return field.ForeignKeyName, true
	}
	return fromschema.GenerateForeignKeyName(table.Name, field.Name), true
}

// covers reports whether the named constraint on tableName is put back by that
// table's own CREATE TABLE.
func (r recreatedTableRestores) covers(tableName, constraintName, constraintType string) bool {
	restore, ok := r.lookup(tableName)
	if !ok {
		return false
	}
	switch strings.ToUpper(strings.TrimSpace(constraintType)) {
	case "PRIMARY KEY":
		return restore.hasPrimaryKey
	case "FOREIGN KEY":
		_, restored := restore.foreignKeyNames[constraintName]
		return restored
	default:
		return false
	}
}

// coversEveryHost reports whether a bare constraint name is fully accounted for
// by table creation: no table-qualified addition of that name survived, at
// least one host was recorded for it, and every recorded host restores it.
func (r recreatedTableRestores) coversEveryHost(
	name string,
	keptNames map[string]struct{},
	hosts []types.ConstraintRemovalInfo,
) bool {
	if _, kept := keptNames[name]; kept {
		return false
	}
	if len(hosts) == 0 {
		return false
	}
	for _, host := range hosts {
		if !r.covers(host.TableName, name, host.Type) {
			return false
		}
	}
	return true
}

// lookup matches a comparator-recorded table name against the re-created
// tables. The comparator qualifies its host names and a hand-built diff may
// not, so the qualified spelling is tried first and the bare one only when it
// picks out a single table; an ambiguous bare name resolves to nothing, which
// leaves the constraint addition in place.
func (r recreatedTableRestores) lookup(tableName string) (recreatedTableRestore, bool) {
	tableName = strings.TrimSpace(tableName)
	if tableName == "" {
		return recreatedTableRestore{}, false
	}
	for _, restore := range r {
		if restore.table.QualifiedName() == tableName {
			return restore, true
		}
	}
	var match recreatedTableRestore
	found := false
	for _, restore := range r {
		if restore.table.Name != tableName {
			continue
		}
		if found {
			return recreatedTableRestore{}, false
		}
		match = restore
		found = true
	}
	return match, found
}
