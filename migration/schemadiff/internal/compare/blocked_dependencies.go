package compare

import (
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/tableref"
)

// keptTable is a table the comparison leaves as the database holds it: one
// whose drop the selection blocks, or one such a table needs.
type keptTable struct {
	table catalog.Table
	// name is the table as the plan would name it in its DROP.
	name string
	// blocker is the blocker of the drop that keeps the table.
	blocker catalog.SelectionBlocker
}

// keptNeeds is what the tables a comparison keeps need from the database, and
// the comparison must not drop either.
type keptNeeds struct {
	tables    map[tableIdentity]struct{}
	functions map[string]struct{}
	types     map[string]struct{}
	removals  []difftypes.BlockedRemoval
	// databases holds each database that holds a table in tables.
	databases HeldDatabases
}

// HeldDatabases holds, by the name the read gives it, each database that
// holds a table a comparison keeps because a table whose drop the selection
// blocks needs it, with the [difftypes.BlockedRemoval] a whole-server
// comparison records in place of its drop: dropping the database drops the
// table, and MySQL refuses `DROP DATABASE` with Error 3730 while a kept table
// in another database references a table in it. The database of a table
// whose drop the selection blocks is not among them: the selection blocks its
// drop as well ([catalog.Database.SchemaRemovalBlockers]). A nil map holds
// none.
type HeldDatabases map[string]difftypes.BlockedRemoval

// hold records the database of table, kept because needer needs it, unless it
// is recorded already or the table names no database.
func (h HeldDatabases) hold(table keptTable, needer string) {
	if table.table.Schema == "" {
		return
	}
	if _, held := h[table.table.Schema]; held {
		return
	}
	h[table.table.Schema] = difftypes.BlockedRemoval{
		Kind: difftypes.BlockedDatabase, Name: table.table.Schema, BlockedBy: table.blocker, NeededBy: needer,
	}
}

// neededByKept returns what the tables in kept need from database that
// desired does not declare, so the comparison would drop it: each table a
// foreign key of theirs references, in scope or out of it, each function a
// trigger of theirs executes, and each enum type a column of theirs has. A
// table it adds is kept too, so what that table needs is added in turn.
//
// A kept table stays whole, so the drop of an object it needs fails: MySQL
// refuses `DROP TABLE t` with Error 3730 while a foreign key of the kept
// table references t, and PostgreSQL refuses it, and `DROP FUNCTION` of a
// trigger function, while the kept table's key and trigger depend on them
// (stokaro/ptah#6847). Each object returned is recorded as a drop the
// selection keeps, naming the table that needs it, and so is each database
// that holds a table kept this way.
//
// A function or a type is held only when desired declares nothing with its
// name, in any schema and with any arguments: one the comparison could pair
// with a declaration must stay in the comparison, or the plan would create it
// again. A table coverage would not drop is left alone, as
// [WithoutBlockedTables] leaves one.
func neededByKept(
	desired *schemamodel.Database,
	database *catalog.Database,
	kept []keptTable,
	declared map[tableIdentity]struct{},
	dialect string,
	semantics identifier.Semantics,
	cov Coverage,
) keptNeeds {
	needs := keptNeeds{
		tables: map[tableIdentity]struct{}{}, functions: map[string]struct{}{}, types: map[string]struct{}{}, databases: HeldDatabases{},
	}
	held := make(map[tableIdentity]struct{}, len(kept))
	for _, table := range kept {
		held[tableMapIdentity(table.table.Schema, table.table.Name, dialect, semantics)] = struct{}{}
	}
	tables := make(map[string]catalog.Table, len(database.Tables))
	for _, table := range database.Tables {
		tables[semantics.QualifiedTableIdentityKey(catalog.QualifyTableName(table.Schema, table.Name))] = table
	}
	declaredFunctions := declaredBareNames(semantics, schemamodelFunctionNames(desired)...)
	declaredTypes := declaredBareNames(semantics, schemamodelEnumNames(desired)...)
	for queue := kept; len(queue) > 0; {
		owner := queue[0]
		queue = queue[1:]
		in := func(schema, table string) bool {
			return tableMapIdentity(schema, table, dialect, semantics) == tableMapIdentity(owner.table.Schema, owner.table.Name, dialect, semantics)
		}
		for _, constraint := range append(append([]catalog.Constraint{}, database.Constraints...), database.OutOfScope.Constraints...) {
			if !strings.EqualFold(constraint.Type, "FOREIGN KEY") || !in(constraint.Schema, constraint.TableName) {
				continue
			}
			referenced, ok := tables[semantics.QualifiedTableIdentityKey(constraint.QualifiedForeignTableName())]
			if !ok {
				continue
			}
			identity := tableMapIdentity(referenced.Schema, referenced.Name, dialect, semantics)
			_, isHeld := held[identity]
			_, isDeclared := declared[identity]
			removal := difftypes.TableRemoval{Name: removedTableName(referenced.Schema, referenced.Name, dialect)}
			if isHeld || isDeclared || len(keepPlannedRemovals(cov, coverage.Schema, []difftypes.TableRemoval{removal}, tableRemovalSchemaOnly)) == 0 {
				continue
			}
			held[identity] = struct{}{}
			needs.tables[identity] = struct{}{}
			needs.removals = append(needs.removals, difftypes.BlockedRemoval{
				Kind: difftypes.BlockedTable, Name: removal.Name, BlockedBy: owner.blocker, NeededBy: owner.name,
			})
			needed := keptTable{table: referenced, name: removal.Name, blocker: owner.blocker}
			needs.databases.hold(needed, owner.name)
			queue = append(queue, needed)
		}
		for _, trigger := range append(append([]catalog.Trigger{}, database.Triggers...), database.OutOfScope.Triggers...) {
			if trigger.ExecuteFunction == "" || !in(trigger.Schema, trigger.Table) {
				continue
			}
			executed := semantics.QualifiedTableIdentityKey(trigger.ExecuteFunction)
			for _, function := range database.Functions {
				key := semantics.QualifiedTableIdentityKey(catalog.QualifyTableName(function.Schema, function.Name))
				if !takesNoInput(function) || key != executed || declaredFunctions[semantics.TableIdentityKey(function.Name)] {
					continue
				}
				if _, done := needs.functions[key]; done {
					continue
				}
				needs.functions[key] = struct{}{}
				needs.removals = append(needs.removals, difftypes.BlockedRemoval{
					Kind: difftypes.BlockedFunction, Name: catalog.QualifyTableName(function.Schema, function.Name),
					BlockedBy: owner.blocker, NeededBy: owner.name,
				})
			}
		}
		for _, column := range owner.table.Columns {
			typed := columnEnumName(column)
			if typed == "" {
				continue
			}
			for _, enum := range database.Enums {
				schema, name := enumParts(enum.Schema, enum.Name)
				key := semantics.QualifiedTableIdentityKey(catalog.QualifyTableName(schema, name))
				if key != semantics.QualifiedTableIdentityKey(typed) || declaredTypes[semantics.TableIdentityKey(name)] {
					continue
				}
				if _, done := needs.types[key]; done {
					continue
				}
				needs.types[key] = struct{}{}
				needs.removals = append(needs.removals, difftypes.BlockedRemoval{
					Kind: difftypes.BlockedType, Name: catalog.QualifyTableName(schema, name),
					BlockedBy: owner.blocker, NeededBy: owner.name,
				})
			}
		}
	}
	return needs
}

// takesNoInput reports whether function may be the overload a trigger
// executes: PostgreSQL runs a trigger function with no input arguments. A
// function whose identity the reader did not capture may be that one.
func takesNoInput(function catalog.Function) bool {
	return function.IdentityArguments == nil || *function.IdentityArguments == ""
}

// columnEnumName is the type a column has as the catalog names it, qualified
// with its schema, for a column whose type is a user-defined type, and the
// element type of an array of one. It is empty for a column of a built-in
// type the read does not name that way.
func columnEnumName(column catalog.Column) string {
	name := column.UDTName
	if name == "" {
		return ""
	}
	if strings.EqualFold(column.DataType, "ARRAY") {
		name = strings.TrimPrefix(name, "_")
	}
	return catalog.QualifyTableName(column.UDTSchema, name)
}

// declaredBareNames is the set of names, without their schema, under the
// table rule of semantics.
func declaredBareNames(semantics identifier.Semantics, names ...string) map[string]bool {
	set := make(map[string]bool, len(names))
	for _, name := range names {
		if ref, ok := tableref.Parse(name); ok {
			name = ref.Name
		}
		set[semantics.TableIdentityKey(name)] = true
	}
	return set
}

// schemamodelFunctionNames are the names of the functions desired declares.
func schemamodelFunctionNames(desired *schemamodel.Database) []string {
	if desired == nil {
		return nil
	}
	names := make([]string, 0, len(desired.Functions))
	for _, function := range desired.Functions {
		names = append(names, function.Name)
	}
	return names
}

// schemamodelEnumNames are the names of the enum types desired declares.
func schemamodelEnumNames(desired *schemamodel.Database) []string {
	if desired == nil {
		return nil
	}
	names := make([]string, 0, len(desired.Enums))
	for _, enum := range desired.Enums {
		names = append(names, enum.Name)
	}
	return names
}

// withoutNeeds is a copy of database without the functions and enum types
// needs holds; [withoutTables] takes the tables.
func withoutNeeds(database *catalog.Database, needs keptNeeds, semantics identifier.Semantics) *catalog.Database {
	if len(needs.functions) == 0 && len(needs.types) == 0 {
		return database
	}
	out := *database
	out.Functions = without(database.Functions, func(function catalog.Function) bool {
		_, needed := needs.functions[semantics.QualifiedTableIdentityKey(catalog.QualifyTableName(function.Schema, function.Name))]
		return needed && takesNoInput(function)
	})
	out.Enums = without(database.Enums, func(enum catalog.Enum) bool {
		schema, name := enumParts(enum.Schema, enum.Name)
		_, needed := needs.types[semantics.QualifiedTableIdentityKey(catalog.QualifyTableName(schema, name))]
		return needed
	})
	return &out
}
