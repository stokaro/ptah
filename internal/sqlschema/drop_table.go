package sqlschema

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/deporder"
)

// applyDropTable removes each table a DROP TABLE names, with what the server
// removes along with it: its columns, indexes, constraints and triggers, its
// row security, its grants, comments and declared rows, and the sequences its
// columns own.
//
// A schema file is a script the server runs in order, so a table created and
// then dropped is not in the schema, and a comparison plans it away. Measured
// on MySQL 8.4.11 and PostgreSQL 18.6, `CREATE TABLE a ...; DROP TABLE a;`
// leaves no table a, and Atlas CE v1.3.0 reports the file synced with the
// database it builds (stokaro/ptah#3876).
//
// What the servers refuse is refused, before anything is removed:
//
//   - a table no file declares, unless the statement says IF EXISTS;
//   - a table another table's foreign key still refers to, unless the same
//     statement drops that table too: `ERROR 3730` on MySQL, `other objects
//     depend on it` on PostgreSQL;
//   - on the PostgreSQL family, a table a view or a materialized view reads.
//     MySQL and MariaDB keep such a view, invalid, and so does the model.
//
// CASCADE is refused as it is on DROP COLUMN: it drops objects the file does
// not list.
func applyDropTable(database, base *schemamodel.Database, document *Document, node *ast.DropTableNode, sourcePlatform string) error {
	names := node.Names
	if len(names) == 0 {
		names = []string{node.Name}
	}
	if node.Cascade {
		return fmt.Errorf(
			"DROP TABLE %s CASCADE also drops the objects that depend on the table, "+
				"which a schema file does not list; drop them by name",
			strings.Join(names, ", "))
	}
	var targets []alterTarget
	for _, name := range names {
		target, declared := findAlterTarget(database, base, name, sourcePlatform)
		if !declared {
			if node.IfExists {
				continue
			}
			return fmt.Errorf("DROP TABLE %s names a table this schema does not declare", name)
		}
		if !slices.ContainsFunc(targets, func(other alterTarget) bool { return other.structName == target.structName }) {
			targets = append(targets, target)
		}
	}
	dropped := func(structName string) bool {
		return slices.ContainsFunc(targets, func(target alterTarget) bool { return target.structName == structName })
	}
	for _, target := range targets {
		if reference := tableReference(target, dropped); reference != "" {
			return fmt.Errorf("DROP TABLE %s: %s still refers to the table", target.written, reference)
		}
	}
	for _, target := range targets {
		target.keys = &document.keys
		removeTable(target)
	}
	return nil
}

// tableReference names the first object outside the dropped tables that keeps
// the server from dropping the table, or returns "".
func tableReference(target alterTarget, dropped func(structName string) bool) string {
	for _, database := range target.databases {
		for _, constraint := range database.Constraints {
			if isForeignKey(constraint) && !dropped(constraint.StructName) && target.reachesTable(constraint.ForeignTable) {
				return fmt.Sprintf("the foreign key %s of %s", constraint.Name, constraint.Table)
			}
		}
		for _, field := range database.Fields {
			if field.Foreign == "" || dropped(field.StructName) {
				continue
			}
			if open := strings.Index(field.Foreign, "("); open > 0 && target.reachesTable(field.Foreign[:open]) {
				return fmt.Sprintf("the foreign key on column %s", field.Name)
			}
		}
	}
	if !platform.IsPostgresFamily(target.sourcePlatform) {
		return ""
	}
	for _, database := range target.databases {
		for _, view := range database.Views {
			if deporder.ReferencesIdentifier(view.Body, target.table.Name) {
				return fmt.Sprintf("view %s", view.Name)
			}
		}
		for _, view := range database.MaterializedViews {
			if deporder.ReferencesIdentifier(view.Body, target.table.Name) {
				return fmt.Sprintf("materialized view %s", view.Name)
			}
		}
	}
	return ""
}

// removeTable takes the table and everything that belongs to it out of every
// database of the target. The table goes last: the others are matched by
// resolving the table name they carry, which needs the table.
func removeTable(target alterTarget) {
	owns := func(structName, table string) bool {
		return structName == target.structName || target.reachesTable(table)
	}
	for _, database := range target.databases {
		database.Fields = slices.DeleteFunc(database.Fields, func(field schemamodel.Field) bool {
			return field.StructName == target.structName
		})
		database.EmbeddedFields = slices.DeleteFunc(database.EmbeddedFields, func(field schemamodel.EmbeddedField) bool {
			return field.StructName == target.structName
		})
		database.Indexes = slices.DeleteFunc(database.Indexes, target.ownsIndex)
		database.Constraints = slices.DeleteFunc(database.Constraints, func(constraint schemamodel.Constraint) bool {
			return owns(constraint.StructName, constraint.Table)
		})
		database.Triggers = slices.DeleteFunc(database.Triggers, func(trigger schemamodel.Trigger) bool {
			return owns(trigger.StructName, trigger.Table)
		})
		database.RLSPolicies = slices.DeleteFunc(database.RLSPolicies, func(policy schemamodel.RLSPolicy) bool {
			return owns(policy.StructName, policy.Table)
		})
		database.RLSEnabledTables = slices.DeleteFunc(database.RLSEnabledTables, func(table schemamodel.RLSEnabledTable) bool {
			return owns(table.StructName, table.Table)
		})
		database.Grants = slices.DeleteFunc(database.Grants, func(grant schemamodel.Grant) bool {
			return grant.OnTable != "" && target.reachesTable(grant.OnTable)
		})
		database.ExtendedProperties = slices.DeleteFunc(database.ExtendedProperties, func(property schemamodel.ExtendedProperty) bool {
			return property.Table != "" && target.reachesTable(schemamodel.QualifyTableName(property.Schema, property.Table))
		})
		database.Hypertables = slices.DeleteFunc(database.Hypertables, func(hypertable schemamodel.Hypertable) bool {
			return owns(hypertable.StructName, hypertable.Table)
		})
		database.ManagedData = slices.DeleteFunc(database.ManagedData, func(data schemamodel.ManagedData) bool {
			return target.reachesTable(schemamodel.QualifyTableName(data.Schema, data.Table))
		})
		database.Sequences = slices.DeleteFunc(database.Sequences, func(sequence schemamodel.Sequence) bool {
			owner := strings.TrimSpace(sequence.OwnedBy)
			dot := strings.LastIndex(owner, ".")
			return dot > 0 && target.reachesTable(owner[:dot])
		})
	}
	target.keys.forgetTable(target.qualified)
	for _, database := range target.databases {
		database.Tables = slices.DeleteFunc(database.Tables, func(table schemamodel.Table) bool {
			return table.StructName == target.structName
		})
	}
}
