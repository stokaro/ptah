package ydb

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/internal/ydbfamily"
	"ptah.run/migration/schemadiff/difftypes"
)

// How a YDB plan changes a table's column families.
//
// One ALTER TABLE per table adds the families the declaration names and the
// table lacks, sets each setting the declaration states and the table holds
// otherwise, and moves each column whose family differs (see
// [ydbfamily.AlterActions]). It comes after the table's added columns, so a
// column added into a family exists when it moves there, and leaves alone a
// column the plan drops. A new table's families go inside its CREATE TABLE.
//
// A setting or a family the declaration leaves out is the table's to keep,
// since a cluster's table profile gives every new table its own (see package
// ydbfamily), so no family change needs a rebuild. A rebuild made for another
// reason writes the families the table holds once the declaration is applied
// ([ydbfamily.Applied]), which the comparison has made the declaration's, so
// the new table keeps them. It refuses a family that keeps its columns in
// memory, which no CREATE TABLE writes.

// familyOperation is the operation that takes a table's column families from
// what the database holds to what the declaration states, or nil when nothing
// moves. A column the modification drops is left out of both sides: it goes
// with its DROP COLUMN, and moving it first would only rewrite its data.
func familyOperation(tableDiff difftypes.TableDiff) ast.AlterOperation {
	change := tableDiff.YDBColumnFamiliesChange
	if change == nil {
		return nil
	}
	dropped := make([]string, 0, len(tableDiff.ColumnsRemoved))
	for _, column := range tableDiff.ColumnsRemoved {
		dropped = append(dropped, column.Name)
	}
	desired := ydbfamily.WithoutColumns(change.Desired, dropped)
	previous := ydbfamily.WithoutColumns(change.Current, dropped)
	if len(ydbfamily.AlterActions(desired, previous)) == 0 {
		return nil
	}
	return &ast.SetYDBColumnFamiliesOperation{Families: desired, Previous: previous}
}

// refuseFamilyChange refuses, before anything is emitted, a change of a
// table's column families this target cannot make: an action the target has
// no key for, a declaration YDB refuses, read against the table's declared
// columns and key (see [ydbfamily.Refusal]), and a keep_in_memory no
// statement writes (see [ydbfamily.ChangeRefusal]).
func (p *Planner) refuseFamilyChange(tableDiff difftypes.TableDiff) error {
	change := tableDiff.YDBColumnFamiliesChange
	if change == nil {
		return nil
	}
	subject := fmt.Sprintf("table %q", tableDiff.TableName)
	for _, requirement := range ydbfamily.ChangeRequirements(change.Desired, change.Current) {
		if !p.caps.Has(requirement.Key) {
			return refuseKey(requirement.Key, "changing the "+requirement.Settings+" of "+subject)
		}
	}
	if tableDiff.Desired.HasTable() {
		columns, key := declaredColumns(tableDiff.Desired)
		if reason := ydbfamily.Refusal(change.Desired, columns, key); reason != "" {
			return refuseFact(subject, reason)
		}
	}
	if reason := ydbfamily.ChangeRefusal(change.Desired, change.Current); reason != "" {
		return refuseFact(subject, reason)
	}
	return nil
}

// refuseRebuiltFamilies refuses the families a rebuild's CREATE TABLE cannot
// write: a family the target has no key for, a declaration YDB refuses, read
// against the declared columns and key, and a family that keeps its columns
// in memory (see [ydbfamily.CreateRefusal]).
func (p *Planner) refuseRebuiltFamilies(declaration difftypes.TableDeclaration, subject string) error {
	families := declaration.Table.YDBColumnFamilies
	for _, requirement := range ydbfamily.Requirements(families) {
		if !p.caps.Has(requirement.Key) {
			return refuseKey(requirement.Key, subject+" with "+requirement.Settings)
		}
	}
	columns, key := declaredColumns(declaration)
	if reason := ydbfamily.Refusal(families, columns, key); reason != "" {
		return refuseFact(subject, reason)
	}
	if reason := ydbfamily.CreateRefusal(families); reason != "" {
		return refuseFact(subject, reason)
	}
	return nil
}

// declaredColumns are the columns a declared table names and its key: the
// key the table names, or its key fields.
func declaredColumns(declaration difftypes.TableDeclaration) (columns, key []string) {
	for _, field := range declaration.Fields {
		if field.StructName != declaration.Table.StructName {
			continue
		}
		columns = append(columns, field.Name)
		if field.Primary {
			key = append(key, field.Name)
		}
	}
	if len(declaration.Table.PrimaryKey) > 0 {
		key = declaration.Table.PrimaryKey
	}
	return columns, key
}
