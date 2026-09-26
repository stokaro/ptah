package schemaprep

import (
	"fmt"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/pgname"
	"ptah.run/internal/tableref"
)

// ColumnCheckNames answers the name of the CHECK each column of table carries
// on dialect, keyed by column name. fields may hold the columns of every table;
// the ones table owns are read in order, and a column without a CHECK is
// absent.
//
// A name the field gives is kept. A CHECK the field leaves unnamed is rendered
// without a name, so the server names it, and the answer is the name the server
// gives, so a comparison looks for the constraint the database holds. On
// PostgreSQL that is [pgname.Check] over the table's columns, numbered past the
// names the table declares and the names of the columns before it: measured on
// PostgreSQL 18.6, `a int CHECK (a IS NULL OR b IS NOT NULL)` on `e` is
// `e_check`, not `e_a_check` (stokaro/ptah#3750). Every other target keeps
// `<table>_<column>_check`.
//
// A name the table declares for a constraint written after the columns is
// claimed although the server has not seen it yet, so the two can disagree
// only where the server refuses the table: it names the column's CHECK first
// and then answers that the declared constraint already exists.
func ColumnCheckNames(
	table schemamodel.Table,
	fields []schemamodel.Field,
	declared []schemamodel.Constraint,
	dialect string,
) map[string]string {
	leaf := tableLeaf(table.Name)
	var own []schemamodel.Field
	var columns []string
	taken := declaredConstraintNames(table, declared)
	for _, field := range fields {
		if field.StructName != table.StructName {
			continue
		}
		own = append(own, field)
		columns = append(columns, field.Name)
		if field.Check != "" && field.CheckName != "" {
			taken[field.CheckName] = struct{}{}
		}
	}
	derivesLikePostgres := platform.NormalizeDialect(dialect) == platform.Postgres
	names := make(map[string]string)
	for _, field := range own {
		switch {
		case field.Check == "":
			continue
		case field.CheckName != "":
			names[field.Name] = field.CheckName
		case derivesLikePostgres:
			name := pgname.Check(leaf, field.Check, columns, func(candidate string) bool {
				_, used := taken[candidate]
				return used
			})
			taken[name] = struct{}{}
			names[field.Name] = name
		default:
			names[field.Name] = leaf + "_" + field.Name + "_check"
		}
	}
	return names
}

// TableCheckConstraints returns the model constraints declared by a table's
// checks list. An explicit CHECK with the same expression supersedes the list
// entry. The names the table declares and the names its columns' CHECKs take on
// dialect -- see [ColumnCheckNames] -- are taken, so an entry is numbered past
// them: the server creates a column's CHECK first, and an entry under the same
// name is refused with `check constraint "e_check" already exists`.
func TableCheckConstraints(
	table schemamodel.Table,
	fields []schemamodel.Field,
	declared []schemamodel.Constraint,
	dialect string,
) []schemamodel.Constraint {
	if len(table.Checks) == 0 {
		return nil
	}
	spelled := declaredCheckExpressions(table, declared)
	taken := declaredConstraintNames(table, declared)
	for _, name := range ColumnCheckNames(table, fields, declared, dialect) {
		taken[name] = struct{}{}
	}
	constraints := make([]schemamodel.Constraint, 0, len(table.Checks))
	ordinal := 0
	for _, check := range table.Checks {
		expression := strings.TrimSpace(check)
		if expression == "" {
			continue
		}
		if _, superseded := spelled[expression]; superseded {
			continue
		}
		name := tableCheckConstraintName(table.Name, ordinal)
		for {
			if _, used := taken[name]; !used {
				break
			}
			ordinal++
			name = tableCheckConstraintName(table.Name, ordinal)
		}
		taken[name] = struct{}{}
		ordinal++
		constraints = append(constraints, schemamodel.Constraint{
			StructName:      table.StructName,
			Name:            name,
			Type:            "CHECK",
			Table:           table.QualifiedName(),
			CheckExpression: expression,
		})
	}
	return constraints
}

func declaredConstraintNames(table schemamodel.Table, declared []schemamodel.Constraint) map[string]struct{} {
	taken := make(map[string]struct{}, len(declared))
	for _, constraint := range declared {
		if !ConstraintBelongsToTable(constraint, table) {
			continue
		}
		if name := strings.TrimSpace(constraint.Name); name != "" {
			taken[name] = struct{}{}
		}
	}
	return taken
}

func declaredCheckExpressions(table schemamodel.Table, declared []schemamodel.Constraint) map[string]struct{} {
	spelled := make(map[string]struct{}, len(declared))
	for _, constraint := range declared {
		if strings.EqualFold(constraint.Type, "CHECK") && ConstraintBelongsToTable(constraint, table) {
			spelled[strings.TrimSpace(constraint.CheckExpression)] = struct{}{}
		}
	}
	return spelled
}

func tableCheckConstraintName(tableName string, ordinal int) string {
	leaf := tableLeaf(tableName)
	if ordinal == 0 {
		return leaf + "_check"
	}
	return fmt.Sprintf("%s_check%d", leaf, ordinal)
}

// tableLeaf is the bare name of a table name that may carry its schema.
func tableLeaf(tableName string) string {
	if ref, ok := tableref.Parse(tableName); ok {
		return ref.Name
	}
	return tableName
}

// ConstraintBelongsToTable reports whether constraint is owned by table under
// either the explicit table identity or the legacy struct identity.
func ConstraintBelongsToTable(constraint schemamodel.Constraint, table schemamodel.Table) bool {
	if constraint.Table != "" {
		return constraint.Table == table.Name || constraint.Table == table.QualifiedName()
	}
	return constraint.StructName == table.StructName
}

// IsForeignKeyConstraint reports whether constraint declares a foreign key.
func IsForeignKeyConstraint(constraint schemamodel.Constraint) bool {
	return strings.EqualFold(constraint.Type, "FOREIGN KEY")
}
