package dbmlrender

import (
	"fmt"
	"sort"
	"strings"

	"ptah.run/core/schemamodel"
)

// Constraint types as the schema model spells them.
const (
	constraintPrimaryKey = "PRIMARY KEY"
	constraintForeignKey = "FOREIGN KEY"
	constraintUnique     = "UNIQUE"
	constraintCheck      = "CHECK"
	constraintExclude    = "EXCLUDE"
)

// primaryKey is how one table's primary key is written.
//
// A single key column with nothing else to say keeps the column's own `pk`.
// Every other key -- composite, or named -- goes in the table's Indexes block
// as `(a, b) [pk]`, which is DBML's only spelling for a composite key. The two
// are never both written: a table with a key in Indexes carries no column `pk`,
// or it would declare two.
type primaryKey struct {
	columns  []string
	name     string
	onColumn bool
}

func primaryKeyOf(table schemamodel.Table, fields []schemamodel.Field) primaryKey {
	if len(table.PrimaryKey) > 0 {
		return primaryKey{columns: table.PrimaryKey, name: table.PrimaryKeyName}
	}
	columns := make([]string, 0, 2)
	for _, field := range fields {
		if field.Primary {
			columns = append(columns, field.Name)
		}
	}
	if len(columns) == 1 && table.PrimaryKeyName == "" {
		return primaryKey{onColumn: true}
	}
	return primaryKey{columns: columns, name: table.PrimaryKeyName}
}

// line renders the key as an Indexes entry, or nothing when the column
// carries it or the table has none.
func (k primaryKey) line() (string, bool) {
	if k.onColumn || len(k.columns) == 0 {
		return "", false
	}
	settings := []string{"pk"}
	if k.name != "" {
		settings = append(settings, "name: "+quote(k.name))
	}
	return columnList(k.columns) + " [" + strings.Join(settings, ", ") + "]", true
}

// constraintsOf is the table's constraints of one type.
//
// A constraint names its table by struct name, or, when it has none, by the
// table's name as written.
func (b *builder) constraintsOf(table schemamodel.Table, kind string) []schemamodel.Constraint {
	matched := make([]schemamodel.Constraint, 0, 2)
	for _, constraint := range b.db.Constraints {
		if !strings.EqualFold(strings.TrimSpace(constraint.Type), kind) || !ownedBy(constraint, table) {
			continue
		}
		matched = append(matched, constraint)
	}
	return matched
}

func ownedBy(constraint schemamodel.Constraint, table schemamodel.Table) bool {
	if constraint.StructName != "" {
		return constraint.StructName == table.StructName
	}
	name := strings.TrimSpace(constraint.Table)
	return name != "" && (name == table.QualifiedName() || name == table.Name)
}

// uniqueLine renders a UNIQUE constraint as a unique Indexes entry, which is
// how DBML writes a rule over more than one column.
func uniqueLine(constraint schemamodel.Constraint) string {
	settings := []string{"unique"}
	if constraint.Name != "" {
		settings = append(settings, "name: "+quote(constraint.Name))
	}
	return columnList(constraint.Columns) + " [" + strings.Join(settings, ", ") + "]"
}

// checksOf renders the table's check constraints as Checks entries, sorted.
//
// A named check is a CHECK constraint; an unnamed one is one of the table's
// own checks. An expression DBML cannot write is left out here and counted by
// [builder.omittedKeys].
func (b *builder) checksOf(table schemamodel.Table) []string {
	lines := make([]string, 0, 2)
	for _, expression := range table.Checks {
		if expressible(expression) {
			lines = append(lines, "`"+expression+"`")
		}
	}
	for _, constraint := range b.constraintsOf(table, constraintCheck) {
		if !expressible(constraint.CheckExpression) {
			continue
		}
		line := "`" + constraint.CheckExpression + "`"
		if constraint.Name != "" {
			line += " [name: " + quote(constraint.Name) + "]"
		}
		lines = append(lines, line)
	}
	sort.Strings(lines)
	return lines
}

// expressible reports whether DBML can write the expression between
// backticks: DBML has no escape for a backtick, and an expression cannot span
// lines.
func expressible(expression string) bool {
	return !strings.ContainsAny(expression, "`\n\r")
}

// columnList renders `("a", "b")`.
func columnList(columns []string) string {
	quoted := make([]string, 0, len(columns))
	for _, column := range columns {
		quoted = append(quoted, quote(column))
	}
	return "(" + strings.Join(quoted, ", ") + ")"
}

// omittedKeys names what the rendered tables' keys and constraints hold and
// DBML cannot write, each as "what (count)".
//
// These are losses inside objects the export does write: a foreign key comes
// out as a Ref, and its DEFERRABLE does not. Naming them is what keeps the
// export from reading as complete when it is not (stokaro/ptah#3917).
func (b *builder) omittedKeys() []string {
	counts := make(map[string]int)
	for _, table := range b.selected() {
		fields := b.fieldsOf(table)
		countTableKeys(counts, table)
		for _, field := range fields {
			countColumnKey(counts, field)
		}
		for _, constraint := range b.db.Constraints {
			if ownedBy(constraint, table) {
				countConstraint(counts, constraint)
			}
		}
	}
	omitted := make([]string, 0, len(counts))
	for what, count := range counts {
		omitted = append(omitted, fmt.Sprintf("%s (%d)", what, count))
	}
	return omitted
}

func countTableKeys(counts map[string]int, table schemamodel.Table) {
	if table.PrimaryKeyDeferrable {
		counts["DEFERRABLE on keys"]++
	}
	if len(table.PrimaryKeyInclude) > 0 {
		counts["INCLUDE columns on keys"]++
	}
	for _, part := range table.PrimaryKeyParts {
		if strings.TrimSpace(part.Prefix) != "" || part.Desc {
			counts["prefix lengths and DESC on primary key columns"]++
			break
		}
	}
	for _, expression := range table.Checks {
		if !expressible(expression) {
			counts["CHECK expressions DBML cannot quote"]++
		}
	}
}

func countColumnKey(counts map[string]int, field schemamodel.Field) {
	if field.Check != "" && !expressible(field.Check) {
		counts["CHECK expressions DBML cannot quote"]++
	}
	if field.Foreign == "" {
		return
	}
	if field.Deferrable {
		counts["DEFERRABLE on keys"]++
	}
	if field.ForeignKeyMatch != "" {
		counts["MATCH FULL and PARTIAL on foreign keys"]++
	}
	if field.ForeignKeyNotEnforced {
		counts["NOT ENFORCED on constraints"]++
	}
}

func countConstraint(counts map[string]int, constraint schemamodel.Constraint) {
	switch strings.ToUpper(strings.TrimSpace(constraint.Type)) {
	case constraintExclude:
		counts["EXCLUDE constraints"]++
		return
	case constraintCheck:
		if !expressible(constraint.CheckExpression) {
			counts["CHECK expressions DBML cannot quote"]++
		}
	}
	if constraint.Deferrable {
		counts["DEFERRABLE on keys"]++
	}
	if constraint.Match != "" && !strings.EqualFold(constraint.Match, "SIMPLE") {
		counts["MATCH FULL and PARTIAL on foreign keys"]++
	}
	if constraint.NotEnforced {
		counts["NOT ENFORCED on constraints"]++
	}
	if len(constraint.IncludeColumns) > 0 {
		counts["INCLUDE columns on keys"]++
	}
	if constraint.NullsDistinct != nil && !*constraint.NullsDistinct {
		counts["NULLS NOT DISTINCT on keys"]++
	}
	if len(constraint.OnDeleteColumns) > 0 {
		counts["ON DELETE column lists"]++
	}
}
