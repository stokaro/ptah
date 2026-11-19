// Package atlascheckform writes the CHECK expressions a compatibility plan
// adds in the form the pinned community binary prints them.
//
// That binary creates the desired schema on the dev database and plans from
// what it reads back there. On an engine whose server rewrites a CHECK, its
// plan therefore carries the server's text rather than the author's: MySQL and
// MariaDB quote each column name, and MySQL introduces each string with its
// character set. [descriptor.Atlas.CheckSpelling] states which engines do so.
//
// The package plans the question and reads the answer; it opens no
// connection. [Plan] lists a probe for each declared CHECK a schema diff
// writes, [Tables] is what the dev database has to hold for them,
// [Statements] adds each probe to its table there, [Read] takes the
// expressions the dev database reports back, and [Spellings.Respell] writes
// them into the schema diff before it is planned.
// internal/atlasschema runs the probes on the dev database for `schema diff`
// and `migrate diff`.
package atlascheckform

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/normalize"
	"ptah.run/internal/sqlident"
	"ptah.run/internal/tableref"
)

// probeNamePrefix starts the name of each constraint a probe adds. The number
// after it keeps the names apart, since MySQL and MariaDB name a CHECK once
// per database rather than once per table.
const probeNamePrefix = "ptah_check_probe_"

// Probe asks the dev database for one declared CHECK: the expression a table
// declares, added to that table under a constraint name of the probe's own.
type Probe struct {
	// Schema and Table name the declared table as the declaration spells them.
	// Schema is empty where the declaration names none.
	Schema, Table string
	// Expression is the declared CHECK without the CHECK keyword.
	Expression string
	// Name is the name the probe adds the constraint under.
	Name string
}

// Plan returns a probe for each distinct CHECK expression a table of desired
// declares, in a table-level constraint or on a column, that diff writes, in
// declaration order. diff is the comparison of desired with the current
// state; a CHECK it does not write is not asked about, so a diff that writes
// none needs no dev database.
//
// It returns nil where the descriptor of dialect says the pinned community
// binary prints a CHECK as declared, where the dialect is unknown, and where
// diff writes no CHECK desired declares. The caller asks the dialect of the
// dev server, since a MySQL-family URL does not say whether MySQL or MariaDB
// answers it.
func Plan(dialect string, desired *schemamodel.Database, diff *difftypes.SchemaDiff) []Probe {
	if spellingOf(dialect) == descriptor.DeclaredCheckSpelling || desired == nil || diff == nil {
		return nil
	}
	written := make(map[spellingKey]bool)
	listed := *diff
	rewriteChecks(&listed, func(table, expression string) (string, bool) {
		written[keyOf(table, expression)] = true
		return "", false
	})
	fields := schemamodel.ProcessEmbeddedFields(desired.EmbeddedFields, desired.Fields)
	var probes []Probe
	seen := make(map[spellingKey]bool)
	add := func(table schemamodel.Table, expression string) {
		expression = strings.TrimSpace(expression)
		key := keyOf(table.Name, expression)
		if expression == "" || seen[key] || !written[key] {
			return
		}
		seen[key] = true
		probes = append(probes, Probe{
			Schema:     table.Schema,
			Table:      table.Name,
			Expression: expression,
			Name:       probeNamePrefix + strconv.Itoa(len(probes)+1),
		})
	}
	for _, table := range desired.Tables {
		for _, field := range fields {
			if field.StructName == table.StructName {
				add(table, field.Check)
			}
		}
		for _, constraint := range schemacapture.ConstraintsFor(desired.Constraints, table) {
			if isCheck(constraint.Type) {
				add(table, constraint.CheckExpression)
			}
		}
	}
	return probes
}

// Tables returns the part of desired that probes need on the dev database:
// each table a probe names, with its columns and its PRIMARY KEY and UNIQUE
// constraints, the schemas those tables are in, and the enums. A probe parses
// its CHECK against those columns and nothing else, so the rest stays out of
// the dev database: another table, a CHECK of its own, a foreign key, an
// index, a column default, an AUTO_INCREMENT, a view, a role or a grant could
// only cost time or make the creation fail. The tables keep their names, so a
// CHECK that names its own table reads the same there.
//
// It returns nil where desired is nil or there is no probe. desired is not
// changed; the result shares no list with it.
func Tables(desired *schemamodel.Database, probes []Probe) *schemamodel.Database {
	if desired == nil || len(probes) == 0 {
		return nil
	}
	type tableKey struct{ schema, name string }
	probed := make(map[tableKey]bool, len(probes))
	for _, probe := range probes {
		probed[tableKey{schema: probe.Schema, name: probe.Table}] = true
	}
	tables := &schemamodel.Database{Enums: make([]schemamodel.Enum, 0, len(desired.Enums))}
	structs := make(map[string]bool)
	schemas := make(map[string]bool)
	for _, table := range desired.Tables {
		if !probed[tableKey{schema: table.Schema, name: table.Name}] {
			continue
		}
		tables.Tables = append(tables.Tables, table.Clone())
		structs[table.StructName] = true
		schemas[table.Schema] = true
		for _, constraint := range schemacapture.ConstraintsFor(desired.Constraints, table) {
			switch strings.ToUpper(strings.TrimSpace(constraint.Type)) {
			case "PRIMARY KEY", "UNIQUE":
				tables.Constraints = append(tables.Constraints, constraint)
			}
		}
	}
	for _, schema := range desired.Schemas {
		if schemas[schema.Name] {
			tables.Schemas = append(tables.Schemas, schema)
		}
	}
	for _, enum := range desired.Enums {
		tables.Enums = append(tables.Enums, enum.Clone())
	}
	for _, field := range schemamodel.ProcessEmbeddedFields(desired.EmbeddedFields, desired.Fields) {
		if structs[field.StructName] {
			tables.Fields = append(tables.Fields, columnOnly(field.Clone()))
		}
	}
	return tables
}

// columnOnly returns field without what [Tables] leaves out of a column.
func columnOnly(field schemamodel.Field) schemamodel.Field {
	field.Check, field.CheckName, field.CheckNotEnforced = "", "", false
	field.Foreign, field.ForeignKeyName, field.OnDelete, field.OnUpdate = "", "", "", ""
	field.Deferrable, field.Initially, field.ForeignKeyMatch, field.ForeignKeyNotEnforced = false, "", "", false
	field.Default, field.DefaultSet, field.DefaultExpr = "", false, ""
	field.AutoInc = false
	field.IdentityGeneration, field.IdentityStart, field.IdentityIncrement, field.IdentityOptions = "", "", "", ""
	return field
}

// Statements returns, for each probe in order, the statement that adds its
// CHECK to its table on the dev database read describes.
//
// read is a catalog read of a dev database that holds the desired tables. A
// probe whose table it does not hold, or holds twice under one name, is an
// error, since the expression would be parsed against columns nobody declared.
func Statements(dialect string, probes []Probe, read *catalog.Database) ([]string, error) {
	statements := make([]string, 0, len(probes))
	for _, probe := range probes {
		table, err := tableOf(read, probe)
		if err != nil {
			return nil, err
		}
		target := sqlident.Quote(dialect, table.Name)
		if table.Schema != "" {
			target = sqlident.Quote(dialect, table.Schema) + "." + target
		}
		statements = append(statements, fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s)",
			target, sqlident.Quote(dialect, probe.Name), probe.Expression))
	}
	return statements, nil
}

// tableOf answers the table of read that probe names: the one of that name in
// the probe's schema, or else the only one of that name.
func tableOf(read *catalog.Database, probe Probe) (catalog.Table, error) {
	if read == nil {
		return catalog.Table{}, fmt.Errorf("check probe %s: no dev database read", probe.Name)
	}
	var named []catalog.Table
	for _, table := range read.Tables {
		if table.Name != probe.Table {
			continue
		}
		if probe.Schema != "" && table.Schema == probe.Schema {
			return table, nil
		}
		named = append(named, table)
	}
	if len(named) != 1 {
		return catalog.Table{}, fmt.Errorf(
			"check probe %s: the dev database holds %d tables named %q", probe.Name, len(named), probe.Table)
	}
	return named[0], nil
}

// Spellings is the form the dev database reported for each declared CHECK of
// one desired schema, as the pinned community binary prints it. The zero
// value holds none, and its [Spellings.Respell] changes nothing.
type Spellings struct {
	forms map[spellingKey]string
}

// spellingKey identifies a declared CHECK by the bare name of its table and
// its expression as declared. The name is compared without regard to case,
// as the diff may carry a folded spelling of it.
type spellingKey struct {
	table      string
	expression string
}

func keyOf(table, expression string) spellingKey {
	return spellingKey{table: strings.ToLower(table), expression: strings.TrimSpace(expression)}
}

// Read returns the form the dev database reported for each probe.
//
// read is a catalog read of the dev database after the probes' [Statements]
// ran. Each CHECK clause is taken as the reader yields it, as SQL, and written
// as the descriptor of dialect says: as reported, or without the pair of
// parentheses MySQL puts around the whole clause. A probe whose constraint
// read does not hold is an error.
func Read(dialect string, probes []Probe, read *catalog.Database) (Spellings, error) {
	if len(probes) == 0 {
		return Spellings{}, nil
	}
	clauses := make(map[string]string)
	if read != nil {
		for _, constraint := range read.Constraints {
			if isCheck(constraint.Type) && constraint.CheckClause != nil {
				clauses[strings.ToLower(constraint.Name)] = *constraint.CheckClause
			}
		}
	}
	spelling := spellingOf(dialect)
	quoting := normalize.QuotingOf(dialect)
	forms := make(map[spellingKey]string, len(probes))
	for _, probe := range probes {
		clause, ok := clauses[strings.ToLower(probe.Name)]
		if !ok {
			return Spellings{}, fmt.Errorf("check probe %s: the dev database reports no CHECK of that name", probe.Name)
		}
		clause = strings.TrimSpace(clause)
		if spelling == descriptor.DevCheckSpellingUnwrapped {
			clause = withoutEnclosingParentheses(clause, quoting)
		}
		forms[keyOf(probe.Table, probe.Expression)] = clause
	}
	return Spellings{forms: forms}, nil
}

// withoutEnclosingParentheses returns clause without the one pair of
// parentheses that encloses all of it, or clause itself when its first
// parenthesis closes before its end. Quoted text is skipped, so a parenthesis
// inside a string or a quoted name does not count.
func withoutEnclosingParentheses(clause string, quoting normalize.Quoting) string {
	last := len(clause) - 1
	if last < 1 || clause[0] != '(' || clause[last] != ')' {
		return clause
	}
	depth := 0
	for i := 0; i <= last; i++ {
		switch clause[i] {
		case '\'', '"', '`':
			i = quoting.QuotedEnd(clause, i)
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 && i != last {
				return clause
			}
		}
	}
	if depth != 0 {
		return clause
	}
	return strings.TrimSpace(clause[1:last])
}

// Respell writes the reported form into each CHECK diff adds: on a table it
// creates, on a column it adds, and as a constraint it adds to a table. A
// CHECK no probe asked about keeps its declared text.
//
// The comparison that produced diff is already made, so the forms change what
// the plan prints and nothing about what it plans. Respell replaces the lists
// it edits with copies, so a shallow copy of a diff can be respelled while
// the diff it was copied from keeps the declared text.
func (s Spellings) Respell(diff *difftypes.SchemaDiff) {
	if diff == nil || len(s.forms) == 0 {
		return
	}
	rewriteChecks(diff, s.form)
}

// rewriteChecks calls rewrite with the bare table name and the expression of
// each CHECK diff writes, and replaces the expression where rewrite answers
// true. A CHECK with no expression is skipped.
//
// [Plan] asks about the CHECKs this walk visits and [Spellings.Respell]
// rewrites them; one walk serves both, because a CHECK the plan writes and no
// probe asked about would keep its declared text. The lists it edits are
// replaced with copies, so a shallow copy of a diff can be rewritten while the
// diff it was copied from is left as it was.
func rewriteChecks(diff *difftypes.SchemaDiff, rewrite func(table, expression string) (string, bool)) {
	visit := func(table, expression string) (string, bool) {
		if strings.TrimSpace(expression) == "" {
			return "", false
		}
		return rewrite(table, expression)
	}
	diff.TablesAdded = slices.Clone(diff.TablesAdded)
	for i := range diff.TablesAdded {
		creation := &diff.TablesAdded[i]
		table := creation.Table.Name
		creation.Fields = rewriteFields(table, creation.Fields, visit)
		creation.Constraints = rewriteConstraints(table, creation.Constraints, visit)
	}
	diff.TablesModified = slices.Clone(diff.TablesModified)
	for i := range diff.TablesModified {
		modified := &diff.TablesModified[i]
		table := bareName(modified.TableName)
		modified.ColumnsAdded = rewriteFields(table, modified.ColumnsAdded, visit)
		modified.Desired.Fields = rewriteFields(table, modified.Desired.Fields, visit)
		modified.Desired.Constraints = rewriteConstraints(table, modified.Desired.Constraints, visit)
	}
	diff.ConstraintsAdded = slices.Clone(diff.ConstraintsAdded)
	for i := range diff.ConstraintsAdded {
		added := &diff.ConstraintsAdded[i]
		if form, ok := visit(bareName(added.TableName), added.CheckExpression); ok {
			added.CheckExpression = form
		}
	}
}

// rewriteFields returns fields with each column CHECK of table rewritten, as
// a copy when one changes.
func rewriteFields(
	table string,
	fields []schemamodel.Field,
	rewrite func(table, expression string) (string, bool),
) []schemamodel.Field {
	var respelled []schemamodel.Field
	for i, field := range fields {
		form, ok := rewrite(table, field.Check)
		if !ok {
			continue
		}
		if respelled == nil {
			respelled = make([]schemamodel.Field, len(fields))
			copy(respelled, fields)
		}
		respelled[i].Check = form
	}
	if respelled == nil {
		return fields
	}
	return respelled
}

// rewriteConstraints returns constraints with each CHECK of table rewritten,
// as a copy when one changes.
func rewriteConstraints(
	table string,
	constraints []schemamodel.Constraint,
	rewrite func(table, expression string) (string, bool),
) []schemamodel.Constraint {
	var respelled []schemamodel.Constraint
	for i, constraint := range constraints {
		if !isCheck(constraint.Type) {
			continue
		}
		form, ok := rewrite(table, constraint.CheckExpression)
		if !ok {
			continue
		}
		if respelled == nil {
			respelled = make([]schemamodel.Constraint, len(constraints))
			copy(respelled, constraints)
		}
		respelled[i].CheckExpression = form
	}
	if respelled == nil {
		return constraints
	}
	return respelled
}

// form answers the reported form of expression on table, and whether a probe
// asked about it.
func (s Spellings) form(table, expression string) (string, bool) {
	form, ok := s.forms[keyOf(table, expression)]
	return form, ok
}

// bareName answers the table name a diff spells, without its schema.
func bareName(name string) string {
	if ref, ok := tableref.Parse(name); ok {
		return ref.Name
	}
	return name
}

// isCheck reports whether a constraint type names a CHECK.
func isCheck(constraintType string) bool {
	return strings.EqualFold(strings.TrimSpace(constraintType), "CHECK")
}

// spellingOf answers the descriptor's CHECK spelling for dialect, and the
// declared spelling for a dialect the registry does not know.
func spellingOf(dialect string) descriptor.CheckSpelling {
	described, ok := platform.Lookup(dialect)
	if !ok {
		return descriptor.DeclaredCheckSpelling
	}
	return described.Atlas.CheckSpelling
}
