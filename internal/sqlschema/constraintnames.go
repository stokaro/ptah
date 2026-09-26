package sqlschema

import (
	"errors"
	"fmt"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/mysqlname"
	"ptah.run/internal/pgname"
	"ptah.run/internal/tableref"
)

// ErrDuplicateForeignKeyName is the class of a foreign key MySQL would name
// after another one already holds the name.
//
// The server does not move a derived name out of the way. Measured on MySQL
// 8.4.11, an unnamed key of `c5` beside an explicit `c5_ibfk_1`, on the same
// table or on another table of the database, and in either case of spelling,
// is `ERROR 1826 (HY000): Duplicate foreign key constraint name`. A model
// holding both keys under one name would lose one of them to the deduplication
// in schemamodel.Finalize without a word, so the document is refused, as the
// server refuses it.
var ErrDuplicateForeignKeyName = errors.New("two foreign keys claim the same name")

// The labels PostgreSQL ends a derived constraint name with.
const (
	foreignKeyLabel = "fkey"
	uniqueLabel     = "key"
)

// namesConstraintsLikePostgres reports whether an unnamed CHECK, UNIQUE or
// FOREIGN KEY read from a source of this dialect takes the name PostgreSQL
// gives it.
//
// PostgreSQL alone: every rule here was measured on PostgreSQL 18.6. The other
// engines name an unnamed constraint differently or not in a way anyone
// measured here -- SQLite keeps no name -- and they keep the name the rest of
// Ptah derives. MySQL's foreign keys have a rule of their own; see
// [namesForeignKeysLikeMySQL].
func namesConstraintsLikePostgres(sourcePlatform string) bool {
	return platform.NormalizeDialect(sourcePlatform) == platform.Postgres
}

// nameCreatedConstraints gives every unnamed CHECK, every unnamed table-level
// UNIQUE and every unnamed FOREIGN KEY one CREATE TABLE declared the name
// PostgreSQL gives it.
//
// The name has to be decided on the desired model, because the other side of a
// comparison is a catalog, and a catalog holds the name the server chose. A
// schema file compared with the database its own SQL created otherwise kept the
// server's `<table>_<column>_fkey` and added a second, identical key named
// Ptah's `fk_<table>_<column>` beside it, and dropped the server's
// `<table>_<columns>_key` to add the same UNIQUE back without a name, which the
// server named again (stokaro/ptah#3643). Left unnamed, a CHECK never pairs
// with the server's `<table>_check`, and the comparison drops the server's
// CHECK to add the same one back (stokaro/ptah#3729).
//
// The order is the server's. PostgreSQL creates the table and its CHECK
// constraints, then the index behind each UNIQUE, then the foreign keys, and
// each derived name avoids every name that exists by then. So every name the
// schema already holds is claimed first -- in this file, in the earlier files
// the document read, and the table's own explicitly named constraints -- then
// the CHECK names, then the UNIQUE names, then the foreign key names. A named
// CHECK written after an unnamed one that derives its name is refused by the
// server, so claiming the explicit names first changes no name a server
// accepts. Measured, `REFERENCES
// parent(id)` beside `CONSTRAINT dup_parent_id_fkey CHECK (...)` becomes
// `dup_parent_id_fkey1`, and `UNIQUE (a)` beside `CONSTRAINT s_a_key CHECK
// (...)` becomes `s_a_key1`. Inline foreign keys are named before table-level
// ones, and a CHECK written on a column before one written on the table. The
// server follows the document's order across the two, which the model does not
// hold; the order decides only which of two constraints deriving the same name
// takes the numbered one. Measured on PostgreSQL 18.6, `CREATE TABLE h (CHECK
// (a > 0), a int CHECK (a < 10))` gives `a > 0` the name `h_a_check`, where the
// model gives it to `a < 10`.
//
// A column-level UNIQUE is not named here: the model keeps it on the column,
// and the comparison matches it by its columns.
func nameCreatedConstraints(
	database, base *schemamodel.Database,
	table schemamodel.Table,
	fieldsStart, constraintsStart int,
	sourcePlatform string,
) {
	if !namesConstraintsLikePostgres(sourcePlatform) {
		return
	}
	databases := []*schemamodel.Database{database, base}
	constraints := constraintNamesInSchema(databases, table.Schema)
	relations := relationNamesInSchema(databases, table.Schema)
	nameCreatedChecks(database, table, fieldsStart, constraintsStart, constraints)
	for i := constraintsStart; i < len(database.Constraints); i++ {
		constraint := &database.Constraints[i]
		if !strings.EqualFold(constraint.Type, "UNIQUE") || constraint.Name != "" {
			continue
		}
		constraint.Name = claimDerivedName(table.Name, constraint.Columns, uniqueLabel, constraints, relations)
		relations.claim(constraint.Name)
	}
	for i := fieldsStart; i < len(database.Fields); i++ {
		field := &database.Fields[i]
		if field.Foreign == "" || field.ForeignKeyName != "" {
			continue
		}
		field.ForeignKeyName = claimDerivedName(table.Name, []string{field.Name}, foreignKeyLabel, constraints)
	}
	for i := constraintsStart; i < len(database.Constraints); i++ {
		constraint := &database.Constraints[i]
		if !strings.EqualFold(constraint.Type, "FOREIGN KEY") || constraint.Name != "" {
			continue
		}
		constraint.Name = claimDerivedName(table.Name, constraint.Columns, foreignKeyLabel, constraints)
	}
}

// namesForeignKeysLikeMySQL reports whether an unnamed FOREIGN KEY read from a
// source of this dialect takes the name MySQL gives it.
//
// MySQL alone: [mysqlname] holds the rule and what it was measured on.
// MariaDB was not measured, and keeps the name the rest of Ptah derives.
func namesForeignKeysLikeMySQL(sourcePlatform string) bool {
	return platform.NormalizeDialect(sourcePlatform) == platform.MySQL
}

// nameCreatedMySQLForeignKeys gives every unnamed foreign key one CREATE TABLE
// declared the name MySQL gives it, `<table>_ibfk_<n>`, numbered from 1 in the
// order the statement writes the unnamed keys.
//
// The name has to be decided on the desired model, for the reason
// [nameCreatedConstraints] gives: the other side of a comparison is a catalog,
// which holds the name the server chose. Left to Ptah's `fk_<table>_<column>`,
// a schema file compared with the database its own SQL built plans to add the
// key again under that name and drop the server's (stokaro/ptah#3725).
//
// A named key does not move the count, and a derived name is not moved out of
// the way of a named one: the server answers a collision with an error, and so
// does this; see [ErrDuplicateForeignKeyName].
//
// It runs after [nameMySQLInlineIndexes], which reads an empty name as a key
// the author left unnamed: the index the server builds for such a key is named
// after its column, not after the constraint.
func nameCreatedMySQLForeignKeys(
	database, base *schemamodel.Database, table schemamodel.Table, constraintsStart int, sourcePlatform string,
) error {
	if !namesForeignKeysLikeMySQL(sourcePlatform) {
		return nil
	}
	databases := []*schemamodel.Database{database, base}
	next := 1
	for i := constraintsStart; i < len(database.Constraints); i++ {
		constraint := &database.Constraints[i]
		if !isForeignKey(*constraint) || constraint.Name != "" {
			continue
		}
		name := mysqlname.ForeignKey(table.Name, next)
		if err := refuseHeldForeignKeyName(databases, table.Schema, name, table.Name); err != nil {
			return err
		}
		constraint.Name = name
		next++
	}
	return nil
}

// nameAddedMySQLForeignKey names an unnamed foreign key an ALTER TABLE adds:
// one more than the highest `<table>_ibfk_<n>` among the keys the table holds
// at that point of the document, in this file and in the earlier ones.
func nameAddedMySQLForeignKey(constraint *schemamodel.Constraint, target alterTarget) error {
	table := target.table
	if table == nil {
		return nil
	}
	highest := 0
	for _, database := range target.databases {
		if database == nil {
			continue
		}
		for _, held := range database.Constraints {
			if !isForeignKey(held) || held.Table != target.qualified {
				continue
			}
			if n, ok := mysqlname.ForeignKeyNumber(table.Name, held.Name); ok && n > highest {
				highest = n
			}
		}
	}
	name := mysqlname.ForeignKey(table.Name, highest+1)
	if err := refuseHeldForeignKeyName(target.databases, table.Schema, name, table.Name); err != nil {
		return err
	}
	constraint.Name = name
	return nil
}

// refuseHeldForeignKeyName refuses a derived name another foreign key of the
// same database already holds, compared without case as the server compares
// it. Every key of the schema counts, because a MySQL database is one
// namespace for foreign key names.
func refuseHeldForeignKeyName(databases []*schemamodel.Database, schema, name, table string) error {
	for _, database := range databases {
		if database == nil {
			continue
		}
		for _, held := range database.Constraints {
			if !isForeignKey(held) || !strings.EqualFold(held.Name, name) {
				continue
			}
			if heldSchema, _ := splitQualifiedTable(held.Table); heldSchema != schema {
				continue
			}
			return fmt.Errorf("%w: %s, the name MySQL gives an unnamed foreign key of %s, "+
				"is held by another foreign key, and MySQL answers ERROR 1826",
				ErrDuplicateForeignKeyName, held.Name, table)
		}
	}
	return nil
}

// isForeignKey reports whether a constraint of the model is a foreign key.
func isForeignKey(constraint schemamodel.Constraint) bool {
	return strings.EqualFold(constraint.Type, "FOREIGN KEY")
}

// nameCreatedChecks names the unnamed CHECKs of the table whose fields start at
// fieldsStart and whose constraints start at constraintsStart: those written on
// a column first, in column order, then those written on the table, in the
// order the table declared them. See [pgname.Check] for the rule.
func nameCreatedChecks(
	database *schemamodel.Database,
	table schemamodel.Table,
	fieldsStart, constraintsStart int,
	constraints namespaceNames,
) {
	fields := database.Fields[fieldsStart:]
	columns := make([]string, 0, len(fields))
	for _, field := range fields {
		columns = append(columns, field.Name)
	}
	for i := range fields {
		field := &fields[i]
		if field.Check == "" || field.CheckName != "" {
			continue
		}
		field.CheckName = claimCheckName(table.Name, field.Check, columns, constraints)
	}
	for i := constraintsStart; i < len(database.Constraints); i++ {
		constraint := &database.Constraints[i]
		if !strings.EqualFold(constraint.Type, "CHECK") || constraint.Name != "" {
			continue
		}
		constraint.Name = claimCheckName(table.Name, constraint.CheckExpression, columns, constraints)
	}
}

// nameAddedColumnCheck names the unnamed CHECK a column added by ALTER TABLE
// carries, by the rule [nameCreatedChecks] follows. columns are the table's
// columns with the added one among them: `ALTER TABLE t ADD COLUMN d int CHECK
// (d > a)` names the CHECK `t_check`, measured on PostgreSQL 18.6.
func nameAddedColumnCheck(field *schemamodel.Field, target alterTarget, columns []string) {
	if !namesConstraintsLikePostgres(target.sourcePlatform) || field.Check == "" || field.CheckName != "" {
		return
	}
	schema, table := splitQualifiedTable(target.qualified)
	field.CheckName = claimCheckName(table, field.Check, columns, constraintNamesInSchema(target.databases, schema))
}

// nameAddedConstraint names an unnamed CHECK, UNIQUE or FOREIGN KEY an ALTER
// TABLE adds, by the rule [nameCreatedConstraints] follows, against every name
// the schema already holds -- in this file and in the earlier ones the document
// read. An unnamed foreign key read for MySQL takes MySQL's name instead; see
// [nameAddedMySQLForeignKey].
func nameAddedConstraint(constraint *schemamodel.Constraint, target alterTarget) error {
	if constraint.Name != "" {
		return nil
	}
	if namesForeignKeysLikeMySQL(target.sourcePlatform) && isForeignKey(*constraint) {
		return nameAddedMySQLForeignKey(constraint, target)
	}
	if !namesConstraintsLikePostgres(target.sourcePlatform) {
		return nil
	}
	schema, table := splitQualifiedTable(target.qualified)
	constraints := constraintNamesInSchema(target.databases, schema)
	switch {
	case strings.EqualFold(constraint.Type, "CHECK"):
		_, columns := tableColumns(target.databases, target.structName)
		constraint.Name = claimCheckName(table, constraint.CheckExpression, columns, constraints)
	case strings.EqualFold(constraint.Type, "UNIQUE"):
		relations := relationNamesInSchema(target.databases, schema)
		constraint.Name = claimDerivedName(table, constraint.Columns, uniqueLabel, constraints, relations)
	case strings.EqualFold(constraint.Type, "FOREIGN KEY"):
		constraint.Name = claimDerivedName(table, constraint.Columns, foreignKeyLabel, constraints)
	}
	return nil
}

// claimCheckName derives the name of an unnamed CHECK on table and claims it in
// the constraint namespace. A CHECK has no index behind it, so the relation
// namespace does not constrain it: measured, an index already called
// `l_a_check` leaves `CHECK (a > 0)` on `l` named `l_a_check`.
func claimCheckName(table, expression string, columns []string, constraints namespaceNames) string {
	name := pgname.Check(table, expression, columns, constraints.taken)
	constraints.claim(name)
	return name
}

// claimDerivedName derives the first name none of the sets holds and claims it
// in the first set, which is the constraint namespace.
func claimDerivedName(table string, columns []string, label string, sets ...namespaceNames) string {
	name := pgname.Constraint(table, columns, label, func(candidate string) bool {
		for _, set := range sets {
			if set.taken(candidate) {
				return true
			}
		}
		return false
	})
	sets[0].claim(name)
	return name
}

// namespaceNames is the set of names one namespace of one schema holds.
//
// Schema-wide rather than per table, because that is where PostgreSQL looks:
// it picks a name no constraint in the schema carries, and for the index
// behind a UNIQUE, no relation either. Exact rather than folded, because every
// name reaching here was already read the way the server stores it.
type namespaceNames map[string]struct{}

func (n namespaceNames) taken(name string) bool {
	_, found := n[name]
	return found
}

func (n namespaceNames) claim(name string) {
	if name != "" {
		n[name] = struct{}{}
	}
}

// constraintNamesInSchema collects every constraint name the databases give
// the tables of one schema: table constraints, the names a column carries for
// its own constraints, and a table's named primary key.
//
// Explicit names and the names derived here are the only ones that can
// collide with a derived `_check`, `_fkey` or `_key`: every other name
// PostgreSQL derives ends in a label of its own, such as `_pkey` or
// `_not_null`.
func constraintNamesInSchema(databases []*schemamodel.Database, schema string) namespaceNames {
	names := make(namespaceNames)
	for _, database := range databases {
		if database == nil {
			continue
		}
		inSchema := tablesInSchema(database, schema)
		for _, table := range database.Tables {
			if table.Schema == schema {
				names.claim(table.PrimaryKeyName)
			}
		}
		for _, constraint := range database.Constraints {
			if tableSchema, _ := splitQualifiedTable(constraint.Table); tableSchema == schema {
				names.claim(constraint.Name)
			}
		}
		for _, field := range database.Fields {
			if !inSchema[field.StructName] {
				continue
			}
			names.claim(field.ForeignKeyName)
			names.claim(field.CheckName)
			names.claim(field.NotNullConstraintName)
		}
	}
	return names
}

// relationNamesInSchema collects the names of the relations the databases
// declare in one schema -- tables, views, materialized views, sequences and
// indexes -- which the index behind a UNIQUE may not take either. Measured, an
// index or a table already called `q_a_key` makes `UNIQUE (a)` on `q` become
// `q_a_key1`.
func relationNamesInSchema(databases []*schemamodel.Database, schema string) namespaceNames {
	names := make(namespaceNames)
	for _, database := range databases {
		if database == nil {
			continue
		}
		inSchema := tablesInSchema(database, schema)
		for _, table := range database.Tables {
			if table.Schema == schema {
				names.claim(table.Name)
			}
		}
		for _, view := range database.Views {
			if viewSchema, name := splitQualifiedTable(view.Name); viewSchema == schema {
				names.claim(name)
			}
		}
		for _, view := range database.MaterializedViews {
			if viewSchema, name := splitQualifiedTable(view.Name); viewSchema == schema {
				names.claim(name)
			}
		}
		for _, sequence := range database.Sequences {
			if sequence.Schema == schema {
				names.claim(sequence.Name)
			}
		}
		for _, index := range database.Indexes {
			if indexInSchema(index, inSchema, schema) {
				names.claim(index.Name)
			}
		}
	}
	return names
}

// tablesInSchema answers, by struct name, which tables of database sit in
// schema.
func tablesInSchema(database *schemamodel.Database, schema string) map[string]bool {
	inSchema := make(map[string]bool, len(database.Tables))
	for _, table := range database.Tables {
		if table.Schema == schema {
			inSchema[table.StructName] = true
		}
	}
	return inSchema
}

// indexInSchema reports whether an index belongs to a table of schema, read
// from the table it names when it names one and from its owner otherwise.
func indexInSchema(index schemamodel.Index, inSchema map[string]bool, schema string) bool {
	if index.TableName != "" {
		tableSchema, _ := splitQualifiedTable(index.TableName)
		return tableSchema == schema
	}
	return inSchema[index.StructName]
}

// splitQualifiedTable returns the schema and bare name of a table reference the
// reader already qualified, and the reference itself with no schema when it
// does not parse.
func splitQualifiedTable(qualified string) (schema, name string) {
	ref, ok := tableref.Parse(qualified)
	if !ok {
		return "", qualified
	}
	return ref.Schema, ref.Name
}
