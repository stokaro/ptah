package sqlschema

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/columnkey"
	"ptah.run/internal/mysqlname"
	"ptah.run/internal/pgname"
	"ptah.run/internal/tableref"
)

// ErrDuplicateForeignKeyName is the class of a foreign key MySQL or MariaDB
// would name after another one already holds the name.
//
// The server does not move a derived name out of the way. Measured, an unnamed
// key of `c5` beside an explicit `c5_ibfk_1`, on the same table or on another
// table of the database, is refused: MySQL 8.4.11 and 26.7.0 answer
// `ERROR 1826 (HY000): Duplicate foreign key constraint name` in either case of
// spelling, and MariaDB 11.8.9 answers `ERROR 1005 (HY000)`, with errno 150 on
// the same table and errno 121 on another. A model holding both keys under one
// name would lose one of them to the deduplication in schemamodel.Finalize
// without a word, so the document is refused, as the server refuses it.
//
// MariaDB 12.1 and later accept these documents, because they name the
// unnamed key `<n>` instead; see [mysqlname.IsNumberedForeignKeyName]. The
// reader cannot tell which line a file is for and derives the older name, so
// it refuses them for those lines too.
var ErrDuplicateForeignKeyName = errors.New("two foreign keys claim the same name")

// ErrForeignKeyNameTooLong is the class of a foreign key MySQL or MariaDB would
// name past the longest name the server keeps as derived. The server refuses
// the statement or cuts the name, and neither leaves a key under the name the
// reader derives; see [mysqlname.ForeignKeyNameLimit].
var ErrForeignKeyNameTooLong = errors.New("the derived foreign key name is longer than the engine keeps")

// foreignKeyLabel is the label PostgreSQL ends the name of an unnamed foreign
// key with.
const foreignKeyLabel = "fkey"

// namesConstraintsLikePostgres reports whether an unnamed CHECK, UNIQUE or
// FOREIGN KEY read from a source of this dialect takes the name PostgreSQL
// gives it.
//
// PostgreSQL alone: every rule here was measured on PostgreSQL 18.6. The other
// engines name an unnamed constraint differently or not in a way anyone
// measured here -- SQLite keeps no name -- and they keep the name the rest of
// Ptah derives. The foreign keys of MySQL and MariaDB have a rule of their
// own; see [mysqlname.NamesForeignKeys].
func namesConstraintsLikePostgres(sourcePlatform string) bool {
	return platform.NormalizeDialect(sourcePlatform) == platform.Postgres
}

// nameCreatedMySQLForeignKeys gives every unnamed foreign key one CREATE TABLE
// declared the name MySQL and MariaDB give it, `<table>_ibfk_<n>`, numbered
// from 1 in the order the statement writes the unnamed keys. keys are the
// table's foreign keys in that order, on a column or on the table; see
// [declaredSites]. MariaDB 12.1 and later write only the number, and the
// comparison reads such a key as the one named here; see
// [mysqlname.IsNumberedForeignKeyName].
//
// The name has to be decided on the desired model, for the reason
// [nameCreatedConstraints] gives: the other side of a comparison is a catalog,
// which holds the name the server chose. Left to Ptah's `fk_<table>_<column>`,
// a schema file compared with the database its own SQL built plans to add the
// key again under that name and drop the server's (stokaro/ptah#3725,
// stokaro/ptah#3743).
//
// A key a column declares with `REFERENCES` counts at the column's place.
// MySQL 8.4 builds nothing from the clause, and the parser refuses it there.
//
// A named key does not move the count, and a derived name is not moved out of
// the way of a named one: the server answers a collision with an error, and so
// does this; see [ErrDuplicateForeignKeyName]. Nor is it shortened; see
// [ErrForeignKeyNameTooLong].
//
// It runs after [nameMySQLInlineIndexes], which reads an empty name as a key
// the author left unnamed: the index the server builds for such a key is named
// after its column, not after the constraint.
func nameCreatedMySQLForeignKeys(
	database, base *schemamodel.Database, table schemamodel.Table, keys []elementSite, sourcePlatform string,
) error {
	if !mysqlname.NamesForeignKeys(sourcePlatform) {
		return nil
	}
	databases := []*schemamodel.Database{database, base}
	next := 1
	for _, site := range keys {
		name := foreignKeySites().name(database, site)
		if *name != "" {
			continue
		}
		derived := mysqlname.ForeignKey(table.Name, next)
		if err := refuseDerivedForeignKeyName(
			databases, table, derived, sourcePlatform, mysqlname.CreateTable,
		); err != nil {
			return err
		}
		*name = derived
		next++
	}
	return nil
}

// nameAddedMySQLForeignKey names an unnamed foreign key an ALTER TABLE adds:
// the next number of the statement, which [newAlterStatement] read from the
// keys the table held before the statement ran, in this file and in the
// earlier ones.
func nameAddedMySQLForeignKey(target alterTarget) (string, error) {
	table := target.table
	name := mysqlname.ForeignKey(table.Name, target.statement.nextForeignKey)
	if err := refuseDerivedForeignKeyName(
		target.databases, *table, name, target.sourcePlatform, mysqlname.AlterTable,
	); err != nil {
		return "", err
	}
	target.statement.nextForeignKey++
	return name, nil
}

// refuseDerivedForeignKeyName refuses a name the server derives for an unnamed
// foreign key of table and would not keep: one longer than the statement
// allows, and one another foreign key of the database already holds.
func refuseDerivedForeignKeyName(
	databases []*schemamodel.Database, table schemamodel.Table, name, sourcePlatform string,
	statement mysqlname.Statement,
) error {
	if limit := mysqlname.ForeignKeyNameLimit(sourcePlatform, statement); utf8.RuneCountInString(name) > limit {
		return fmt.Errorf("%w: %s, the name %s gives an unnamed foreign key of %s in %s, is %d characters, "+
			"and the server keeps at most %d there as derived; name the key",
			ErrForeignKeyNameTooLong, name, platform.NormalizeDialect(sourcePlatform), table.Name,
			statementName(statement), utf8.RuneCountInString(name), limit)
	}
	return refuseHeldForeignKeyName(databases, table.Schema, name, table.Name)
}

// statementName spells the statement as SQL does.
func statementName(statement mysqlname.Statement) string {
	if statement == mysqlname.AlterTable {
		return "ALTER TABLE"
	}
	return "CREATE TABLE"
}

// nameAddedColumnForeignKeys names the key a column added by ALTER TABLE ...
// ADD COLUMN declares with `REFERENCES`, in the statement's sequence: measured
// on MySQL 26.7.0 and MariaDB 11.8.9, `ADD COLUMN b INT REFERENCES p(id), ADD
// FOREIGN KEY (a) ...` on a table holding `c_ibfk_2` names the column's key
// `c_ibfk_3` and the other `c_ibfk_4`.
func nameAddedColumnForeignKeys(fields []schemamodel.Field, target alterTarget) error {
	if !mysqlname.NamesForeignKeys(target.sourcePlatform) {
		return nil
	}
	for i := range fields {
		field := &fields[i]
		if field.Foreign == "" || field.ForeignKeyName != "" {
			continue
		}
		name, err := nameAddedMySQLForeignKey(target)
		if err != nil {
			return err
		}
		field.ForeignKeyName = name
	}
	return nil
}

// refuseRestatedColumnForeignKey refuses `ALTER TABLE ... ADD COLUMN IF NOT
// EXISTS` restating a column that already exists with a `REFERENCES` clause,
// on an engine that names unnamed foreign keys by [mysqlname].
//
// The restatement is not the no-op the rest of the model reads it as. Measured
// on MariaDB 11.8.9 and 12.3.3, `ALTER TABLE c ADD COLUMN IF NOT EXISTS a int
// REFERENCES p(id)` on a table that already has `a int REFERENCES p(id)` skips
// the column and still adds the key: the table ends with `c_ibfk_1` and
// `c_ibfk_2`, or `1` and `2`, both over `a`. Read as a restatement, the model
// holds one key, and a comparison with the database the file built plans to
// drop the other. MySQL builds no key from the clause, and the parser refuses
// it there.
func refuseRestatedColumnForeignKey(field schemamodel.Field, target alterTarget, column string) error {
	if field.Foreign == "" || !mysqlname.NamesForeignKeys(target.sourcePlatform) {
		return nil
	}
	return fmt.Errorf(
		"%w: ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s restates a column that exists, and the "+
			"server skips the column but still adds the foreign key its REFERENCES clause declares; "+
			"declare the key with ALTER TABLE %s ADD FOREIGN KEY instead",
		ErrUnmodeledStatement, target.written, column, target.written)
}

// refuseHeldForeignKeyName refuses a derived name another foreign key of the
// same database already holds, compared without case as the server compares
// it. Every key of the schema counts, a column's own included, because a
// MySQL or MariaDB database is one namespace for foreign key names.
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
			return duplicateForeignKeyNameError(held.Name, table)
		}
		inSchema := tablesInSchema(database, schema)
		for _, field := range database.Fields {
			if field.Foreign == "" || !inSchema[field.StructName] || !strings.EqualFold(field.ForeignKeyName, name) {
				continue
			}
			return duplicateForeignKeyNameError(field.ForeignKeyName, table)
		}
	}
	return nil
}

func duplicateForeignKeyNameError(held, table string) error {
	return fmt.Errorf("%w: %s, the name the server gives an unnamed foreign key of %s, "+
		"is held by another foreign key, and the server refuses the statement",
		ErrDuplicateForeignKeyName, held, table)
}

// isForeignKey reports whether a constraint of the model is a foreign key.
func isForeignKey(constraint schemamodel.Constraint) bool {
	return strings.EqualFold(constraint.Type, "FOREIGN KEY")
}

// nameAddedColumnCheck names the unnamed CHECK a column added by ALTER TABLE
// carries, by the rule [nameCreatedConstraints] follows. columns are the table's
// columns with the added one among them: `ALTER TABLE t ADD COLUMN d int CHECK
// (d > a)` names the CHECK `t_check`, measured on PostgreSQL 18.6.
func nameAddedColumnCheck(field *schemamodel.Field, target alterTarget, columns []string) {
	if !namesConstraintsLikePostgres(target.sourcePlatform) || field.Check == "" || field.CheckName != "" {
		return
	}
	schema, table := splitQualifiedTable(target.qualified)
	field.CheckName = claimCheckName(table, field.Check, columns, constraintNamesInSchema(target.databases, schema))
}

// nameAddedConstraint names an unnamed CHECK, UNIQUE, EXCLUDE or FOREIGN KEY
// an ALTER TABLE adds, by the rule [nameCreatedConstraints] follows, against
// every name the schema already holds -- in this file and in the earlier ones
// the document read. MySQL and MariaDB name UNIQUE and FOREIGN KEY differently:
// an unnamed foreign key takes [nameAddedMySQLForeignKey], and an unnamed
// UNIQUE is an index named like any other; see [nameAddedMySQLIndex].
func nameAddedConstraint(constraint *schemamodel.Constraint, target alterTarget) error {
	if constraint.Name != "" {
		return nil
	}
	if mysqlname.NamesForeignKeys(target.sourcePlatform) && isForeignKey(*constraint) {
		name, err := nameAddedMySQLForeignKey(target)
		if err != nil {
			return err
		}
		constraint.Name = name
		return nil
	}
	if strings.EqualFold(constraint.Type, "UNIQUE") {
		if _, ok := namingFor(target.sourcePlatform); ok {
			return nameAddedMySQLUnique(constraint, target)
		}
	}
	if isCheck(*constraint) {
		if err := nameAddedMySQLFamilyCheck(&constraint.Name, nil, target); err != nil {
			return err
		}
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
	case strings.EqualFold(constraint.Type, "UNIQUE"), strings.EqualFold(constraint.Type, "EXCLUDE"):
		nameIndexedConstraint(constraint, table, constraints, pgname.RelationNames(target.databases, schema))
	case strings.EqualFold(constraint.Type, "FOREIGN KEY"):
		constraint.Name = claimDerivedName(table, constraint.Columns, foreignKeyLabel, constraints)
	}
	return nil
}

// nameIndexedConstraint names an unnamed UNIQUE or EXCLUDE on table, the two
// kinds the server builds an index for. The index is a relation, so the name
// avoids every relation of the schema as well as every constraint, and claims
// its place in both. Any other constraint, and one with a name, is left alone.
func nameIndexedConstraint(
	constraint *schemamodel.Constraint,
	table string,
	constraints, relations pgname.Names,
) {
	if constraint.Name != "" || !isIndexedConstraint(*constraint) {
		return
	}
	constraint.Name = indexedConstraintName(*constraint, table, func(candidate string) bool {
		return constraints.Taken(candidate) || relations.Taken(candidate)
	})
	constraints.Claim(constraint.Name)
	relations.Claim(constraint.Name)
}

// isIndexedConstraint reports whether the server builds an index for a
// constraint: a UNIQUE or an EXCLUDE. The primary key is kept on the table.
func isIndexedConstraint(constraint schemamodel.Constraint) bool {
	return strings.EqualFold(constraint.Type, "UNIQUE") || strings.EqualFold(constraint.Type, "EXCLUDE")
}

// indexedConstraintName answers the name PostgreSQL gives an unnamed UNIQUE or
// EXCLUDE on table, numbered past every name taken answers true for.
func indexedConstraintName(constraint schemamodel.Constraint, table string, taken func(string) bool) string {
	if strings.EqualFold(constraint.Type, "EXCLUDE") {
		return pgname.Exclude(table, constraint.ExcludeElements, taken)
	}
	return pgname.Unique(table, constraint.Columns, constraint.IncludeColumns, taken)
}

// claimCheckName derives the name of an unnamed CHECK on table and claims it in
// the constraint namespace. A CHECK has no index behind it, so the relation
// namespace does not constrain it: measured, an index already called
// `l_a_check` leaves `CHECK (a > 0)` on `l` named `l_a_check`.
func claimCheckName(table, expression string, columns []string, constraints pgname.Names) string {
	name := pgname.Check(table, expression, columns, constraints.Taken)
	constraints.Claim(name)
	return name
}

// claimDerivedName derives the first name none of the sets holds and claims it
// in the first set, which is the constraint namespace.
func claimDerivedName(table string, columns []string, label string, sets ...pgname.Names) string {
	name := pgname.Constraint(table, columns, label, func(candidate string) bool {
		for _, set := range sets {
			if set.Taken(candidate) {
				return true
			}
		}
		return false
	})
	sets[0].Claim(name)
	return name
}

// constraintNamesInSchema collects every constraint name the databases give
// the tables of one schema, as [pgname.ConstraintNames] reads them, and the
// name of each column's own UNIQUE.
//
// A column's own UNIQUE holds `<table>_<column>_key` although the model keeps
// no name for it; see [columnkey.Name]. Measured on PostgreSQL 18.6, `ALTER
// TABLE c ADD COLUMN b int UNIQUE, ADD UNIQUE (b)` names the column's key
// c_b_key and the other c_b_key1, and `UNIQUE (b)` added to a table whose b is
// UNIQUE is c_b_key1. Named c_b_key here, the table's key would pair with the
// column's in the database, and the comparison would plan the column's key
// under a name the database holds.
func constraintNamesInSchema(databases []*schemamodel.Database, schema string) pgname.Names {
	names := pgname.ConstraintNames(databases, schema)
	for _, database := range databases {
		if database == nil {
			continue
		}
		tables := make(map[string]string, len(database.Tables))
		for _, table := range database.Tables {
			if table.Schema == schema {
				tables[table.StructName] = table.Name
			}
		}
		for _, field := range database.Fields {
			table, inSchema := tables[field.StructName]
			if inSchema && field.Unique && strings.TrimSpace(field.UniqueExpr) == "" {
				name, _ := columnkey.Name(platform.Postgres, table, field.Name, nil)
				names.Claim(name)
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
