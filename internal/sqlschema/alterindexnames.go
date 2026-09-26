package sqlschema

import (
	"fmt"
	"strings"

	"ptah.run/core/schemamodel"
)

// nameAddedIndex gives an unnamed index an ALTER TABLE adds the name the
// server gives it, on the engines [namingFor] answers for. See
// [nameAddedMySQLIndex].
func nameAddedIndex(index *schemamodel.Index, target alterTarget) error {
	if index.Name != "" {
		return nil
	}
	if _, ok := namingFor(target.sourcePlatform); !ok {
		return nil
	}
	name, err := nameAddedMySQLIndex(target, firstIndexColumn(*index))
	if err != nil {
		return err
	}
	index.Name = name
	return nil
}

// nameAddedMySQLUnique names an unnamed UNIQUE an ALTER TABLE adds. On MySQL
// and MariaDB a UNIQUE is an index, and it takes the name an index takes.
func nameAddedMySQLUnique(constraint *schemamodel.Constraint, target alterTarget) error {
	if len(constraint.Columns) == 0 {
		return fmt.Errorf("%w: a unique constraint on %s names no column",
			ErrUnnamedIndex, target.written)
	}
	name, err := nameAddedMySQLIndex(target, constraint.Columns[0])
	if err != nil {
		return err
	}
	constraint.Name = name
	return nil
}

// nameAddedMySQLIndex answers the name MySQL and MariaDB give an unnamed index
// or UNIQUE that ALTER TABLE adds: the first column's name, then `_2`, `_3`
// and on, against every index the table holds at that point of the document.
//
// The rule is the one [nameMySQLInlineIndexes] follows inside CREATE TABLE,
// and it has to be decided here for the reason that function gives: the
// catalog holds the name the server chose. Left empty, a UNIQUE compared with
// the database its own file built plans to add the key again and drop the
// server's (stokaro/ptah#3742), and the comparison refuses an index that has
// no name. Measured identically on MySQL 8.4.11, 26.7.0 and MariaDB 11.8.9, on
// `CREATE TABLE c (id INT PRIMARY KEY, a INT, b INT, ...)`:
//
//	the table holds             ALTER TABLE c ...                     names
//	nothing                     ADD UNIQUE (a)                        a
//	KEY a (b)                   ADD UNIQUE (a)                        a_2
//	KEY A (b)                   ADD UNIQUE (a)                        a_2
//	KEY a (b), KEY a_2 (b)      ADD UNIQUE (a)                        a_3
//	a INT UNIQUE                ADD UNIQUE (a, b)                     a_2
//	nothing                     ADD INDEX (a), ADD KEY (a), ADD UNIQUE (a)   a, a_2, a_3
//	nothing                     ADD UNIQUE (a), ADD UNIQUE a_2 (b), ADD UNIQUE (a)   a, a_2, a_3
//	`PRIMARY` INT               ADD UNIQUE (`PRIMARY`)                PRIMARY_2
//
// Each operation of a statement claims its name before the next one is named,
// which is what the model already says after each operation is applied.
//
// The index the server builds for a foreign key is in the namespace too; see
// [heldIndexNames] for what it holds and when it lets go.
func nameAddedMySQLIndex(target alterTarget, column string) (string, error) {
	naming, _ := namingFor(target.sourcePlatform)
	table := *target.table
	if column == "" {
		column = naming.functionalBase
	}
	if column == "" {
		return "", fmt.Errorf(
			"%w: the index on %s starts with an expression, which this engine has no functional index for",
			ErrUnnamedIndex, table.Name)
	}
	name, err := derive(heldIndexNames(target), column, table, naming)
	if err != nil {
		return "", err
	}
	if err := refuseNonASCIIIndexName(name, table); err != nil {
		return "", err
	}
	return name, nil
}

// heldIndexNames is the index namespace of the table an ALTER TABLE names, as
// the document holds it at that point.
//
// The model keeps every index an author wrote or the reader named, every
// UNIQUE, and each column's own UNIQUE, which the server names after its
// column. The index the server built for a foreign key is in the namespace too,
// and the document records it under the name the server gave it; see
// [keyIndex]. Measured on every engine [mysqlname.NamesForeignKeys] answers
// for, with `FK (x)` standing for `FOREIGN KEY (x) REFERENCES p(id)`:
//
//	the table holds                ALTER TABLE c ...    names
//	CONSTRAINT b FK (a)            ADD UNIQUE (b)       b_2, and `b` stays the key's
//	FK (a, b)                      ADD UNIQUE (a)       a_2, and `a` stays the key's
//	FK (a)                         ADD UNIQUE (a)       a, and the key's `a` is dropped
//	CONSTRAINT fk FK (a)           ADD UNIQUE (a)       a, and the key's `fk` is dropped
//	FK (a)                         ADD UNIQUE (a, b)    a, and the key's `a` is dropped
//	FK (a)                         ADD KEY (a DESC)     a_2 on MySQL; a on MariaDB, which drops the key's
//
// The server drops the key's index before it names the index being added, and
// [alterTarget.releaseKeyIndexes] has already taken it out by the time this
// runs.
func heldIndexNames(target alterTarget) indexNames {
	claimed := make(indexNames)
	claimed.claim("PRIMARY")
	for _, database := range target.databases {
		claimDeclaredNames(claimed, database, target)
	}
	for _, built := range target.keys.onTable(target.qualified) {
		claimed.claim(built.name)
	}
	return claimed
}

// claimDeclaredNames claims the names of the keys database declares for the
// table: its indexes, its UNIQUEs and its columns' own UNIQUEs.
func claimDeclaredNames(claimed indexNames, database *schemamodel.Database, target alterTarget) {
	for _, field := range database.Fields {
		if field.StructName == target.structName && field.Unique {
			claimed.claim(field.Name)
		}
	}
	for _, index := range database.Indexes {
		if target.ownsIndex(index) && index.Name != "" {
			claimed.claim(index.Name)
		}
	}
	for _, constraint := range database.Constraints {
		if constraint.Table == target.qualified && strings.EqualFold(constraint.Type, "UNIQUE") &&
			constraint.Name != "" {
			claimed.claim(constraint.Name)
		}
	}
}
