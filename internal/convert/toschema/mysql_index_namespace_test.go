package toschema_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/convert/toschema"
	"go.5x5.cz/ptah/internal/parser"
)

// dialectSchema parses one document as a named dialect.
//
// The dialect is a parameter here, unlike mysqlSchema, because the three
// defects this file covers are exactly the ones where MySQL and MariaDB differ
// and a family-wide answer was wrong for one of them.
func dialectSchema(c *qt.C, dialect, sql string) (schemamodel.Database, error) {
	c.Helper()
	statements, err := parser.NewParser(sql, parser.WithDialect(dialect)).Parse()
	c.Assert(err, qt.IsNil)
	return toschema.ToDatabase(statements, dialect)
}

// TestToDatabase_TheIndexNamespaceFoldsCaseFailurePath covers stokaro/ptah#2757.
//
// Measured on MySQL 9.7.2 and MariaDB 11.8.9: `KEY Foo (a), KEY foo (b)` is
// `ERROR 1061 (42000): Duplicate key name 'foo'` on both. A Go map keyed on the
// raw string called those two names distinct, so the desired model accepted a
// table neither server can create.
func TestToDatabase_TheIndexNamespaceFoldsCaseFailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "mysql", dialect: platform.MySQL},
		{name: "mariadb", dialect: platform.MariaDB},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := dialectSchema(c, test.dialect,
				"CREATE TABLE case_names (a INT, b INT, KEY Foo (a), KEY foo (b));")

			c.Assert(err, qt.ErrorIs, toschema.ErrDuplicateIndexName)
		})
	}
}

// TestToDatabase_ADerivedNameKeepsItsColumnsSpellingHappyPath is the other half
// of folding, and the one a case-insensitive map alone would get wrong.
//
// Measured: a table with a column `Foo` and an index explicitly named `foo`
// yields `foo` and `Foo_2`. The FOLDED name decides identity; the original
// decides what is written.
func TestToDatabase_ADerivedNameKeepsItsColumnsSpellingHappyPath(t *testing.T) {
	c := qt.New(t)

	database, err := dialectSchema(c, platform.MySQL,
		"CREATE TABLE case_derived (Foo INT, b INT, KEY foo (b), KEY (Foo));")

	c.Assert(err, qt.IsNil)
	c.Assert(indexNames(database), qt.DeepEquals, []string{"foo", "Foo_2"})
}

// TestToDatabase_PrimaryIsReservedWithoutAPrimaryKeyHappyPath is the second
// half of stokaro/ptah#2757.
//
// Measured on both engines: a table whose only key is “KEY (`PRIMARY`)“ over
// a column called `PRIMARY` gets `PRIMARY_2`, with no primary key anywhere.
// Ptah seeded that claim only when it saw a primary key, so it derived
// `PRIMARY` -- a name the server refuses outright.
func TestToDatabase_PrimaryIsReservedWithoutAPrimaryKeyHappyPath(t *testing.T) {
	c := qt.New(t)

	database, err := dialectSchema(c, platform.MySQL,
		"CREATE TABLE primary_exact (`PRIMARY` INT, KEY (`PRIMARY`));")

	c.Assert(err, qt.IsNil)
	c.Assert(indexNames(database), qt.DeepEquals, []string{"PRIMARY_2"})
}

// TestToDatabase_AnIndexNamedPrimaryIsRefusedFailurePath separates the reserved
// name from a collision.
//
// Both engines answer `ERROR 1280 (42000): Incorrect index name 'PRIMARY'`, and
// they answer it on a table with no other index. Reporting a duplicate would
// send the reader looking for the other one.
func TestToDatabase_AnIndexNamedPrimaryIsRefusedFailurePath(t *testing.T) {
	c := qt.New(t)

	_, err := dialectSchema(c, platform.MySQL,
		"CREATE TABLE explicit_primary (a INT, KEY `PRIMARY` (a));")

	c.Assert(err, qt.ErrorIs, toschema.ErrReservedIndexName)
}

// TestToDatabase_SuffixingHonorsTheIdentifierLimit covers stokaro/ptah#2759.
//
// MySQL truncates the base to 61 BYTES and does it whatever the suffix costs --
// measured on 9.7.2, a 64-character column yields a 63-character `_2` and a
// 64-character `_10`, so it is not "fit within 64". MariaDB does not truncate:
// a 63-character column is `ERROR 1280` while 62 is accepted, so Ptah refuses
// at conversion rather than letting the failure arrive at execution.
func TestToDatabase_SuffixingHonorsTheIdentifierLimit(t *testing.T) {
	long := strings.Repeat("a", 64)
	fits := strings.Repeat("a", 62)

	t.Run("mysql truncates the base to 61 bytes", func(t *testing.T) {
		c := qt.New(t)

		database, err := dialectSchema(c, platform.MySQL,
			"CREATE TABLE long_names ("+long+" INT, KEY ("+long+"), KEY ("+long+"));")

		c.Assert(err, qt.IsNil)
		c.Assert(indexNames(database), qt.DeepEquals,
			[]string{long, strings.Repeat("a", 61) + "_2"})
	})

	t.Run("mysql keeps 61 bytes as the suffix grows", func(t *testing.T) {
		c := qt.New(t)
		keys := strings.Repeat(", KEY ("+long+")", 10)

		database, err := dialectSchema(c, platform.MySQL,
			"CREATE TABLE grow ("+long+" INT"+keys+");")

		c.Assert(err, qt.IsNil)
		names := indexNames(database)
		// `_10` is 64 bytes rather than 63, which is what tells a base cut to
		// 61 from one cut to `64 - len(suffix)`.
		c.Assert(names[len(names)-1], qt.Equals, strings.Repeat("a", 61)+"_10")
		c.Assert(len(names[len(names)-1]), qt.Equals, 64)
	})

	t.Run("mariadb refuses a name it cannot render", func(t *testing.T) {
		c := qt.New(t)
		wide := strings.Repeat("a", 63)

		_, err := dialectSchema(c, platform.MariaDB,
			"CREATE TABLE wide_names ("+wide+" INT, KEY ("+wide+"), KEY ("+wide+"));")

		c.Assert(err, qt.ErrorIs, toschema.ErrIndexNameTooLong)
	})

	t.Run("mariadb accepts one that fits", func(t *testing.T) {
		c := qt.New(t)

		database, err := dialectSchema(c, platform.MariaDB,
			"CREATE TABLE fitting ("+fits+" INT, KEY ("+fits+"), KEY ("+fits+"));")

		c.Assert(err, qt.IsNil)
		c.Assert(indexNames(database), qt.DeepEquals, []string{fits, fits + "_2"})
	})
}
