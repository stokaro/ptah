package registration_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/sqlschema"
)

// numberingFamily is an engine that names unnamed foreign keys and indexes as
// the MySQL family does, with MariaDB's clause and CREATE TABLE limit and
// MySQL's cut of a long index name: a combination neither built-in engine
// states.
func numberingFamily(name string, aliases ...string) descriptor.Dialect {
	engine := described(name, "", aliases...)
	engine.Identifiers.IndexNamespace = "table"
	engine.IdentifierLimit = descriptor.IdentifierLimit{Max: 64, Unit: "characters"}
	engine.IndexNaming = descriptor.IndexNaming{MaxBytes: 64, UniqueIsIndex: true, BaseBytes: 61}
	engine.ForeignKeyNaming = descriptor.ForeignKeyNamingTableNumbered
	engine.ForeignKeyNameRules = descriptor.ForeignKeyNameRules{CreateTableMaxChars: 63}
	engine.Grammar.Keys = descriptor.KeyGrammar{
		IndexKeywords:               []string{"KEY", "INDEX"},
		ForeignKeyIndexName:         true,
		ForeignKeyIndexNameNamesKey: true,
	}
	return engine
}

// foreignKeyNames lists the names of a model's foreign keys in order.
func foreignKeyNames(database schemamodel.Database) []string {
	var names []string
	for _, constraint := range database.Constraints {
		names = append(names, constraint.Name)
	}
	return names
}

// TestRegister_NamesTheForeignKeysOfAnEngineNobodyShipsByItsRules registers
// an engine with the MySQL family's naming and a mix of MariaDB's and MySQL's
// variants, and the SQL reader names its keys and indexes by what the
// descriptor states, though nothing names the engine. The reader is asked
// first: the parser refuses an index name after FOREIGN KEY, an unnamed key
// keeps no name, and two unnamed indexes over one column read as one unnamed
// index, as on any engine that states no index naming.
func TestRegister_NamesTheForeignKeysOfAnEngineNobodyShipsByItsRules(t *testing.T) {
	const parents = "CREATE TABLE p (id int PRIMARY KEY);\n"
	clause := parents + "CREATE TABLE c (id int PRIMARY KEY, p_id int, FOREIGN KEY idx_c_p (p_id) REFERENCES p (id));"
	longTable := strings.Repeat("t", 57)
	createLimit := parents + "CREATE TABLE " + longTable + " (id int PRIMARY KEY, p_id int, FOREIGN KEY (p_id) REFERENCES p (id));"
	longColumn := strings.Repeat("c", 64)
	cut := "CREATE TABLE x (id int PRIMARY KEY, " + longColumn + " int, KEY (" + longColumn + "), KEY (" + longColumn + "));"
	c := qt.New(t)
	database, _, err := sqlschema.Read([]byte(clause), "numberdb")
	c.Assert(err, qt.ErrorMatches, `.*an index name after FOREIGN KEY at position \d+ is the MySQL family's alone; .*`)
	c.Assert(database, qt.DeepEquals, schemamodel.Database{})
	database, _, err = sqlschema.Read([]byte(createLimit), "numberdb")
	c.Assert(err, qt.IsNil)
	c.Assert(foreignKeyNames(database), qt.DeepEquals, []string{""})
	database, _, err = sqlschema.Read([]byte(cut), "numberdb")
	c.Assert(err, qt.IsNil)
	c.Assert(indexNames(database), qt.DeepEquals, []string{""})

	c.Assert(platform.Register(numberingFamily("numbering", "numberdb")), qt.IsNil)

	database, _, err = sqlschema.Read([]byte(clause), "numberdb")
	c.Assert(err, qt.IsNil)
	c.Assert(foreignKeyNames(database), qt.DeepEquals, []string{"idx_c_p"})
	database, _, err = sqlschema.Read([]byte(createLimit), "numberdb")
	c.Assert(err, qt.ErrorMatches, `.*: `+longTable+`_ibfk_1, the name numbering gives an unnamed foreign key of `+longTable+
		` in CREATE TABLE, is 64 characters, and the server keeps at most 63 there as derived; name the key`)
	c.Assert(database, qt.DeepEquals, schemamodel.Database{})
	database, _, err = sqlschema.Read([]byte(cut), "numberdb")
	c.Assert(err, qt.IsNil)
	c.Assert(indexNames(database), qt.DeepEquals, []string{longColumn, longColumn[:61] + "_2"})
}

// indexNames lists the names of a model's indexes in order.
func indexNames(database schemamodel.Database) []string {
	var names []string
	for _, index := range database.Indexes {
		names = append(names, index.Name)
	}
	return names
}
