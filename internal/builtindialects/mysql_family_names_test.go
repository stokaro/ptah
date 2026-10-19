package builtindialects_test

import (
	"cmp"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/mysqlname"
)

// everySpelling is every spelling a dialect accepts, as declared, in upper
// case and padded with spaces, and four that no dialect accepts.
func everySpelling() []string {
	var spellings []string
	for _, spelling := range platform.DialectSpellings() {
		spellings = append(spellings, spelling, strings.ToUpper(spelling), " "+spelling+" ")
	}
	return append(spellings, "", "unknown", "mysql8", "maria db")
}

// spellingsOf is the spellings of everySpelling that fold onto one of
// dialects.
func spellingsOf(dialects ...string) []string {
	return slices.DeleteFunc(everySpelling(), func(spelling string) bool {
		return !slices.Contains(dialects, platform.NormalizeDialect(spelling))
	})
}

// TestDescriptors_ForeignKeyIndexNameNamesKeyAgreesWithTheSwitchItReplaces
// pins Grammar.Keys.ForeignKeyIndexNameNamesKey, which the parser reads,
// against the switch of mysqlname.ClauseIndexNamesKey it replaces, written out
// here: the name between FOREIGN KEY and the column list is the key's own on
// every spelling NormalizeDialect folds onto MariaDB, and on no other
// spelling, an unknown one included.
func TestDescriptors_ForeignKeyIndexNameNamesKeyAgreesWithTheSwitchItReplaces(t *testing.T) {
	switchNamed := map[string]bool{platform.MariaDB: true}
	spellings := everySpelling()
	c := qt.New(t)
	c.Assert(len(spellings) > 3*len(platform.DialectSpellings()), qt.IsTrue,
		qt.Commentf("the spellings include every accepted one, respelled, and some no dialect accepts"))
	for _, spelling := range spellings {
		t.Run(spelling, func(t *testing.T) {
			c := qt.New(t)

			described, _ := platform.Lookup(spelling)

			c.Assert(described.Grammar.Keys.ForeignKeyIndexNameNamesKey, qt.Equals, switchNamed[platform.NormalizeDialect(spelling)])
		})
	}
}

// TestDescriptors_ForeignKeyNameLimitAgreesWithTheSwitchItReplaces pins the
// limit mysqlname.ForeignKeyNameLimit answers from ForeignKeyNameRules and
// IdentifierLimit against the switch it replaces, written out here: 63
// characters in CREATE TABLE on MariaDB, and 64 in every other case. The SQL
// reader asks it only for a dialect whose descriptor states a
// ForeignKeyNaming, which is every spelling of MySQL and MariaDB, and no
// other dialect states rules for it.
func TestDescriptors_ForeignKeyNameLimitAgreesWithTheSwitchItReplaces(t *testing.T) {
	switchLimit := map[string]map[mysqlname.Statement]int{platform.MariaDB: {mysqlname.CreateTable: 63}}
	statements := map[mysqlname.Statement]string{mysqlname.CreateTable: "CREATE TABLE", mysqlname.AlterTable: "ALTER TABLE"}
	family := spellingsOf(platform.MySQL, platform.MariaDB)
	c := qt.New(t)
	c.Assert(len(family) >= 6, qt.IsTrue, qt.Commentf("both engines' spellings, respelled"))
	for _, spelling := range family {
		for statement, statementName := range statements {
			t.Run(spelling+"/"+statementName, func(t *testing.T) {
				c := qt.New(t)
				described, found := platform.Lookup(spelling)
				c.Assert(found, qt.IsTrue)
				c.Assert(described.ForeignKeyNaming, qt.Equals, descriptor.ForeignKeyNamingTableNumbered)

				got := mysqlname.ForeignKeyNameLimit(described.ForeignKeyNameRules, described.IdentifierLimit, statement)

				c.Assert(got, qt.Equals, cmp.Or(switchLimit[platform.NormalizeDialect(spelling)][statement], 64))
			})
		}
	}
	for _, spelling := range slices.DeleteFunc(everySpelling(), func(spelling string) bool { return slices.Contains(family, spelling) }) {
		t.Run(spelling, func(t *testing.T) {
			c := qt.New(t)

			described, _ := platform.Lookup(spelling)

			c.Assert(described.ForeignKeyNaming, qt.Equals, descriptor.ForeignKeyNaming(""))
			c.Assert(described.ForeignKeyNameRules, qt.Equals, descriptor.ForeignKeyNameRules{})
		})
	}
}

// TestDescriptors_NumberedForeignKeyNamesAgreeWithTheSwitchItReplaces pins
// mysqlname.IsNumberedForeignKeyName over ForeignKeyNameRules, which the
// comparison asks on every dialect, against the switch it replaces, written
// out here: a positive number without a leading zero is a key's name on every
// spelling that folds onto MariaDB, and no name is on any other spelling.
func TestDescriptors_NumberedForeignKeyNamesAgreeWithTheSwitchItReplaces(t *testing.T) {
	switchNumbered := map[string]bool{platform.MariaDB: true}
	names := map[string]bool{"1": true, "12": true, "03": false, "0": false, "c_ibfk_1": false, "": false}
	spellings := everySpelling()
	c := qt.New(t)
	c.Assert(len(spellings) > 3*len(platform.DialectSpellings()), qt.IsTrue,
		qt.Commentf("the spellings include every accepted one, respelled, and some no dialect accepts"))
	for _, spelling := range spellings {
		for name, number := range names {
			t.Run(spelling+"/"+name, func(t *testing.T) {
				c := qt.New(t)
				described, _ := platform.Lookup(spelling)

				got := mysqlname.IsNumberedForeignKeyName(described.ForeignKeyNameRules, name)

				c.Assert(got, qt.Equals, switchNumbered[platform.NormalizeDialect(spelling)] && number)
			})
		}
	}
}

// TestDescriptors_IndexBaseBytesAgreesWithTheSwitchItReplaces pins
// mysqlname.IndexName over IndexNaming.BaseBytes against the switch it
// replaces, written out here: the name is cut to 61 bytes before `_2` and on,
// on a character boundary, on every spelling that folds onto MySQL and on no
// other. Each row carries the switch's answer on MySQL and elsewhere for a
// name the table holds.
func TestDescriptors_IndexBaseBytesAgreesWithTheSwitchItReplaces(t *testing.T) {
	long := strings.Repeat("c", 64)
	accented := strings.Repeat("c", 60) + "é" + "ccc"
	tests := []struct {
		base, mysql, elsewhere string
	}{
		{base: "p_id", mysql: "p_id_2", elsewhere: "p_id_2"},
		{base: long[:61], mysql: long[:61] + "_2", elsewhere: long[:61] + "_2"},
		{base: long[:62], mysql: long[:61] + "_2", elsewhere: long[:62] + "_2"},
		{base: long, mysql: long[:61] + "_2", elsewhere: long + "_2"},
		{base: accented, mysql: accented[:60] + "_2", elsewhere: accented + "_2"},
	}
	spellings := everySpelling()
	c := qt.New(t)
	c.Assert(len(spellings) > 3*len(platform.DialectSpellings()), qt.IsTrue,
		qt.Commentf("the spellings include every accepted one, respelled, and some no dialect accepts"))
	for _, spelling := range spellings {
		for _, test := range tests {
			t.Run(spelling+"/"+test.base, func(t *testing.T) {
				c := qt.New(t)
				described, _ := platform.Lookup(spelling)
				want := map[bool]string{true: test.mysql, false: test.elsewhere}[platform.NormalizeDialect(spelling) == platform.MySQL]

				got := mysqlname.IndexName(described.IndexNaming.BaseBytes, test.base, func(name string) bool { return name == test.base })

				c.Assert(got, qt.Equals, want)
			})
		}
	}
}

// TestDescriptors_CheckNameLimitAgreesWithTheConstantItReplaces pins the
// IdentifierLimit the SQL reader refuses a derived CHECK name past against
// the constant it replaces, mysqlname.MaxIdentifierChars, 64. The reader asks
// on a dialect whose descriptor states CheckNamingTableNumbered alone, which
// is every spelling of MySQL.
func TestDescriptors_CheckNameLimitAgreesWithTheConstantItReplaces(t *testing.T) {
	numbering := slices.DeleteFunc(everySpelling(), func(spelling string) bool {
		described, _ := platform.Lookup(spelling)
		return described.CheckNaming != descriptor.CheckNamingTableNumbered
	})
	c := qt.New(t)
	c.Assert(numbering, qt.DeepEquals, spellingsOf(platform.MySQL))
	for _, spelling := range numbering {
		t.Run(spelling, func(t *testing.T) {
			c := qt.New(t)

			described, _ := platform.Lookup(spelling)

			c.Assert(described.IdentifierLimit, qt.Equals, descriptor.IdentifierLimit{Max: 64, Unit: "characters"})
		})
	}
}
