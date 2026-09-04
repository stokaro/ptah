package identifiervalidation_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/migration/internal/identifiervalidation"
)

// twoColumnTarget declares one table carrying exactly the two column names.
func twoColumnTarget(first, second string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t"}},
		Fields: []schemamodel.Field{
			{StructName: "T", Name: first, Type: "INT"},
			{StructName: "T", Name: second, Type: "INT"},
		},
	}
}

// The MySQL family folds column names with a collation table Ptah has no
// offline copy of, and the two engines disagree about which non-ASCII names
// they fold. Measured on mysql:8.4.11 and mariadb:11.8.9, two columns in one
// table, read back through HEX(COLUMN_NAME):
//
//	pair              MySQL 8.4.11    MariaDB 11.8.9
//	A / a             duplicate       duplicate
//	I / ı             distinct        duplicate
//	İ / i             duplicate       distinct
//	σ / ς             distinct        duplicate
//	K(U+212A) / K     duplicate       distinct
//
// The same fold decides foreign-key resolution: where an engine calls the pair
// one column, a foreign key written with either spelling binds to the declared
// column and reuses its key; where it does not, the key column is reported
// missing. So a name carrying a non-ASCII rune is reported as a possible
// conflict with every column in its table, ASCII ones included -- MySQL calls
// `İ` and plain `i` one column. See stokaro/ptah#2771.
func TestValidateTarget_NonASCIIColumnNamesThatMayCollide_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		first  string
		second string
	}{
		{name: "dotted capital I beside ASCII i", first: "İ", second: "i"},
		{name: "ASCII I beside a dotless i", first: "I", second: "ı"},
		{name: "small sigma beside a final sigma", first: "σ", second: "ς"},
		// Escaped: pasted through a shell the Kelvin sign arrives as ASCII K,
		// and the row then compares a name with itself.
		{name: "Kelvin sign beside ASCII K", first: "\u212A", second: "K"},
		{name: "A umlaut beside a umlaut", first: "Ä", second: "ä"},
	}

	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		for _, test := range tests {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				desired := twoColumnTarget(test.first, test.second)

				err := identifiervalidation.ValidateTarget(
					desired, dialect, identifier.ForDialect(dialect))

				c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
				c.Assert(err.Error(), qt.Contains, "may have the same catalog identity")
			})
		}
	}
}

// ASCII case is folded by both engines, which the model used to deny by
// calling MySQL-family column names exact. Measured: a table declaring `A` and
// `a` answers ERROR 1060 Duplicate column name 'a' on both.
func TestValidateTarget_ASCIICaseColumnNames_FailurePath(t *testing.T) {
	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			desired := twoColumnTarget("A", "a")

			err := identifiervalidation.ValidateTarget(
				desired, dialect, identifier.ForDialect(dialect))

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
		})
	}
}

// The controls. Two ordinary ASCII names still validate, and a dialect outside
// the MySQL family is untouched by the rule -- otherwise "everything
// conflicts" would pass every row above.
func TestValidateTarget_DistinctColumnNames_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		first   string
		second  string
	}{
		{name: "mysql keeps two ASCII names apart", dialect: platform.MySQL, first: "alpha", second: "beta"},
		{name: "mariadb keeps two ASCII names apart", dialect: platform.MariaDB, first: "alpha", second: "beta"},
		{name: "postgres keeps a non-ASCII pair apart", dialect: platform.Postgres, first: "İ", second: "i"},
		{name: "postgres keeps ASCII case apart", dialect: platform.Postgres, first: "A", second: "a"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := twoColumnTarget(test.first, test.second)

			err := identifiervalidation.ValidateTarget(
				desired, test.dialect, identifier.ForDialect(test.dialect))

			c.Assert(err, qt.IsNil)
		})
	}
}
