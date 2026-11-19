package atlasfilter_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlasfilter"
	"ptah.run/internal/builtintest"
)

// readTableNames and keptGeneratedNames list the tables a filtered state
// keeps, as the state names them, without a schema.
func keptGeneratedNames(database *schemamodel.Database) []string {
	names := make([]string, 0, len(database.Tables))
	for _, table := range database.Tables {
		names = append(names, table.Name)
	}
	return names
}

func readTableNames(database *catalog.Database) []string {
	names := make([]string, 0, len(database.Tables))
	for _, table := range database.Tables {
		names = append(names, table.Name)
	}
	return names
}

// oracleNamesakes is an Oracle read holding the table created quoted as
// "orders", its folded namesake ORDERS and a third table, and the schema
// converted from that read, which spells the first `"orders"`.
func oracleNamesakes() (*catalog.Database, *schemamodel.Database) {
	read := &catalog.Database{StoredNames: true, Tables: []catalog.Table{
		{Name: "KEEP_ME", Schema: "APP"}, {Name: "orders", Schema: "APP"}, {Name: "ORDERS", Schema: "APP"},
	}}
	converted := &schemamodel.Database{Tables: []schemamodel.Table{
		{StructName: "keep_me", Name: "KEEP_ME", Schema: "APP"},
		{StructName: "orders", Name: `"orders"`, Schema: "APP"},
		{StructName: "ORDERS", Name: "ORDERS", Schema: "APP"},
	}}
	return read, converted
}

// postgresQuotedName is a PostgreSQL read holding a table whose stored name
// is "q", quote characters included, as `CREATE TABLE app2."""q"""` makes,
// beside a table stored as q, and the schema converted from it, which names
// each table by its stored name.
func postgresQuotedName() (*catalog.Database, *schemamodel.Database) {
	read := &catalog.Database{Tables: []catalog.Table{
		{Name: `"q"`, Schema: "app2"}, {Name: "q", Schema: "app2"},
	}}
	converted := &schemamodel.Database{Tables: []schemamodel.Table{
		{StructName: "quoted", Name: `"q"`, Schema: "app2"},
		{StructName: "plain", Name: "q", Schema: "app2"},
	}}
	return read, converted
}

// TestExclude_MatchesTheStoredSpellingOnBothSides excludes tables from an
// Oracle read and from the schema converted from it. A selector meets the
// stored name on both, so orders leaves the same table on each side and ORDERS
// the other one. Matched against the spelling, orders would leave the read's
// table and keep the converted one, and a diff of the database against itself
// would plan the excluded table.
func TestExclude_MatchesTheStoredSpellingOnBothSides(t *testing.T) {
	tests := []struct {
		exclude string
		// read and converted are the tables each side keeps, as each spells
		// them.
		read      []string
		converted []string
	}{
		{exclude: "orders", read: []string{"KEEP_ME", "ORDERS"}, converted: []string{"KEEP_ME", "ORDERS"}},
		{exclude: "ORDERS", read: []string{"KEEP_ME", "orders"}, converted: []string{"KEEP_ME", `"orders"`}},
		{exclude: "APP.orders", read: []string{"KEEP_ME", "ORDERS"}, converted: []string{"KEEP_ME", "ORDERS"}},
		{exclude: "o*", read: []string{"KEEP_ME", "ORDERS"}, converted: []string{"KEEP_ME", "ORDERS"}},
	}
	for _, test := range tests {
		t.Run(test.exclude, func(t *testing.T) {
			c := qt.New(t)
			scope := atlasfilter.Scope{Selections: ownerSelections(), Exclude: []string{test.exclude}, DefaultSchema: "APP", Dialect: "oracle"}
			read, converted := oracleNamesakes()

			scopedRead, _, readErr := atlasfilter.ScopeDatabaseReport(read, scope)
			scopedConverted, _, convertedErr := atlasfilter.ExcludeGeneratedScopeReport(converted, scope)

			c.Assert(readErr, qt.IsNil)
			c.Assert(convertedErr, qt.IsNil)
			c.Assert(readTableNames(scopedRead), qt.ContentEquals, test.read)
			c.Assert(keptGeneratedNames(scopedConverted), qt.ContentEquals, test.converted)
		})
	}
}

// TestInclude_MatchesTheStoredSpellingOnBothSides is the include selection
// over the same pair: orders keeps the same table on each side.
func TestInclude_MatchesTheStoredSpellingOnBothSides(t *testing.T) {
	c := qt.New(t)
	scope := atlasfilter.Scope{Selections: ownerSelections(), Include: []string{"orders"}, DefaultSchema: "APP", Dialect: "oracle"}
	read, converted := oracleNamesakes()

	scopedRead, readErr := atlasfilter.ScopeDatabase(read, scope)
	scopedConverted, convertedErr := atlasfilter.ScopeGenerated(converted, scope)

	c.Assert(readErr, qt.IsNil)
	c.Assert(convertedErr, qt.IsNil)
	c.Assert(readTableNames(scopedRead), qt.DeepEquals, []string{"orders"})
	c.Assert(keptGeneratedNames(scopedConverted), qt.DeepEquals, []string{`"orders"`})
}

// TestExclusion_MatchesTheStoredSpellingOnBothSides drives the same pair
// through the migration exclusion, which takes the dialect from the
// connection's naming.
func TestExclusion_MatchesTheStoredSpellingOnBothSides(t *testing.T) {
	c := qt.New(t)
	exclusion, err := atlasfilter.NewExclusion(builtintest.Runtime(), []string{"orders"})
	c.Assert(err, qt.IsNil)
	naming := atlasfilter.ConnectionNaming(catalog.ServerInfo{Dialect: "oracle", URL: "oracle://localhost:1521/FREEPDB1", Schema: "APP"})
	read, converted := oracleNamesakes()

	observed, _, observedErr := exclusion.Observed(read, naming)
	desired, _, desiredErr := exclusion.Desired(converted, naming)

	c.Assert(observedErr, qt.IsNil)
	c.Assert(desiredErr, qt.IsNil)
	c.Assert(readTableNames(observed), qt.ContentEquals, []string{"KEEP_ME", "ORDERS"})
	c.Assert(keptGeneratedNames(desired), qt.ContentEquals, []string{"KEEP_ME", "ORDERS"})
}

// oracleDeclared is an Oracle read holding ORDERS and KEEP_ME, and a schema
// declared for it in a SQL file, which writes both names bare and in lower
// case, as Oracle stores them in upper case.
func oracleDeclared() (*catalog.Database, *schemamodel.Database) {
	read := &catalog.Database{StoredNames: true, Tables: []catalog.Table{
		{Name: "KEEP_ME", Schema: "APP"}, {Name: "ORDERS", Schema: "APP"},
	}}
	declared := &schemamodel.Database{Tables: []schemamodel.Table{
		{StructName: "keep_me", Name: "keep_me"},
		{StructName: "orders", Name: "orders"},
	}}
	return read, declared
}

// TestExclude_FoldsABareDeclaredName excludes from an Oracle read and from a
// declaration that writes its names bare. A bare name is stored in the fold's
// case, so ORDERS leaves the declared orders and the read's ORDERS alike, and
// orders, which names a table created quoted, leaves neither. Matched as
// written, orders would leave the declared table and keep the read's, and the
// plan would drop the table the selector meant to leave alone.
func TestExclude_FoldsABareDeclaredName(t *testing.T) {
	tests := []struct {
		exclude string
		// read and declared are the tables each side keeps, as each spells
		// them.
		read     []string
		declared []string
	}{
		{exclude: "ORDERS", read: []string{"KEEP_ME"}, declared: []string{"keep_me"}},
		{exclude: "APP.ORDERS", read: []string{"KEEP_ME"}, declared: []string{"keep_me"}},
		{exclude: "O*", read: []string{"KEEP_ME"}, declared: []string{"keep_me"}},
		{exclude: "orders", read: []string{"KEEP_ME", "ORDERS"}, declared: []string{"keep_me", "orders"}},
	}
	for _, test := range tests {
		t.Run(test.exclude, func(t *testing.T) {
			c := qt.New(t)
			scope := atlasfilter.Scope{Selections: ownerSelections(), Exclude: []string{test.exclude}, DefaultSchema: "APP", Dialect: "oracle"}
			read, declared := oracleDeclared()

			scopedRead, _, readErr := atlasfilter.ScopeDatabaseReport(read, scope)
			scopedDeclared, _, declaredErr := atlasfilter.ExcludeGeneratedScopeReport(declared, scope)

			c.Assert(readErr, qt.IsNil)
			c.Assert(declaredErr, qt.IsNil)
			c.Assert(readTableNames(scopedRead), qt.ContentEquals, test.read)
			c.Assert(keptGeneratedNames(scopedDeclared), qt.ContentEquals, test.declared)
		})
	}
}

// TestInclude_FoldsABareDeclaredName is the include selection over the same
// pair: ORDERS keeps the declared orders and the read's ORDERS.
func TestInclude_FoldsABareDeclaredName(t *testing.T) {
	c := qt.New(t)
	scope := atlasfilter.Scope{Selections: ownerSelections(), Include: []string{"ORDERS"}, DefaultSchema: "APP", Dialect: "oracle"}
	read, declared := oracleDeclared()

	scopedRead, readErr := atlasfilter.ScopeDatabase(read, scope)
	scopedDeclared, declaredErr := atlasfilter.ScopeGenerated(declared, scope)

	c.Assert(readErr, qt.IsNil)
	c.Assert(declaredErr, qt.IsNil)
	c.Assert(readTableNames(scopedRead), qt.DeepEquals, []string{"ORDERS"})
	c.Assert(keptGeneratedNames(scopedDeclared), qt.DeepEquals, []string{"orders"})
}

// TestExclude_KeepsTheDefaultSchemaAsStored excludes a declared table that
// names no schema, in an account whose schema was created quoted and is
// stored as app. The default schema is a stored name, so app.ORDERS leaves
// the table; folded with the declared names, the default would read APP and
// the selector would match nothing.
func TestExclude_KeepsTheDefaultSchemaAsStored(t *testing.T) {
	c := qt.New(t)
	scope := atlasfilter.Scope{Selections: ownerSelections(), Exclude: []string{"app.ORDERS"}, DefaultSchema: "app", Dialect: "oracle"}
	_, declared := oracleDeclared()

	scopedDeclared, _, err := atlasfilter.ExcludeGeneratedScopeReport(declared, scope)

	c.Assert(err, qt.IsNil)
	c.Assert(keptGeneratedNames(scopedDeclared), qt.DeepEquals, []string{"keep_me"})
}

// TestExclude_QuoteCharactersInAStoredName excludes from a PostgreSQL read
// and its converted schema. PostgreSQL states no fold, so a name meets a
// selector as written on both sides: the selector "q", quotes included,
// leaves the table stored with them, as does its qualified form, which quotes
// the name as SQL does, and q leaves the other one. Unquoting the names first
// would let q leave both and "q" leave none.
func TestExclude_QuoteCharactersInAStoredName(t *testing.T) {
	tests := []struct {
		exclude string
		want    []string
	}{
		{exclude: `"q"`, want: []string{"q"}},
		{exclude: `app2."""q"""`, want: []string{"q"}},
		{exclude: "q", want: []string{`"q"`}},
		{exclude: "app2.q", want: []string{`"q"`}},
	}
	for _, test := range tests {
		t.Run(test.exclude, func(t *testing.T) {
			c := qt.New(t)
			scope := atlasfilter.Scope{Selections: ownerSelections(), Exclude: []string{test.exclude}, DefaultSchema: "public", RealmRelativePatterns: true, Dialect: "postgres"}
			read, converted := postgresQuotedName()

			scopedRead, _, readErr := atlasfilter.ScopeDatabaseReport(read, scope)
			scopedConverted, _, convertedErr := atlasfilter.ExcludeGeneratedScopeReport(converted, scope)

			c.Assert(readErr, qt.IsNil)
			c.Assert(convertedErr, qt.IsNil)
			c.Assert(readTableNames(scopedRead), qt.DeepEquals, test.want)
			c.Assert(keptGeneratedNames(scopedConverted), qt.DeepEquals, test.want)
		})
	}
}
