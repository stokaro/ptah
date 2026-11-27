package schemaprep_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemaprep"
)

// insensitiveKey and sensitiveKey stand for the keys a SQL Server connection
// resolves under a case-insensitive and a case-sensitive collation.
var (
	insensitiveKey = strings.ToLower
	sensitiveKey   = func(name string) string { return name }
)

// Under a collation a connection resolved, a schema and a table the document
// spells in another case than the database holds them take the held spelling
// where the collation reads the two as one, and keep what they were written
// with where it reads them as two. The input is not modified.
func TestRespellReadNamesBy_CatalogRule_HappyPath(t *testing.T) {
	read := schemaprep.ReadNames{
		Session:   "dbo",
		Databases: []string{"dbo", "Sales"},
		Relations: []schemaprep.ReadRelation{{Database: "Sales", Name: "Items"}},
	}
	tests := []struct {
		name             string
		key              func(string) string
		wantDB, wantName string
	}{
		{name: "a case-insensitive collation", key: insensitiveKey, wantDB: "Sales", wantName: "Items"},
		{name: "a case-sensitive collation", key: sensitiveKey, wantDB: "sales", wantName: "items"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := spelledTable("sales", "items")

			got, err := schemaprep.RespellReadNamesBy(database, read, schemaprep.CatalogRule(test.key))

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, spelledTable(test.wantDB, test.wantName))
			c.Assert(database, qt.DeepEquals, spelledTable("sales", "items"))
		})
	}
}

// SQL Server stores a view body as it was written, so a body keeps the
// qualifiers it was written with under a collation rule, where a MySQL-family
// server that folds names stores them as it holds the database.
func TestRespellReadNamesBy_ViewBodies(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		rule    schemaprep.NameRule
		want    string
	}{
		{
			name: "a SQL Server collation", dialect: platform.SQLServer,
			rule: schemaprep.CatalogRule(insensitiveKey), want: "SELECT id FROM SALES.items",
		},
		{
			name: "a MySQL server that folds names", dialect: platform.MySQL,
			rule: schemaprep.FoldingRule(catalog.NameFoldingFolds), want: "SELECT id FROM `sales`.items",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := &schemamodel.Database{Views: []schemamodel.View{{Name: "sales.v", Body: "SELECT id FROM SALES.items"}}}
			read := schemaprep.ReadNames{Databases: []string{"sales"}, Dialect: test.dialect}

			got, err := schemaprep.RespellReadNamesBy(database, read, test.rule)

			c.Assert(err, qt.IsNil)
			c.Assert(got.Views[0].Body, qt.Equals, test.want)
		})
	}
}

// Where no connection said how the database collation compares names, a
// schema or a table that matches a held one only without regard to case is
// refused, naming the collation as what decides it.
func TestRespellReadNamesBy_UnansweredCatalogRule_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		database *schemamodel.Database
		read     schemaprep.ReadNames
		want     schemaprep.UnknownFoldingError
	}{
		{
			name:     "a schema",
			database: spelledTable("sales", "Items"),
			read: schemaprep.ReadNames{
				Session: "dbo", Databases: []string{"Sales"},
				Relations: []schemaprep.ReadRelation{{Database: "Sales", Name: "Items"}},
			},
			want: schemaprep.UnknownFoldingError{Kind: "schema", Written: "sales", Found: "Sales", Setting: "the database collation"},
		},
		{
			name:     "a table",
			database: spelledTable("Sales", "items"),
			read: schemaprep.ReadNames{
				Session: "dbo", Databases: []string{"Sales"},
				Relations: []schemaprep.ReadRelation{{Database: "Sales", Name: "Items"}},
			},
			want: schemaprep.UnknownFoldingError{Kind: "table", Written: "items", Found: "Items", Setting: "the database collation"},
		},
		{
			name:     "two schemas the document declares",
			database: &schemamodel.Database{Schemas: []schemamodel.Schema{{Name: "Sales"}, {Name: "sales"}}},
			read:     schemaprep.ReadNames{Session: "dbo"},
			want:     schemaprep.UnknownFoldingError{Kind: "schema", Written: "sales", Found: "Sales", Setting: "the database collation"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := schemaprep.RespellReadNamesBy(test.database, test.read, schemaprep.UnansweredCatalogRule())

			var unknown *schemaprep.UnknownFoldingError
			c.Assert(err, qt.ErrorAs, &unknown)
			c.Assert(*unknown, qt.Equals, test.want)
			c.Assert(got, qt.IsNil)
		})
	}
}

// A document spelled as the database holds it, or naming what the database
// does not hold, passes a rule no connection answered unchanged.
func TestRespellReadNamesBy_UnansweredCatalogRule_HappyPath(t *testing.T) {
	read := schemaprep.ReadNames{
		Session: "dbo", Databases: []string{"Sales"},
		Relations: []schemaprep.ReadRelation{{Database: "Sales", Name: "Items"}},
	}
	tests := []struct {
		name      string
		db, table string
	}{
		{name: "spelled as the database holds it", db: "Sales", table: "Items"},
		{name: "a schema the database does not hold", db: "archive", table: "items"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := schemaprep.RespellReadNamesBy(spelledTable(test.db, test.table), read, schemaprep.UnansweredCatalogRule())

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, spelledTable(test.db, test.table))
		})
	}
}

// A name takes the one spelling the rule reads as it, and keeps what it was
// written with where it is spelled as one of them, where it matches none, and
// under a rule that compares names as bytes.
func TestRespellName_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		rule      schemaprep.NameRule
		written   string
		spellings []string
		want      string
	}{
		{name: "a server that folds names", rule: schemaprep.FoldingRule(catalog.NameFoldingFolds), written: "SALES", spellings: []string{"sales"}, want: "sales"},
		{name: "a case-insensitive collation", rule: schemaprep.CatalogRule(insensitiveKey), written: "sales", spellings: []string{"Sales"}, want: "Sales"},
		{name: "a case-sensitive collation", rule: schemaprep.CatalogRule(sensitiveKey), written: "sales", spellings: []string{"Sales"}, want: "sales"},
		{name: "a server that keeps names apart", rule: schemaprep.FoldingRule(catalog.NameFoldingExact), written: "SALES", spellings: []string{"sales"}, want: "SALES"},
		{name: "spelled as one of them", rule: schemaprep.FoldingRule(catalog.NameFoldingUnknown), written: "Sales", spellings: []string{"sales", "Sales"}, want: "Sales"},
		{name: "matching none", rule: schemaprep.UnansweredCatalogRule(), written: "archive", spellings: []string{"Sales"}, want: "archive"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := schemaprep.RespellName(test.rule, test.written, test.spellings)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// A name matching two spellings is refused under a rule a connection
// answered, and a name that would be respelled under one no connection
// answered.
func TestRespellName_FailurePath(t *testing.T) {
	t.Run("two spellings", func(t *testing.T) {
		c := qt.New(t)

		got, err := schemaprep.RespellName(schemaprep.CatalogRule(insensitiveKey), "sales", []string{"Sales", "SALES"})

		c.Assert(err, qt.ErrorMatches, `schema "sales" matches "Sales" and "SALES", names the server reads as one `+
			`under the database collation; no spelling is chosen`)
		c.Assert(got, qt.Equals, "")
	})
	tests := []struct {
		name string
		rule schemaprep.NameRule
		want schemaprep.UnknownFoldingError
	}{
		{
			name: "a MySQL server whose folding no connection read", rule: schemaprep.FoldingRule(catalog.NameFoldingUnknown),
			want: schemaprep.UnknownFoldingError{Kind: "database", Written: "SALES", Found: "Sales"},
		},
		{
			name: "a SQL Server collation no connection answered", rule: schemaprep.UnansweredCatalogRule(),
			want: schemaprep.UnknownFoldingError{Kind: "schema", Written: "SALES", Found: "Sales", Setting: "the database collation"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := schemaprep.RespellName(test.rule, "SALES", []string{"Sales"})

			var unknown *schemaprep.UnknownFoldingError
			c.Assert(err, qt.ErrorAs, &unknown)
			c.Assert(*unknown, qt.Equals, test.want)
			c.Assert(got, qt.Equals, "")
		})
	}
}

// The refusals say what decides how the server compares names.
func TestNameRefusals_Error(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "a collation no connection answered",
			err:  &schemaprep.UnknownFoldingError{Kind: "schema", Written: "sales", Found: "Sales", Setting: "the database collation"},
			want: `schema "sales" and "Sales" differ only in case, and whether they name one schema depends on ` +
				`the database collation, which this comparison does not know; compare against a database ` +
				`connection, which reads it, or spell both names the same way`,
		},
		{
			name: "two spellings under a collation",
			err:  &schemaprep.AmbiguousNameError{Kind: "schema", Name: "sales", Matches: []string{"Sales", "SALES"}, Reading: "under the database collation"},
			want: `schema "sales" matches "Sales" and "SALES", names the server reads as one under the database ` +
				`collation; no spelling is chosen`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(test.err.Error(), qt.Equals, test.want)
		})
	}
}

// A rule compares names as bytes only where the server keeps them apart.
func TestNameRule_Exact(t *testing.T) {
	tests := []struct {
		name string
		rule schemaprep.NameRule
		want bool
	}{
		{name: "the zero value", rule: schemaprep.NameRule{}, want: true},
		{name: "a server that keeps names apart", rule: schemaprep.FoldingRule(catalog.NameFoldingExact), want: true},
		{name: "a server that folds names", rule: schemaprep.FoldingRule(catalog.NameFoldingFolds), want: false},
		{name: "a server whose folding no connection read", rule: schemaprep.FoldingRule(catalog.NameFoldingUnknown), want: false},
		{name: "a collation a connection answered", rule: schemaprep.CatalogRule(sensitiveKey), want: false},
		{name: "a collation no connection answered", rule: schemaprep.UnansweredCatalogRule(), want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(test.rule.Exact(), qt.Equals, test.want)
		})
	}
}

// A read lists every schema it found and the schema of each relation, and a
// document is listed the same way, a qualified name standing in the schema it
// names.
func TestReadNamesOf(t *testing.T) {
	c := qt.New(t)
	database := &catalog.Database{
		Schemas:   []catalog.Schema{{Name: "Sales"}},
		Tables:    []catalog.Table{{Schema: "Sales", Name: "t"}},
		Views:     []catalog.View{{Schema: "dbo", Name: "v"}},
		Sequences: []catalog.Sequence{{Schema: "Audit", Name: "s"}},
	}

	got := schemaprep.ReadNamesOf(database, "dbo", platform.SQLServer)

	c.Assert(got, qt.DeepEquals, schemaprep.ReadNames{
		Session:   "dbo",
		Dialect:   platform.SQLServer,
		Databases: []string{"Sales", "Sales", "dbo", "Audit"},
		Relations: []schemaprep.ReadRelation{{Database: "Sales", Name: "t"}, {Database: "dbo", Name: "v"}, {Database: "Audit", Name: "s"}},
	})
	c.Assert(schemaprep.ReadNamesOf(nil, "dbo", platform.SQLServer), qt.DeepEquals, schemaprep.ReadNames{Session: "dbo", Dialect: platform.SQLServer})
}

func TestDeclaredNamesOf(t *testing.T) {
	c := qt.New(t)
	database := &schemamodel.Database{
		Schemas:   []schemamodel.Schema{{Name: "Sales"}},
		Tables:    []schemamodel.Table{{Schema: "Sales", Name: "t"}, {Name: "audit.log"}, {Name: "plain"}},
		Views:     []schemamodel.View{{Name: "dbo.v"}},
		Sequences: []schemamodel.Sequence{{Schema: "Audit", Name: "s"}},
	}

	got := schemaprep.DeclaredNamesOf(database, "dbo", platform.SQLServer)

	c.Assert(got, qt.DeepEquals, schemaprep.ReadNames{
		Session:   "dbo",
		Dialect:   platform.SQLServer,
		Databases: []string{"Sales", "Sales", "audit", "dbo", "Audit"},
		Relations: []schemaprep.ReadRelation{
			{Database: "Sales", Name: "t"}, {Database: "audit", Name: "log"}, {Name: "plain"},
			{Database: "dbo", Name: "v"}, {Database: "Audit", Name: "s"},
		},
	})
	c.Assert(schemaprep.DeclaredNamesOf(nil, "dbo", ""), qt.DeepEquals, schemaprep.ReadNames{Session: "dbo"})
}
