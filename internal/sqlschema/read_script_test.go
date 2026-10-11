package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/sqlext"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/sqlschema"
)

// TestReadScript_ReadsTheLoadAsOneScript reads the documents of a load in
// order, each against what the earlier ones declared: the second document's
// ALTER TABLE adds a changefeed to the table the first one created, and the
// sequence it names resolves against the request's database root. Each file's
// declarations are its own.
func TestReadScript_ReadsTheLoadAsOneScript(t *testing.T) {
	c := qt.New(t)
	request := sqlext.GrammarRequest{Target: "ydb", DatabasePath: "/Root/db", Files: []sqlext.SourceFile{
		{Path: "001.sql", Text: yqlSerialTable},
		{Path: "002.sql", Text: "ALTER TABLE `app/orders` ADD CHANGEFEED updates WITH (mode = 'UPDATES', format = 'JSON');" +
			"ALTER SEQUENCE `/Root/db/app/orders/_serial_column_id` START WITH 100;"},
	}}

	result, err := sqlschema.ReadScript(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Database.Tables, qt.HasLen, 1)
	table := result.Database.Tables[0]
	c.Assert(must.Must(ydbschema.DesiredChangefeeds(result.Database.FeatureObjects, table.Schema, table.Name)), qt.DeepEquals,
		[]ydbschema.ChangefeedSpec{{Name: "updates", Mode: "UPDATES", Format: "JSON"}})
	c.Assert(result.Database.Fields[0].IdentityStart, qt.Equals, "100")
	c.Assert(result.Database.FeatureCoverage.IsZero(), qt.IsTrue)
	c.Assert(result.Files, qt.HasLen, 2)
	c.Assert(result.Files[0].Path, qt.Equals, "001.sql")
	c.Assert(result.Files[0].Objects, qt.DeepEquals, []sqlext.Declaration{{Kind: "table", Name: "app.orders"}})
	c.Assert(result.Files[0].Tables, qt.DeepEquals, []sqlext.TableStatement{{Name: "`app/orders`"}})
	c.Assert(result.Files[1].Path, qt.Equals, "002.sql")
	c.Assert(result.Files[1].Objects, qt.HasLen, 0)
	c.Assert(result.Files[1].Tables, qt.HasLen, 0)
}

// TestReadScript_ReadsADocumentAlone reads a load of one document as that
// document, without the merge a larger load takes.
func TestReadScript_ReadsADocumentAlone(t *testing.T) {
	c := qt.New(t)
	alone, _, err := sqlschema.Read([]byte(yqlSerialTable), "ydb")
	c.Assert(err, qt.IsNil)

	result, err := sqlschema.ReadScript(t.Context(),
		sqlext.GrammarRequest{Target: "ydb", Files: []sqlext.SourceFile{{Path: "schema.sql", Text: yqlSerialTable}}})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Database, qt.DeepEquals, alone)
	c.Assert(result.Files, qt.HasLen, 1)
}

// TestReadScript_FailurePath names the refused document, and marks a change
// to an object no document of the load declares as such, keeping the
// refusal's text and its kind.
func TestReadScript_FailurePath(t *testing.T) {
	c := qt.New(t)
	request := sqlext.GrammarRequest{Target: "ydb", Files: []sqlext.SourceFile{
		{Path: "001.sql", Text: yqlSerialTable},
		{Path: "002.sql", Text: "ALTER TABLE users ADD CHANGEFEED updates WITH (mode = 'UPDATES', format = 'JSON');"},
	}}

	result, err := sqlschema.ReadScript(t.Context(), request)

	c.Assert(err, qt.ErrorMatches,
		`002.sql: the schema model has no place for this statement: ALTER TABLE users ADD CHANGEFEED updates names a table this schema does not declare`)
	c.Assert(err, qt.ErrorIs, sqlext.ErrUndeclared)
	c.Assert(err, qt.ErrorIs, sqlschema.ErrUnmodeledStatement)
	c.Assert(result, qt.DeepEquals, sqlext.GrammarResult{})
}
