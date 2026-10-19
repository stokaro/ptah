package registration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/core/sqlutil"
	"ptah.run/internal/deporder"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
	"ptah.run/internal/lexerext/yql"
	"ptah.run/internal/sqlident"
	"ptah.run/internal/sqlreach"
)

// TestRegister_ReadsTheTextOfAnEngineByTheLexerItNames registers an engine
// whose descriptor names the YQL lexer, which a registered service serves.
// The splitter, the reach scan and the view ordering read its text by that
// lexer's tokens, though nothing names the engine: @@...@@ is one string, so
// the semicolons, the ATTACH and the view name inside it are text. Each is
// asked first, under the shared rules an unknown dialect is read by, so the
// answers derived from the earlier registry exist and have to give way to
// the new one.
func TestRegister_ReadsTheTextOfAnEngineByTheLexerItNames(t *testing.T) {
	const (
		script    = "SELECT @@a;b@@; SELECT 2"
		attach    = "SELECT @@;ATTACH DATABASE 'x' AS y;@@"
		reporting = "SELECT @@z_base@@ AS label FROM users"
	)
	views := []deporder.ViewLike{
		{Name: "a_report", Body: reporting},
		{Name: "z_base", Body: "SELECT id FROM users"},
	}
	c := qt.New(t)
	c.Assert(dialectlexer.Options("ownlexdb"), qt.DeepEquals, lexer.Options{StandardStrings: true})
	name, read := sqlident.ReadName("ownlexdb", "`tick\\`name`")
	c.Assert(read, qt.IsFalse)
	c.Assert(name, qt.Equals, "")
	c.Assert(sqlutil.SplitSQLStatementsForDialect(script, "ownlexdb"), qt.DeepEquals, []string{"SELECT @@a", "b@@", "SELECT 2"})
	finding, found, err := sqlreach.Scan(attach, "ownlexdb")
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(finding.Construct, qt.Equals, "ATTACH")
	c.Assert(viewNames(deporder.ViewLikesForCreateForDialect(views, "ownlexdb")), qt.DeepEquals, []string{"z_base", "a_report"})

	engine := described("ownlexer", "", "ownlexdb")
	engine.Lexical = descriptor.Lexical{Lexer: descriptor.LexerYQL}
	c.Assert(platform.Register(engine), qt.IsNil)

	c.Assert(dialectlexer.Options("ownlexdb"), qt.DeepEquals, lexer.Options{Scanner: yql.Scanner{}})
	// A lexer service does not grant path naming to an engine that states none.
	c.Assert(sqlident.NamesArePaths("ownlexdb"), qt.IsFalse)
	name, read = sqlident.ReadName("ownlexdb", "`tick\\`name`")
	c.Assert(read, qt.IsFalse)
	c.Assert(name, qt.Equals, "")
	name, read = sqlident.ReadName("ydb", "`tick\\`name`")
	c.Assert(read, qt.IsTrue)
	c.Assert(name, qt.Equals, "tick`name")
	c.Assert(sqlutil.SplitSQLStatementsForDialect(script, "ownlexdb"), qt.DeepEquals, []string{"SELECT @@a;b@@", "SELECT 2"})
	finding, found, err = sqlreach.Scan(attach, "ownlexdb")
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsFalse)
	c.Assert(finding, qt.DeepEquals, sqlreach.Finding{})
	c.Assert(viewNames(deporder.ViewLikesForCreateForDialect(views, "ownlexdb")), qt.DeepEquals, []string{"a_report", "z_base"})
}

func viewNames(objects []deporder.ViewLike) []string {
	names := make([]string, 0, len(objects))
	for _, object := range objects {
		names = append(names, object.Name)
	}
	return names
}
