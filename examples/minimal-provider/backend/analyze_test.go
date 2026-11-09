package backend_test

import (
	"testing"

	"example.com/ptah-tutorial-provider/backend"
	qt "github.com/frankban/quicktest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

func TestAnalyzeSQL_HappyPath(t *testing.T) {
	c := qt.New(t)
	source := "create table \"notes\" (\"id\" INTEGER NOT NULL PRIMARY KEY);\n\nALTER TABLE \"notes\" ADD COLUMN \"body\" TEXT;\n"
	request := &protocol.AnalyzeSQLRequest{Engine: backend.Engine, Sources: []string{source}}
	result, err := (&backend.Provider{}).AnalyzeSQL(c.Context(), request)
	c.Assert(err, qt.IsNil)
	// The SDK refuses a split with a gap, an overlap or a wrong line number.
	c.Assert(provider.ValidateSQLAnalysis(request, result), qt.IsNil)
	statements := result.GetFiles()[0].GetStatements()
	c.Assert(statements, qt.HasLen, 2)
	c.Assert(statements[0].GetCanonical(), qt.Equals, `CREATE TABLE "notes" ( "id" INTEGER NOT NULL PRIMARY KEY )`)
	c.Assert(statements[0].GetSourceWords()[0], qt.Equals, "create")
	c.Assert(statements[1].GetLine(), qt.Equals, uint32(3))
	c.Assert(statements[1].GetSafety(), qt.Equals, protocol.SQLStatement_COMMON_DDL)
}

func TestAnalyzeSQL_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name, source, reason string
	}{
		{"a comment", "-- notes\nDROP TABLE \"notes\";", "the tutorial engine's SQL has no comments, literals or operators"},
		{"a statement the engine does not run", "DELETE FROM \"notes\";", "the tutorial engine runs CREATE TABLE, DROP TABLE and ALTER TABLE ADD COLUMN or DROP COLUMN only"},
		{"an unclosed name", "DROP TABLE \"notes;", "a quoted name is not closed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := &protocol.AnalyzeSQLRequest{Engine: backend.Engine, Sources: []string{test.source}}
			result, err := (&backend.Provider{}).AnalyzeSQL(c.Context(), request)
			c.Assert(status.Code(err), qt.Equals, codes.InvalidArgument)
			c.Assert(status.Convert(err).Message(), qt.Equals, test.reason)
			c.Assert(result, qt.IsNil)
		})
	}
}
