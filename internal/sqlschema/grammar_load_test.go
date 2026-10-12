package sqlschema_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/coverage"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/sqlext"
	"ptah.run/engine"
	"ptah.run/internal/sqlschema"
)

// wholeGrammar is an owner of target "custom" that reads every document of a
// load itself. It declares one table per document, named by the answer it is
// configured with, and records the requests and the header limits it is
// given.
type wholeGrammar struct {
	requests *[]sqlext.GrammarRequest
	// answer returns the declarations the owner answers for the request.
	answer func(sqlext.GrammarRequest) []sqlext.FileDeclarations
	// objects are the owner's final declarations after the whole script.
	objects schemaext.Objects
}

func (g wholeGrammar) ReadSQL(_ context.Context, request sqlext.GrammarRequest) (sqlext.GrammarResult, error) {
	*g.requests = append(*g.requests, request)
	result := sqlext.GrammarResult{Database: schemamodel.Database{FeatureObjects: g.objects}}
	for _, file := range request.Files {
		name := "from_" + file.Path
		result.Database.Tables = append(result.Database.Tables, schemamodel.Table{Name: name, StructName: name})
	}
	result.Files = g.answer(request)
	return result, nil
}

// echoFiles answers one declaration list per requested file, naming it as the
// request did.
func echoFiles(request sqlext.GrammarRequest) []sqlext.FileDeclarations {
	files := make([]sqlext.FileDeclarations, 0, len(request.Files))
	for _, file := range request.Files {
		files = append(files, sqlext.FileDeclarations{Path: file.Path, Objects: []sqlext.Declaration{{Kind: "table", Name: "from_" + file.Path}}})
	}
	return files
}

// grammarRuntime registers target "custom" whose documents grammar reads,
// with "widget" as the header kind the owner reads as a limit on its claim.
// Each claim appends the limits it was handed to limits.
func grammarRuntime(grammar wholeGrammar, limits *[][]coverage.Object) *engine.Runtime {
	return must.Must(engine.New(engine.Provider{
		ID:      "example.org/custom",
		Targets: []engine.Target{{Name: "custom"}},
		SQL: []sqlext.Extension{{
			Owner:   "example.org/custom",
			Targets: []string{"custom"},
			Grammar: grammar,
			Header:  []coverage.Kind{"widget"},
			Coverage: func(request sqlext.CoverageRequest) (schemaext.Coverage, error) {
				*limits = append(*limits, request.Limits)
				return schemaext.NewCoverage(schemaext.Desired, nil, nil)
			},
		}},
	}))
}

// TestReadLoad_ReachesTheTargetsGrammar shows that a load of a target an
// owner's grammar reads reaches that owner through the runtime in one call,
// with every document in order, and that the reader adds each document's
// header: the owner's own kinds go to its claim, one document's at a time, and
// the common kinds a document leaves undescribed are undescribed for the load.
func TestReadLoad_ReachesTheTargetsGrammar(t *testing.T) {
	c := qt.New(t)
	var requests []sqlext.GrammarRequest
	var limits [][]coverage.Object
	runtime := grammarRuntime(wholeGrammar{requests: &requests, answer: echoFiles}, &limits)
	files := []sqlext.SourceFile{
		{Path: "a.sql", Text: "-- ptah:not-described widget \"w1\"\n-- ptah:not-described view\nanything"},
		{Path: "b.sql", Text: "-- ptah:not-described sequence\nanything else"},
	}

	load, err := sqlschema.ReadLoad(t.Context(),
		sqlext.GrammarRequest{Target: "custom", Files: files, DatabasePath: "/root/db"},
		sqlschema.Options{Owners: runtime.SQL()})

	c.Assert(err, qt.IsNil)
	c.Assert(requests, qt.DeepEquals, []sqlext.GrammarRequest{{Target: "custom", Files: files, DatabasePath: "/root/db"}})
	c.Assert(load.Files, qt.DeepEquals, echoFiles(requests[0]))
	c.Assert(tableNames(load.Database), qt.DeepEquals, []string{"from_a.sql", "from_b.sql"})
	c.Assert(limits, qt.DeepEquals, [][]coverage.Object{{{Kind: "widget", Name: "w1"}}, nil})
	c.Assert(load.Database.FeatureCoverage.Representation(), qt.Equals, schemaext.Desired)
	c.Assert(load.Database.NotDescribed.Describes(coverage.View), qt.IsFalse)
	c.Assert(load.Database.NotDescribed.Describes(coverage.Sequence), qt.IsFalse)
	c.Assert(load.Database.NotDescribed.Describes(coverage.Domain), qt.IsTrue)
}

// TestReadWith_ReadsAGrammarTargetAsALoadOfOne reads one document of such a
// target through the same grammar, as a load of one unnamed file, and returns
// no statements: the grammar answers declarations, not syntax.
func TestReadWith_ReadsAGrammarTargetAsALoadOfOne(t *testing.T) {
	c := qt.New(t)
	var requests []sqlext.GrammarRequest
	var limits [][]coverage.Object
	runtime := grammarRuntime(wholeGrammar{requests: &requests, answer: echoFiles}, &limits)

	database, statements, err := sqlschema.ReadWith(t.Context(), []byte("CREATE TABLE t (id INTEGER);"), "custom", nil,
		sqlschema.Options{Owners: runtime.SQL()})

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.IsNil)
	c.Assert(requests, qt.DeepEquals, []sqlext.GrammarRequest{{Target: "custom", Files: []sqlext.SourceFile{{Text: "CREATE TABLE t (id INTEGER);"}}}})
	c.Assert(tableNames(database), qt.DeepEquals, []string{"from_"})
	c.Assert(limits, qt.HasLen, 1)
}

// TestReadWith_WithoutTheGrammarReadsTheSharedGrammar is the control: without
// the owner the shared grammar reads the same document, and declares the table
// the statement names rather than the one the grammar would.
func TestReadWith_WithoutTheGrammarReadsTheSharedGrammar(t *testing.T) {
	c := qt.New(t)

	database, statements, err := sqlschema.ReadWith(t.Context(), []byte("CREATE TABLE t (id INTEGER);"), "custom", nil,
		sqlschema.Options{Owners: sqlext.None()})

	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)
	c.Assert(tableNames(database), qt.DeepEquals, []string{"t"})
}

// TestReadLoad_FailurePath refuses a load the reader cannot vouch for: one no
// grammar reads, one whose owner answered for other files than it was asked
// about, and a document the reader or the owner refuses, which is named.
func TestReadLoad_FailurePath(t *testing.T) {
	answerNone := func(sqlext.GrammarRequest) []sqlext.FileDeclarations { return nil }
	answerOther := func(sqlext.GrammarRequest) []sqlext.FileDeclarations {
		return []sqlext.FileDeclarations{{Path: "b.sql"}}
	}
	one := []sqlext.SourceFile{{Path: "a.sql", Text: "anything"}}
	tests := []struct {
		name    string
		target  string
		answer  func(sqlext.GrammarRequest) []sqlext.FileDeclarations
		files   []sqlext.SourceFile
		wantErr string
	}{
		{name: "a target no grammar reads", target: "postgres", answer: echoFiles, files: one,
			wantErr: `no SQL owner reads postgres documents as a load`},
		{name: "an answer for fewer files", target: "custom", answer: answerNone, files: one,
			wantErr: `the SQL owner answered 0 files for a load of 1`},
		{name: "an answer for another file", target: "custom", answer: answerOther, files: one,
			wantErr: `the SQL owner answered file 1 as "b.sql", not "a.sql"`},
		{name: "a feature coverage header", target: "custom", answer: echoFiles,
			files:   []sqlext.SourceFile{{Path: "a.sql", Text: "-- ptah:feature-coverage {}\n"}},
			wantErr: `a.sql: unsupported feature: SQL schema sources cannot decode a feature coverage header; read the HCL source instead`},
		{name: "a header the reader refuses", target: "custom", answer: echoFiles,
			files:   []sqlext.SourceFile{{Path: "a.sql", Text: "-- ptah:not-described gadget\n"}},
			wantErr: `a.sql: .*gadget.*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var requests []sqlext.GrammarRequest
			var limits [][]coverage.Object
			runtime := grammarRuntime(wholeGrammar{requests: &requests, answer: test.answer}, &limits)

			load, err := sqlschema.ReadLoad(t.Context(), sqlext.GrammarRequest{Target: test.target, Files: test.files},
				sqlschema.Options{Owners: runtime.SQL()})

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(load, qt.DeepEquals, sqlschema.Load{})
		})
	}
}

// TestReadLoad_RefusesAnUnselectedOwnerSet refuses the zero set before
// anything is read.
func TestReadLoad_RefusesAnUnselectedOwnerSet(t *testing.T) {
	c := qt.New(t)

	load, err := sqlschema.ReadLoad(t.Context(), sqlext.GrammarRequest{Target: "ydb"}, sqlschema.Options{})

	c.Assert(err, qt.ErrorIs, sqlext.ErrUnselected)
	c.Assert(load, qt.DeepEquals, sqlschema.Load{})
}

// TestReadWith_RefusesADocumentOfALargerGrammarLoad refuses a document read
// against earlier ones of a target an owner's grammar reads: the owner reads a
// load whole, so such a document is read with the rest of its load.
func TestReadWith_RefusesADocumentOfALargerGrammarLoad(t *testing.T) {
	c := qt.New(t)
	var requests []sqlext.GrammarRequest
	var limits [][]coverage.Object
	runtime := grammarRuntime(wholeGrammar{requests: &requests, answer: echoFiles}, &limits)

	database, statements, err := sqlschema.ReadWith(t.Context(), []byte("anything"), "custom", sqlschema.NewDocument(nil),
		sqlschema.Options{Owners: runtime.SQL()})

	c.Assert(err, qt.ErrorMatches, `a custom document of a larger load is read with the rest of the load, not on its own`)
	c.Assert(statements, qt.IsNil)
	c.Assert(database.Tables, qt.HasLen, 0)
	c.Assert(requests, qt.HasLen, 0)
}

func tableNames(database schemamodel.Database) []string {
	names := make([]string, 0, len(database.Tables))
	for _, table := range database.Tables {
		names = append(names, table.Name)
	}
	return names
}

// TestReadLoad_CoverageUsesTheWholeLoadsDeclarations keeps a grammar's
// final objects available to every file's claim while header limits remain
// specific to the file, for a single file and for a directory.
func TestReadLoad_CoverageUsesTheWholeLoadsDeclarations(t *testing.T) {
	tests := []struct {
		name       string
		files      []sqlext.SourceFile
		wantLimits [][]coverage.Object
	}{
		{name: "one file", files: []sqlext.SourceFile{{Path: "a.sql", Text: "-- ptah:not-described widget \"w1\"\nanything"}},
			wantLimits: [][]coverage.Object{{{Kind: "widget", Name: "w1"}}}},
		{name: "directory", files: []sqlext.SourceFile{{Path: "a.sql", Text: "-- ptah:not-described widget \"w1\"\nanything"},
			{Path: "b.sql", Text: "anything else"}}, wantLimits: [][]coverage.Object{{{Kind: "widget", Name: "w1"}}, nil}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var parses []sqlext.GrammarRequest
			var claims []sqlext.CoverageRequest
			objects := must.Must(schemaext.NewObjects(widgetObject("final", "amended")))
			runtime := must.Must(engine.New(engine.Provider{
				ID: "example.org/custom", Targets: []engine.Target{{Name: "custom"}},
				SQL: []sqlext.Extension{{Owner: "example.org/custom", Targets: []string{"custom"},
					Grammar: wholeGrammar{requests: &parses, answer: echoFiles, objects: objects}, Header: []coverage.Kind{"widget"},
					Coverage: func(request sqlext.CoverageRequest) (schemaext.Coverage, error) {
						claims = append(claims, request)
						return schemaext.NewCoverage(schemaext.Desired, nil, nil)
					},
				}},
			}))

			load, err := sqlschema.ReadLoad(t.Context(), sqlext.GrammarRequest{Target: "custom", Files: test.files},
				sqlschema.Options{Owners: runtime.SQL()})

			c.Assert(err, qt.IsNil)
			c.Assert(claims, qt.HasLen, len(test.files))
			for i, claim := range claims {
				c.Assert(widgetComments(c, claim.Declared), qt.DeepEquals, map[string]string{"final": "amended"})
				c.Assert(claim.Limits, qt.DeepEquals, test.wantLimits[i])
			}
			c.Assert(widgetComments(c, load.Database.FeatureObjects), qt.DeepEquals, map[string]string{"final": "amended"})
		})
	}
}
