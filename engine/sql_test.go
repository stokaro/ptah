package engine_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/core/sqlext"
	"ptah.run/engine"
)

// noSQL answers every claim with an empty value.
type noSQL struct{}

func (noSQL) ParseSQL(_ context.Context, request sqlext.Request) (sqlext.Result, error) {
	return sqlext.Result{Values: make([]sqlext.Value, len(request.Claims))}, nil
}

func sqlProvider(id, target string, extension sqlext.Extension) engine.Provider {
	return engine.Provider{ID: id, Targets: []engine.Target{{Name: target}}, SQL: []sqlext.Extension{extension}}
}

func bucketExtension(owner string, targets ...string) sqlext.Extension {
	return sqlext.Extension{Owner: owner, Targets: targets, Parser: noSQL{},
		Patterns: []sqlext.Pattern{{Name: "bucket", Site: sqlext.TableElement, Words: []string{"BUCKET"}}}}
}

func TestRuntimeSQL_HappyPath(t *testing.T) {
	c := qt.New(t)
	runtime := mustRuntime(c, sqlProvider("example.org/custom", "custom", bucketExtension("example.org/custom", "custom")))

	set := runtime.SQL()

	c.Assert(set.Selected(), qt.IsTrue)
	c.Assert(set.Candidates("custom", sqlext.TableElement), qt.HasLen, 1)
	c.Assert(mustRuntime(c).SQL().Selected(), qt.IsTrue)
}

func TestRuntimeSQL_FailurePath(t *testing.T) {
	server := comparisonProvider(comparisonFunc(unchangedComparison))
	server.Targets = nil
	tests := []struct {
		name      string
		providers []engine.Provider
		wantErr   string
	}{
		{name: "another owner", providers: []engine.Provider{
			sqlProvider("example.org/custom", "custom", bucketExtension("example.org/other", "custom"))},
			wantErr: `.*provider "example.org/custom" registers SQL syntax owned by "example.org/other"`},
		{name: "a target nobody registers", providers: []engine.Provider{
			sqlProvider("example.org/custom", "custom", bucketExtension("example.org/custom", "elsewhere"))},
			wantErr: `.*provider "example.org/custom" registers SQL syntax for target "elsewhere" it does not register`},
		{name: "another provider's target", providers: []engine.Provider{
			{ID: "example.org/first", Targets: []engine.Target{{Name: "first"}}},
			sqlProvider("example.org/custom", "custom", bucketExtension("example.org/custom", "first"))},
			wantErr: `.*provider "example.org/custom" registers SQL syntax for target "first" it does not register`},
		{name: "two owners of one syntax", providers: []engine.Provider{
			sqlProvider("example.org/first", "first", bucketExtension("example.org/first", "first")),
			sqlProvider("example.org/second", "second", bucketExtension("example.org/second", "second"))},
			wantErr: `.*can match the same table-element syntax`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			runtime, err := engine.New(test.providers...)

			c.Assert(err, qt.ErrorIs, engine.ErrInvalidRegistration)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(runtime, qt.IsNil)
		})
	}
}

// statementReader is a feature owner that registers no target: it compares
// its first model on custom, which another provider registers, and reads
// statements declaring kinds on targets.
func statementReader(site sqlext.Site, kinds []schemaext.Kind, targets ...string) engine.Provider {
	provider := comparisonProvider(comparisonFunc(unchangedComparison))
	provider.Targets = nil
	provider.SQL = []sqlext.Extension{{Owner: provider.ID, Targets: targets, Kinds: kinds, Parser: noSQL{},
		Patterns: []sqlext.Pattern{{Name: "create", Site: site, Words: []string{"CREATE", "FIRST"}}}}}
	return provider
}

// hostProvider registers the targets a feature owner reads statements on.
func hostProvider() engine.Provider {
	return engine.Provider{ID: "example.org/host", Targets: []engine.Target{{Name: "custom"}, {Name: "elsewhere"}}}
}

// TestRuntimeSQL_AFeatureOwnerReadsItsStatements pins that an owner reads
// statements declaring its own objects on a target another provider
// registers, where it serves them.
func TestRuntimeSQL_AFeatureOwnerReadsItsStatements(t *testing.T) {
	c := qt.New(t)

	runtime := mustRuntime(c, hostProvider(), statementReader(sqlext.Statement, []schemaext.Kind{conversionFirst}, "custom"))

	c.Assert(runtime.SQL().Candidates("custom", sqlext.Statement), qt.HasLen, 1)
}

// TestRuntimeSQL_AFeatureOwnerReadsNothingElse pins what an owner that does
// not register a target may not read there: syntax inside another owner's
// statements, statements of no kind, statements of a kind another owner
// serves there, and statements on a target where it serves nothing.
func TestRuntimeSQL_AFeatureOwnerReadsNothingElse(t *testing.T) {
	foreign := engine.Provider{ID: "example.org/reader", SQL: []sqlext.Extension{{Owner: "example.org/reader",
		Targets: []string{"custom"}, Kinds: []schemaext.Kind{conversionFirst}, Parser: noSQL{},
		Patterns: []sqlext.Pattern{{Name: "create", Site: sqlext.Statement, Words: []string{"CREATE", "FIRST"}}}}}}
	server := comparisonProvider(comparisonFunc(unchangedComparison))
	server.Targets = nil
	tests := []struct {
		name      string
		providers []engine.Provider
		wantErr   string
	}{
		{name: "syntax inside a statement",
			providers: []engine.Provider{hostProvider(), statementReader(sqlext.TableElement, []schemaext.Kind{conversionFirst}, "custom")},
			wantErr:   `.*provider "example.org/converter" registers SQL syntax for target "custom" it does not register`},
		{name: "statements of no kind",
			providers: []engine.Provider{hostProvider(), statementReader(sqlext.Statement, nil, "custom")},
			wantErr:   `.*provider "example.org/converter" registers SQL syntax for target "custom" it does not register`},
		{name: "a kind another owner serves",
			providers: []engine.Provider{hostProvider(), server, foreign},
			wantErr:   `.*provider "example.org/reader" registers SQL syntax for target "custom" it does not register`},
		{name: "a target it serves nothing on",
			providers: []engine.Provider{hostProvider(), statementReader(sqlext.Statement, []schemaext.Kind{conversionFirst}, "elsewhere")},
			wantErr:   `.*provider "example.org/converter" registers SQL syntax for target "elsewhere" it does not register`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			runtime, err := engine.New(test.providers...)

			c.Assert(err, qt.ErrorIs, engine.ErrInvalidRegistration)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(runtime, qt.IsNil)
		})
	}
}
