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
		{name: "object declaration at a facet site",
			providers: []engine.Provider{hostProvider(), statementReader(sqlext.TableOption, []schemaext.Kind{conversionFirst}, "custom")},
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

// facetReader is a feature owner that registers no target: it compares its
// two models as facets on custom, which another provider registers, and reads
// a column option that sets facets of kinds on targets, beside patterns.
func facetReader(kinds []schemaext.Kind, targets []string, patterns ...sqlext.Pattern) engine.Provider {
	provider := facetProvider(facetComparisonFunc(func(context.Context, schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
		return schemaext.FacetComparisonResult{}, nil
	}))
	provider.Targets = nil
	provider.SQL = []sqlext.Extension{{Owner: provider.ID, Targets: targets, Facets: kinds, Parser: noSQL{},
		Patterns: append([]sqlext.Pattern{{Name: "label", Site: sqlext.ColumnOption, Words: []string{"LABELED"}}}, patterns...)}}
	return provider
}

// facetOwner compares the facets the readers name on custom and reads no
// SQL, under an identity of its own.
func facetOwner() engine.Provider {
	provider := facetReader(nil, nil)
	provider.ID = "example.org/facets"
	provider.SQL = nil
	return provider
}

// TestRuntimeSQL_AFeatureOwnerReadsItsFacets pins that an owner reads syntax
// that only sets facets of its own on a target another provider registers,
// where it compares them.
func TestRuntimeSQL_AFeatureOwnerReadsItsFacets(t *testing.T) {
	c := qt.New(t)

	runtime := mustRuntime(c, hostProvider(), facetReader([]schemaext.Kind{conversionFirst}, []string{"custom"}))

	c.Assert(runtime.SQL().Candidates("custom", sqlext.ColumnOption), qt.HasLen, 1)
}

// TestRuntimeSQL_AFeatureOwnerReadsItsIndexOptions pins that an option after
// a key's parts sets facets alone, so a feature owner reads one on a target
// another provider registers as it reads a column option.
func TestRuntimeSQL_AFeatureOwnerReadsItsIndexOptions(t *testing.T) {
	c := qt.New(t)

	runtime := mustRuntime(c, hostProvider(), facetReader([]schemaext.Kind{conversionFirst}, []string{"custom"},
		sqlext.Pattern{Name: "packed", Site: sqlext.IndexOption, Words: []string{"PACKED"}}))

	c.Assert(runtime.SQL().Candidates("custom", sqlext.IndexOption), qt.HasLen, 1)
}

// TestRuntimeSQL_AFacetOwnerReadsNothingElse pins what an owner that does not
// register a target may not read there through facets: a kind it does not
// compare on that target, a kind whose codec it does not own, a kind another
// owner compares there, and a pattern beside its facets that is not at a facet
// site.
func TestRuntimeSQL_AFacetOwnerReadsNothingElse(t *testing.T) {
	reader := engine.Provider{ID: "example.org/reader", SQL: []sqlext.Extension{{
		Owner: "example.org/reader", Targets: []string{"custom"}, Facets: []schemaext.Kind{conversionFirst}, Parser: noSQL{},
		Patterns: []sqlext.Pattern{{Name: "label", Site: sqlext.ColumnOption, Words: []string{"LABELED"}}}}}}
	tests := []struct {
		name      string
		providers []engine.Provider
		wantErr   string
	}{
		{name: "a kind it does not compare there",
			providers: []engine.Provider{hostProvider(), facetReader([]schemaext.Kind{conversionFirst}, []string{"elsewhere"})},
			wantErr:   `.*provider "example.org/converter" registers SQL syntax for target "elsewhere" it does not register`},
		{name: "a kind it does not own",
			providers: []engine.Provider{hostProvider(), facetReader([]schemaext.Kind{"example.org/other/label"}, []string{"custom"})},
			wantErr:   `.*provider "example.org/converter" registers SQL syntax for target "custom" it does not register`},
		{name: "a kind another owner compares there", providers: []engine.Provider{hostProvider(), facetOwner(), reader},
			wantErr: `.*provider "example.org/reader" registers SQL syntax for target "custom" it does not register`},
		{name: "a pattern at another site", providers: []engine.Provider{hostProvider(), facetReader([]schemaext.Kind{conversionFirst}, []string{"custom"},
			sqlext.Pattern{Name: "bucket", Site: sqlext.TableElement, Words: []string{"BUCKET"}})},
			wantErr: `.*provider "example.org/converter" registers SQL syntax for target "custom" it does not register`},
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

func TestRuntimeSQL_AFeatureOwnerReadsItsTableChildren(t *testing.T) {
	for _, site := range []sqlext.Site{sqlext.TableElement, sqlext.AlterOperation} {
		t.Run(string(site), func(t *testing.T) {
			c := qt.New(t)
			runtime := mustRuntime(c, hostProvider(), statementReader(site, []schemaext.Kind{conversionFirst}, "custom"))
			c.Assert(runtime.SQL().Candidates("custom", site), qt.HasLen, 1)
		})
	}
}
