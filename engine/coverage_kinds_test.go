package engine_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/coverage"
	"ptah.run/engine"
)

// TestRuntime_CoverageVocabulary_HappyPath accepts an owner's coverage kind
// only in a runtime that selects the owner, next to the common kinds every
// runtime accepts.
func TestRuntime_CoverageVocabulary_HappyPath(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(engine.New(engine.Provider{ID: "example.org/feeds", CoverageKinds: []coverage.Kind{"changefeed"}}))

	kind, err := runtime.CoverageVocabulary().ParseKind("Changefeed")
	c.Assert(err, qt.IsNil)
	c.Assert(kind, qt.Equals, coverage.Kind("changefeed"))
	common, err := runtime.CoverageVocabulary().ParseKind("sequence")
	c.Assert(err, qt.IsNil)
	c.Assert(common, qt.Equals, coverage.Sequence)
}

// TestRuntime_CoverageVocabulary_FailurePath refuses `changefeed` by name in a
// runtime that does not select the owner that registers it, and refuses a
// registration that two providers make or that names a common kind.
func TestRuntime_CoverageVocabulary_FailurePath(t *testing.T) {
	c := qt.New(t)
	without := must.Must(engine.New(engine.Provider{ID: "example.org/other"}))

	kind, err := without.CoverageVocabulary().ParseKind("changefeed")
	c.Assert(err, qt.ErrorMatches, `unknown coverage kind "changefeed": valid kinds are .*`)
	c.Assert(kind, qt.Equals, coverage.Kind(""))

	twice, err := engine.New(
		engine.Provider{ID: "example.org/first", CoverageKinds: []coverage.Kind{"changefeed"}},
		engine.Provider{ID: "example.org/second", CoverageKinds: []coverage.Kind{"changefeed"}},
	)
	c.Assert(err, qt.ErrorIs, engine.ErrInvalidRegistration)
	c.Assert(err, qt.ErrorMatches, `.*coverage kind "changefeed" is registered twice`)
	c.Assert(twice, qt.IsNil)

	common, err := engine.New(engine.Provider{ID: "example.org/common", CoverageKinds: []coverage.Kind{coverage.Sequence}})
	c.Assert(err, qt.ErrorIs, engine.ErrInvalidRegistration)
	c.Assert(err, qt.ErrorMatches, `.*coverage kind "sequence" is a common kind and cannot be an owner's`)
	c.Assert(common, qt.IsNil)
}

// TestRuntime_CoverageVocabulary_KnowsInProcessKinds knows a provider's
// in-process kind, which no document names, so the vocabulary refuses it by
// name.
func TestRuntime_CoverageVocabulary_KnowsInProcessKinds(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(engine.New(engine.Provider{ID: "example.org/replicas", CoverageKinds: []coverage.Kind{"changefeed"},
		ProcessCoverageKinds: []coverage.Kind{"replica_table"}}))

	c.Assert(runtime.CoverageVocabulary().InProcess("replica_table"), qt.IsTrue)
	c.Assert(runtime.CoverageVocabulary().InProcess("changefeed"), qt.IsFalse)
	kind, err := runtime.CoverageVocabulary().ParseKind("replica_table")
	c.Assert(err, qt.ErrorMatches, `unknown coverage kind "replica_table": valid kinds are .*`)
	c.Assert(kind, qt.Equals, coverage.Kind(""))
}

// TestRuntime_CoverageVocabulary_RefusesAnInProcessKind_FailurePath refuses
// an in-process kind two providers register, and one another provider
// registers as a kind documents name.
func TestRuntime_CoverageVocabulary_RefusesAnInProcessKind_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		providers []engine.Provider
		wantErr   string
	}{
		{name: "twice", providers: []engine.Provider{
			{ID: "example.org/first", ProcessCoverageKinds: []coverage.Kind{"replica_table"}},
			{ID: "example.org/second", ProcessCoverageKinds: []coverage.Kind{"replica_table"}}},
			wantErr: `.*in-process coverage kind "replica_table" is registered twice`},
		{name: "a kind documents name", providers: []engine.Provider{
			{ID: "example.org/first", CoverageKinds: []coverage.Kind{"replica_table"}},
			{ID: "example.org/second", ProcessCoverageKinds: []coverage.Kind{"replica_table"}}},
			wantErr: `.*coverage kind "replica_table" is a kind documents name and cannot be in-process`},
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
