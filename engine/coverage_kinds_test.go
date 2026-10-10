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
