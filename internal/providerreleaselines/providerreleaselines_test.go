package providerreleaselines_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/internal/providerreleaselines"
)

func TestYDB_HappyPath(t *testing.T) {
	c := qt.New(t)

	document, err := providerreleaselines.YDB()

	c.Assert(err, qt.IsNil)
	c.Assert(document.Engines, qt.HasLen, 1)
	c.Assert(document.Engines[0].ID, qt.Equals, "ydb")
	c.Assert(document.Engines[0].Lines, qt.HasLen, 6)
}

func TestSupportLevel_HappyPath(t *testing.T) {
	for name, want := range map[string]capability.SupportLevel{
		"certified":          capability.Certified,
		"legacy_tested":      capability.LegacyTested,
		"best_effort":        capability.BestEffort,
		"known_incompatible": capability.KnownIncompatible,
	} {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			got, err := providerreleaselines.SupportLevel(name)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, want)
		})
	}
}

func TestSupportLevel_FailurePath(t *testing.T) {
	c := qt.New(t)

	got, err := providerreleaselines.SupportLevel("best-effort")

	c.Assert(err, qt.ErrorMatches, `unknown release line support level "best-effort"`)
	c.Assert(string(got), qt.Equals, "")
}
