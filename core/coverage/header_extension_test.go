package coverage_test

import (
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/coverage"
)

func TestHeaderRoutesOwnedKindsWithoutAddingCommonCoverage(t *testing.T) {
	c := qt.New(t)
	var owned []coverage.Object
	got, err := coverage.DecodeHeader(`-- ptah:not-described schema "app"
-- ptah:not-described NATIVE_KIND reason=not-inspected provenance=observed "a.b"
CREATE TABLE example (id int);
-- ptah:not-described native_kind "inside SQL"
`, coverage.Vocabulary{}, func(object coverage.Object) (bool, error) {
		owned = append(owned, object)
		return object.Kind == "native_kind", nil
	})
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, coverage.Set{}.With(coverage.Object{Kind: coverage.Schema, Name: "app"}))
	c.Assert(owned, qt.DeepEquals, []coverage.Object{{Kind: "native_kind", Name: "a.b", Reason: coverage.NotInspected, Provenance: coverage.Observed}})
}

func TestHeaderRefusesUnknownOwnedKindsAndMalformedAttributes(t *testing.T) {
	for _, test := range []struct {
		name, document, want string
		calls                int
	}{
		{"unclaimed", "-- ptah:not-described unknown_kind", `unknown coverage kind "unknown_kind": .*`, 1},
		{"attribute", "-- ptah:not-described native_kind unsupported=yes", `unknown ptah:not-described attribute "unsupported": .*`, 0},
		{"duplicate reason", "-- ptah:not-described native_kind reason=not-inspected reason=unsupported", `malformed ptah:not-described directive .*: reason given twice`, 0},
		{"empty name", `-- ptah:not-described native_kind ""`, `malformed ptah:not-described directive .*: name must not be empty`, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			calls := 0
			got, err := coverage.DecodeHeader(test.document, coverage.Vocabulary{}, func(object coverage.Object) (bool, error) {
				calls++
				return object.Kind == "native_kind", nil
			})
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(got, qt.DeepEquals, coverage.Set{})
			c.Assert(calls, qt.Equals, test.calls)
		})
	}
}

func TestHeaderReturnsOwnerFailureWithoutCommonPrefix(t *testing.T) {
	c := qt.New(t)
	failure := errors.New("owner refused the source limit")
	got, err := coverage.DecodeHeader("-- ptah:not-described schema\n-- ptah:not-described native_kind", coverage.Vocabulary{}, func(coverage.Object) (bool, error) {
		return false, failure
	})
	c.Assert(err, qt.ErrorIs, failure)
	c.Assert(got, qt.DeepEquals, coverage.Set{})
}
