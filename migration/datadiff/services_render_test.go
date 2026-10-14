package datadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/dataext"
	"ptah.run/migration/datadiff"
)

// TestRenderStatements_FailurePath_OwnerServicesMissing pins the refusal of a
// dialect whose owner writes some of its literals when the caller passes no
// data services: rendering every value from its Go type alone would send YDB
// literals it refuses and Oracle text it reads through a session format.
func TestRenderStatements_FailurePath_OwnerServicesMissing(t *testing.T) {
	for _, dialect := range []string{"ydb", "ydbs", "oracle"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			diff := &datadiff.DataDiff{
				Table:       "events",
				Keys:        []string{"id"},
				ColumnTypes: map[string]string{"id": "Int64", "seen_at": "TIMESTAMP"},
				Inserts:     []datadiff.Row{{"id": 1, "seen_at": "2024-03-01 12:30:45"}},
			}

			up, down, err := datadiff.RenderStatements(diff, dialect, dataext.Services{})

			c.Assert(err, qt.ErrorMatches, `datadiff: (ydb|oracle) writes literals through its owner's data services, `+
				`and the caller gave none; pass the services engine.Runtime.DataServices returns for the dialect`)
			c.Assert(up, qt.IsNil)
			c.Assert(down, qt.IsNil)
		})
	}
}

// TestRenderStatements_HappyPath_EmptyDiffNeedsNoServices pins that the
// refusal waits for a statement to write: an empty diff renders nothing on
// every dialect, as it always has.
func TestRenderStatements_HappyPath_EmptyDiffNeedsNoServices(t *testing.T) {
	c := qt.New(t)

	up, down, err := datadiff.RenderStatements(&datadiff.DataDiff{Table: "events"}, "ydb", dataext.Services{})

	c.Assert(err, qt.IsNil)
	c.Assert(up, qt.IsNil)
	c.Assert(down, qt.IsNil)
}
