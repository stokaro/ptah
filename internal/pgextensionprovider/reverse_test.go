package pgextensionprovider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgextension"
	"ptah.run/feature/pgextension/extensionreverse"
)

const versionLimit = "PostgreSQL updates an extension in one direction only: after the rollback, pg_trgm stays at version 1.6 rather than 1.5."

// TestReverseExtensions pins each inverse: an installation drops, a drop
// installs again as the extension was, and a surviving extension goes back
// where it can. A version stays, with a limitation saying so, and an update
// that changed nothing else has no statement at all.
func TestReverseExtensions(t *testing.T) {
	commented := &pgextension.ObservedExtension{Schema: "public", Version: "1.5", Relocatable: true, Comment: new("old")}
	tests := []struct {
		name     string
		change   *pgextension.Change
		want     *pgextension.Change
		strategy string
		limits   []string
		forward  schemaext.Value
	}{
		{
			name:     "an installation",
			change:   &pgextension.Change{After: &pgextension.DesiredExtension{Schema: "ext", Version: "1.6", Comment: "trigrams", IfNotExists: true}},
			want:     &pgextension.Change{Before: &pgextension.ObservedExtension{Schema: "ext", Version: "1.6", Comment: new("trigrams")}},
			strategy: "drop the extension the forward plan installed",
			forward:  &pgextension.ObservedExtension{Schema: "ext", Version: "1.6", Comment: new("trigrams")},
		},
		{
			name:     "a drop",
			change:   &pgextension.Change{Before: commented},
			want:     &pgextension.Change{After: &pgextension.DesiredExtension{Schema: "public", Version: "1.5", Comment: "old", IfNotExists: true}},
			strategy: "install the extension again at the version and in the schema it had",
		},
		{
			name:   "a move and a comment",
			change: &pgextension.Change{Before: commented, After: &pgextension.DesiredExtension{Schema: "ext", Comment: "new"}, Schema: "ext", Comment: true},
			want: &pgextension.Change{Before: &pgextension.ObservedExtension{Schema: "ext", Version: "1.5", Relocatable: true, Comment: new("new")},
				After: &pgextension.DesiredExtension{Schema: "public", Comment: "old"}, Schema: "public", Comment: true},
			strategy: "move the extension back, restore its comment",
			forward:  &pgextension.ObservedExtension{Schema: "ext", Version: "1.5", Relocatable: true, Comment: new("new")},
		},
		{
			name:   "a version and a move",
			change: &pgextension.Change{Before: installedTrgm(), After: &pgextension.DesiredExtension{Schema: "ext", Version: "1.6"}, Schema: "ext"},
			want: &pgextension.Change{Before: &pgextension.ObservedExtension{Schema: "ext", Version: "1.6", Relocatable: true},
				After: &pgextension.DesiredExtension{Schema: "public"}, Schema: "public"},
			strategy: "move the extension back, keep the version the migration installed",
			limits:   []string{versionLimit},
			forward:  &pgextension.ObservedExtension{Schema: "ext", Version: "1.6", Relocatable: true},
		},
		{
			name:     "a version alone",
			change:   &pgextension.Change{Before: installedTrgm(), After: &pgextension.DesiredExtension{Version: "1.6"}},
			strategy: "keep the version the migration installed",
			limits:   []string{versionLimit},
			forward:  &pgextension.ObservedExtension{Schema: "public", Version: "1.6", Relocatable: true},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			reversed, err := newRuntime(c).ReverseChanges(t.Context(), schemaext.ReversalRequest{Target: "postgres", Capabilities: capability.Postgres17(),
				Changes: []schemaext.ChangeRecord{change("pg_trgm", test.change)}})

			c.Assert(err, qt.IsNil)
			c.Assert(reversed, qt.HasLen, 1)
			c.Assert(reversed[0].Change.Subject, qt.DeepEquals, pgextension.Ref("pg_trgm"))
			c.Assert(reversed[0].Change.Value, qt.DeepEquals, wantValue(test.want))
			c.Assert(reversed[0].Strategy, qt.Equals, test.strategy)
			c.Assert(reversed[0].Limitations, qt.DeepEquals, test.limits)
			c.Assert(reversed[0].ForwardState, qt.DeepEquals, []schemaext.ProjectedValue{
				{Placement: schemaext.ObjectPlacement, Kind: pgextension.ExtensionKind, Value: test.forward}})
		})
	}
}

// wantValue is the reversal's change value: a nil change stands for no
// statement, which the reversal spells as a nil value.
func wantValue(change *pgextension.Change) schemaext.ChangeValue {
	values := map[bool]schemaext.ChangeValue{true: nil, false: change}
	return values[change == nil]
}

func TestReverseExtensions_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		request schemaext.ReversalRequest
		is      error
	}{
		{name: "a target outside the family", is: ptaherr.ErrUnsupportedDialect,
			request: schemaext.ReversalRequest{Target: "mysql", Changes: []schemaext.ChangeRecord{change("pg_trgm", &pgextension.Change{Before: installedTrgm()})}}},
		{name: "an invalid change", is: schemaext.ErrInvalidValue,
			request: schemaext.ReversalRequest{Target: "postgres", Changes: []schemaext.ChangeRecord{change("pg_trgm", &pgextension.Change{})}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			reversed, err := extensionreverse.Service{}.ReverseChanges(t.Context(), test.request)

			c.Assert(err, qt.ErrorIs, test.is)
			c.Assert(reversed, qt.IsNil)
		})
	}
}

// TestReportExtensions counts each extension once in either representation.
func TestReportExtensions(t *testing.T) {
	tests := []struct {
		name           string
		representation schemaext.Representation
		values         []schemaext.Value
	}{
		{name: "declarations", representation: schemaext.Desired, values: []schemaext.Value{&pgextension.DesiredExtension{}, &pgextension.DesiredExtension{Schema: "ext"}}},
		{name: "observations", representation: schemaext.Observed, values: []schemaext.Value{installedTrgm(), installedTrgm()}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			report, err := newRuntime(c).ReportFeatures(t.Context(), schemaext.ReportingRequest{Target: "postgres", Representation: test.representation, Values: test.values})

			c.Assert(err, qt.IsNil)
			c.Assert(report.Values, qt.HasLen, 2)
			c.Assert(report.Values[0].Counts, qt.DeepEquals, []schemaext.MetricCount{{Name: "extensions", Value: 1}})
		})
	}
}
