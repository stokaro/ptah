package pgextension_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgextension"
)

// TestPlacementSchema pins the schema each value and change puts its
// extension into, which the host creates first.
func TestPlacementSchema(t *testing.T) {
	installed := &pgextension.ObservedExtension{Schema: "public", Version: "1.5", Relocatable: true}
	tests := []struct {
		name  string
		value schemaext.SchemaPlacement
		want  string
	}{
		{name: "a declaration naming a schema", value: &pgextension.DesiredExtension{Schema: "ext"}, want: "ext"},
		{name: "a declaration naming none", value: &pgextension.DesiredExtension{}, want: ""},
		{name: "an installation", value: &pgextension.Change{After: &pgextension.DesiredExtension{Schema: "ext"}}, want: "ext"},
		{name: "a move", value: &pgextension.Change{Before: installed, After: &pgextension.DesiredExtension{Schema: "Ext"}, Schema: "ext"}, want: "ext"},
		{name: "an update that stays", value: &pgextension.Change{Before: installed, After: &pgextension.DesiredExtension{Schema: "ext", Version: "1.6"}}, want: ""},
		{name: "a drop", value: &pgextension.Change{Before: installed}, want: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(test.value.PlacementSchema(pgextension.Ref("pg_trgm")), qt.Equals, test.want)
		})
	}
}

// TestProvidedCapabilities pins that installing timescaledb provides
// hypertables, and that nothing else a change does provides anything.
func TestProvidedCapabilities(t *testing.T) {
	installed := &pgextension.ObservedExtension{Schema: "public", Version: "2.14.0"}
	tests := []struct {
		name     string
		provider schemaext.CapabilityProvider
		subject  string
		want     bool
	}{
		{name: "a declared timescaledb", provider: &pgextension.DesiredExtension{}, subject: "timescaledb", want: true},
		{name: "a declared other extension", provider: &pgextension.DesiredExtension{}, subject: "pg_trgm", want: false},
		{name: "an installation of timescaledb", provider: &pgextension.Change{After: &pgextension.DesiredExtension{}}, subject: "timescaledb", want: true},
		{name: "an update of timescaledb", provider: &pgextension.Change{Before: installed, After: &pgextension.DesiredExtension{Version: "2.15.0"}},
			subject: "timescaledb", want: false},
		{name: "a drop of timescaledb", provider: &pgextension.Change{Before: installed}, subject: "timescaledb", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			caps := capability.Postgres17().With(capability.Hypertables, false)

			provided := test.provider.ProvidedCapabilities(pgextension.Ref(test.subject), caps)

			c.Assert(provided.Has(capability.Hypertables), qt.Equals, test.want)
		})
	}
}

// TestDesiredExtension_Observed pins the observation a declaration predicts.
func TestDesiredExtension_Observed(t *testing.T) {
	c := qt.New(t)

	c.Assert((&pgextension.DesiredExtension{Schema: "ext", Version: "1.6", Comment: "trigrams", IfNotExists: true, Provides: []string{"similarity"}}).Observed(),
		qt.DeepEquals, &pgextension.ObservedExtension{Schema: "ext", Version: "1.6", Comment: new("trigrams"), Provides: []string{"similarity"}})
	c.Assert((&pgextension.DesiredExtension{}).Observed(), qt.DeepEquals, &pgextension.ObservedExtension{})
	c.Assert((*pgextension.DesiredExtension)(nil).Observed(), qt.IsNil)
}
