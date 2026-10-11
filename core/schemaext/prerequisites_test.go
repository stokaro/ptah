package schemaext_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
)

// installation is a change that installs the object it names, which provides
// hypertables when it is timescaledb.
type installation struct{}

func (*installation) Kind() schemaext.Kind                 { return "example.org/installation" }
func (v *installation) CloneChange() schemaext.ChangeValue { return &installation{} }
func (*installation) ProvidedCapabilities(subject objectidentity.ID, caps capability.Capabilities) capability.Capabilities {
	return caps.With(capability.Hypertables, subject.Name.Source == "timescaledb")
}

// silentChange provides nothing.
type silentChange struct{}

func (*silentChange) Kind() schemaext.Kind                 { return "example.org/silent" }
func (v *silentChange) CloneChange() schemaext.ChangeValue { return &silentChange{} }

func named(name string) objectidentity.ID {
	return objectidentity.ID{Kind: "example.org/object", Name: objectidentity.Part{Source: name, Normalized: name}}
}

func TestProvidedByChanges(t *testing.T) {
	tests := []struct {
		name    string
		changes []schemaext.ChangeRecord
		want    bool
	}{
		{name: "a provider among other changes", want: true, changes: []schemaext.ChangeRecord{
			{Subject: named("other"), Value: &silentChange{}}, {Subject: named("timescaledb"), Value: &installation{}}}},
		{name: "a provider whose subject provides nothing", changes: []schemaext.ChangeRecord{{Subject: named("pg_trgm"), Value: &installation{}}}},
		{name: "no provider", changes: []schemaext.ChangeRecord{{Subject: named("timescaledb"), Value: &silentChange{}}}},
		{name: "no changes"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			caps := capability.Postgres17().With(capability.Hypertables, false)

			widened := schemaext.ProvidedByChanges(caps, test.changes)

			c.Assert(widened.Has(capability.Hypertables), qt.Equals, test.want)
			c.Assert(caps.Has(capability.Hypertables), qt.IsFalse)
		})
	}
}
