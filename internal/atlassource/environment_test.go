package atlassource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgextension"
	"ptah.run/internal/atlassource"
)

// replayedState is a state read from a dev database that held pg_trgm and
// citext before the replay, and that holds a third extension the replay
// created. The read holds each as the extension owner's object, and its
// declaration view holds the same.
func replayedState() atlassource.State {
	read := func(name string) schemaext.Object {
		return must.Must(pgextension.ObservedObject(pgextension.Ref(name), pgextension.ObservedExtension{Schema: "public", Version: "1.0"}))
	}
	declared := func(name string) schemaext.Object {
		return must.Must(pgextension.DesiredObject(pgextension.Ref(name), pgextension.DesiredExtension{Schema: "public", Version: "1.0"}))
	}
	return atlassource.State{
		DB:                    &catalog.Database{FeatureObjects: must.Must(schemaext.NewObjects(read("pg_trgm"), read("citext"), read("isn")))},
		Schema:                &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(declared("pg_trgm"), declared("citext"), declared("isn")))},
		EnvironmentExtensions: []string{"pg_trgm", "citext"},
	}
}

// TestState_WithoutEnvironmentLeavesOutTheOwnersEnvironment pins the
// environment of a replayed state held as the extension owner's objects: an
// extension the dev database held before the replay leaves both views unless
// the other side declares it, and one the replay created stays.
func TestState_WithoutEnvironmentLeavesOutTheOwnersEnvironment(t *testing.T) {
	c := qt.New(t)
	state := replayedState()

	filtered := state.WithoutEnvironment(map[string]bool{"citext": true})

	want := []objectidentity.ID{pgextension.Ref("citext"), pgextension.Ref("isn")}
	c.Assert(filtered.DB.FeatureObjects.Refs(), qt.DeepEquals, want)
	c.Assert(filtered.Schema.FeatureObjects.Refs(), qt.DeepEquals, want)
	c.Assert(state.Schema.FeatureObjects.Len(), qt.Equals, 3)
	c.Assert(state.ExtensionNames(), qt.DeepEquals, map[string]bool{"pg_trgm": true, "citext": true, "isn": true})
}
