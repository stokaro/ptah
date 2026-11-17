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
	"ptah.run/feature/pgpublication"
	"ptah.run/internal/atlassource"
	"ptah.run/internal/devclean"
)

// replayedState is a state read from a dev database that held pg_trgm and
// citext, and the publications realtime and audit, before the replay, and
// that holds a third extension and a third publication the replay created.
// The read holds each as its owner's object, and its declaration view holds
// the same.
func replayedState() atlassource.State {
	read := func(name string) schemaext.Object {
		return must.Must(pgextension.ObservedObject(pgextension.Ref(name), pgextension.ObservedExtension{Schema: "public", Version: "1.0"}))
	}
	declared := func(name string) schemaext.Object {
		return must.Must(pgextension.DesiredObject(pgextension.Ref(name), pgextension.DesiredExtension{Schema: "public", Version: "1.0"}))
	}
	published := func(name string) schemaext.Object {
		return must.Must(pgpublication.ObservedObject(pgpublication.Ref(name), pgpublication.ObservedPublication{Publish: make([]string, 0)}))
	}
	declaredPublication := func(name string) schemaext.Object {
		return must.Must(pgpublication.DesiredObject(pgpublication.Ref(name), pgpublication.DesiredPublication{}))
	}
	return atlassource.State{
		DB: &catalog.Database{FeatureObjects: must.Must(schemaext.NewObjects(read("pg_trgm"), read("citext"), read("isn"),
			published("realtime"), published("audit"), published("orders")))},
		Schema: &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(declared("pg_trgm"), declared("citext"), declared("isn"),
			declaredPublication("realtime"), declaredPublication("audit"), declaredPublication("orders")))},
		Environment: devclean.Environment{Extensions: []string{"pg_trgm", "citext"}, Publications: []string{"realtime", "audit"}},
	}
}

// TestState_WithoutEnvironmentLeavesOutTheOwnersEnvironment pins the
// environment of a replayed state held as the owners' objects: an extension
// or a publication the dev database held before the replay leaves both views
// unless the other side declares it, and one the replay created stays. An
// extension and a publication of one name are two objects.
func TestState_WithoutEnvironmentLeavesOutTheOwnersEnvironment(t *testing.T) {
	c := qt.New(t)
	state := replayedState()

	filtered := state.WithoutEnvironment(devclean.Environment{Extensions: []string{"citext", "audit"}, Publications: []string{"realtime"}})

	want := []objectidentity.ID{pgextension.Ref("citext"), pgextension.Ref("isn"), pgpublication.Ref("orders"), pgpublication.Ref("realtime")}
	c.Assert(filtered.DB.FeatureObjects.Refs(), qt.DeepEquals, want)
	c.Assert(filtered.Schema.FeatureObjects.Refs(), qt.DeepEquals, want)
	c.Assert(state.Schema.FeatureObjects.Len(), qt.Equals, 6)
	c.Assert(state.Holds(), qt.DeepEquals, devclean.Environment{Extensions: []string{"citext", "isn", "pg_trgm"},
		Publications: []string{"audit", "orders", "realtime"}})
}
