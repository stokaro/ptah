package devclean_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgextension"
	"ptah.run/feature/pgpublication"
	"ptah.run/internal/devclean"
)

func installed(names ...string) schemaext.Objects {
	var objects []schemaext.Object
	for _, name := range names {
		objects = append(objects, must.Must(pgextension.ObservedObject(pgextension.Ref(name), pgextension.ObservedExtension{Schema: "public", Version: "1.0"})))
	}
	return must.Must(schemaext.NewObjects(objects...))
}

// TestWithoutKept_LeavesOutTheEnvironment pins the environment filter: a
// kept extension or publication the other side does not declare leaves, one
// it declares stays, and one the environment does not name stays.
func TestWithoutKept_LeavesOutTheEnvironment(t *testing.T) {
	c := qt.New(t)
	objects := slices.Concat(must.Must(installed("citext", "uuid-ossp", "pgcrypto").All()), published("realtime", "orders", "audit"))
	current := &catalog.Database{FeatureObjects: must.Must(schemaext.NewObjects(objects...))}

	filtered := devclean.WithoutKept(current,
		devclean.Environment{Extensions: []string{"pg_trgm", "citext", "pgcrypto"}, Publications: []string{"realtime", "audit"}},
		devclean.Environment{Extensions: []string{"pgcrypto"}, Publications: []string{"audit"}})

	c.Assert(filtered.FeatureObjects.Refs(), qt.DeepEquals, []objectidentity.ID{pgextension.Ref("pgcrypto"), pgextension.Ref("uuid-ossp"),
		pgpublication.Ref("audit"), pgpublication.Ref("orders")})
	c.Assert(current.FeatureObjects.Len(), qt.Equals, 6)
}

// TestWithoutKept_KeepsEverythingWithoutAnEnvironment pins that a baseline
// that keeps nothing leaves the read as it is.
func TestWithoutKept_KeepsEverythingWithoutAnEnvironment(t *testing.T) {
	c := qt.New(t)
	current := &catalog.Database{FeatureObjects: must.Must(schemaext.NewObjects(published("realtime")...))}

	c.Assert(devclean.WithoutKept(current, devclean.Environment{}, devclean.Environment{}), qt.Equals, current)
	c.Assert(devclean.WithoutKept(nil, devclean.Environment{Publications: []string{"realtime"}}, devclean.Environment{}), qt.IsNil)
}

// TestDeclaredAndReadEnvironment pins the extensions and publications each
// side holds.
func TestDeclaredAndReadEnvironment(t *testing.T) {
	c := qt.New(t)
	declared := &schemamodel.Database{
		FeatureObjects: must.Must(schemaext.NewObjects(must.Must(pgextension.DesiredObject(pgextension.Ref("citext"),
			pgextension.DesiredExtension{})), must.Must(pgpublication.DesiredObject(pgpublication.Ref("orders"), pgpublication.DesiredPublication{})))),
	}
	read := &catalog.Database{FeatureObjects: must.Must(schemaext.NewObjects(slices.Concat(must.Must(installed("isn").All()), published("realtime"))...))}

	c.Assert(devclean.DeclaredEnvironment(declared), qt.DeepEquals, devclean.Environment{Extensions: []string{"citext"}, Publications: []string{"orders"}})
	c.Assert(devclean.ReadEnvironment(read), qt.DeepEquals, devclean.Environment{Extensions: []string{"isn"}, Publications: []string{"realtime"}})
	c.Assert(devclean.DeclaredEnvironment(nil), qt.DeepEquals, devclean.Environment{})
	c.Assert(devclean.ReadEnvironment(nil), qt.DeepEquals, devclean.Environment{})
}

func published(names ...string) []schemaext.Object {
	var objects []schemaext.Object
	for _, name := range names {
		objects = append(objects, must.Must(pgpublication.ObservedObject(pgpublication.Ref(name), pgpublication.ObservedPublication{Publish: make([]string, 0)})))
	}
	return objects
}

// TestReadExtensions pins the extensions a read holds.
func TestReadExtensions(t *testing.T) {
	c := qt.New(t)
	read := &catalog.Database{FeatureObjects: must.Must(schemaext.NewObjects(slices.Concat(must.Must(installed("isn").All()), published("isn"))...))}

	c.Assert(devclean.ReadExtensions(read), qt.DeepEquals, map[string]bool{"isn": true})
	c.Assert(devclean.ReadExtensions(nil), qt.DeepEquals, make(map[string]bool))
}
