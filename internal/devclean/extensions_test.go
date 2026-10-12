package devclean_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgextension"
	"ptah.run/internal/devclean"
)

func installed(names ...string) schemaext.Objects {
	var objects []schemaext.Object
	for _, name := range names {
		objects = append(objects, must.Must(pgextension.ObservedObject(pgextension.Ref(name), pgextension.ObservedExtension{Schema: "public", Version: "1.0"})))
	}
	return must.Must(schemaext.NewObjects(objects...))
}

// TestWithoutKeptExtensions_LeavesOutTheEnvironment pins the environment
// filter: a kept extension the other side does not declare leaves, and one it
// declares stays.
func TestWithoutKeptExtensions_LeavesOutTheEnvironment(t *testing.T) {
	c := qt.New(t)
	current := &catalog.Database{FeatureObjects: installed("citext", "uuid-ossp", "pgcrypto")}

	filtered := devclean.WithoutKeptExtensions(current, []string{"pg_trgm", "citext", "pgcrypto"}, map[string]bool{"pgcrypto": true})

	c.Assert(filtered.FeatureObjects.Refs(), qt.DeepEquals, []objectidentity.ID{pgextension.Ref("pgcrypto"), pgextension.Ref("uuid-ossp")})
	c.Assert(current.FeatureObjects.Len(), qt.Equals, 3)
}

// TestDeclaredAndReadExtensions pins the names each side holds.
func TestDeclaredAndReadExtensions(t *testing.T) {
	c := qt.New(t)
	declared := &schemamodel.Database{
		FeatureObjects: must.Must(schemaext.NewObjects(must.Must(pgextension.DesiredObject(pgextension.Ref("citext"),
			pgextension.DesiredExtension{})))),
	}
	read := &catalog.Database{FeatureObjects: installed("isn")}

	c.Assert(devclean.DeclaredExtensions(declared), qt.DeepEquals, map[string]bool{"citext": true})
	c.Assert(devclean.ReadExtensions(read), qt.DeepEquals, map[string]bool{"isn": true})
	c.Assert(devclean.DeclaredExtensions(nil), qt.DeepEquals, make(map[string]bool))
	c.Assert(devclean.ReadExtensions(nil), qt.DeepEquals, make(map[string]bool))
}
