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

// TestWithoutKeptExtensions_LeavesOutTheOwnersEnvironmentToo pins the
// environment filter over both models: a kept extension the other side does
// not declare leaves, from the common list and from the owner's objects, and
// one it declares stays.
func TestWithoutKeptExtensions_LeavesOutTheOwnersEnvironmentToo(t *testing.T) {
	c := qt.New(t)
	current := &catalog.Database{
		Extensions:     []catalog.Extension{{Name: "pg_trgm"}, {Name: "hstore"}},
		FeatureObjects: installed("citext", "uuid-ossp", "pgcrypto"),
	}

	filtered := devclean.WithoutKeptExtensions(current, []string{"pg_trgm", "citext", "pgcrypto"}, map[string]bool{"pgcrypto": true})

	c.Assert(filtered.Extensions, qt.DeepEquals, []catalog.Extension{{Name: "hstore"}})
	c.Assert(filtered.FeatureObjects.Refs(), qt.DeepEquals, []objectidentity.ID{pgextension.Ref("pgcrypto"), pgextension.Ref("uuid-ossp")})
	c.Assert(current.FeatureObjects.Len(), qt.Equals, 3)
	c.Assert(current.Extensions, qt.HasLen, 2)
}

// TestDeclaredAndReadExtensions pins the names each side holds, from both
// models.
func TestDeclaredAndReadExtensions(t *testing.T) {
	c := qt.New(t)
	declared := &schemamodel.Database{
		Extensions: []schemamodel.Extension{{Name: "pg_trgm"}},
		FeatureObjects: must.Must(schemaext.NewObjects(must.Must(pgextension.DesiredObject(pgextension.Ref("citext"),
			pgextension.DesiredExtension{})))),
	}
	read := &catalog.Database{Extensions: []catalog.Extension{{Name: "hstore"}}, FeatureObjects: installed("isn")}

	c.Assert(devclean.DeclaredExtensions(declared), qt.DeepEquals, map[string]bool{"pg_trgm": true, "citext": true})
	c.Assert(devclean.ReadExtensions(read), qt.DeepEquals, map[string]bool{"hstore": true, "isn": true})
	c.Assert(devclean.DeclaredExtensions(nil), qt.DeepEquals, make(map[string]bool))
	c.Assert(devclean.ReadExtensions(nil), qt.DeepEquals, make(map[string]bool))
}
