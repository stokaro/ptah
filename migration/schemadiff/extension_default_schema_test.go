package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

func TestCompare_ImplicitExtensionSchemaMatchesPostgreSQLDefault(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{Extensions: []schemamodel.Extension{{Name: "pgcrypto"}}}
	database := &catalog.Database{
		Extensions: []catalog.Extension{{Name: "pgcrypto", Schema: "public"}},
	}

	diff := must.Must(schemadiff.Compare(t.Context(), desired, database, must.Must(builtin.New())))

	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("diff: %#v", diff))
}

func TestCompareWithOptions_ImplicitExtensionSchemaMatchesPostgreSQLDefault(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{Extensions: []schemamodel.Extension{{Name: "pgcrypto"}}}
	database := &catalog.Database{
		Extensions: []catalog.Extension{{Name: "pgcrypto", Schema: "public"}},
	}

	diff := must.Must(schemadiff.CompareWithOptions(t.Context(), desired, database, nil, must.Must(builtin.New())))

	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("diff: %#v", diff))
}
