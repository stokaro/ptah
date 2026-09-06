package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff"
)

func TestCompare_ImplicitExtensionSchemaMatchesPostgreSQLDefault(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{Extensions: []schemamodel.Extension{{Name: "pgcrypto"}}}
	database := &catalog.Database{
		Extensions: []catalog.Extension{{Name: "pgcrypto", Schema: "public"}},
	}

	diff := schemadiff.Compare(desired, database)

	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("diff: %#v", diff))
}

func TestCompareWithOptions_ImplicitExtensionSchemaMatchesPostgreSQLDefault(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{Extensions: []schemamodel.Extension{{Name: "pgcrypto"}}}
	database := &catalog.Database{
		Extensions: []catalog.Extension{{Name: "pgcrypto", Schema: "public"}},
	}

	diff := schemadiff.CompareWithOptions(desired, database, nil)

	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("diff: %#v", diff))
}
