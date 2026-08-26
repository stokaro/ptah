package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/migration/schemadiff"
)

func TestCompare_ImplicitExtensionSchemaMatchesPostgreSQLDefault(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{Extensions: []schemamodel.Extension{{Name: "pgcrypto"}}}
	current := &catalog.Database{
		Extensions: []catalog.Extension{{Name: "pgcrypto", Schema: "public"}},
	}

	diff := schemadiff.Compare(desired, current)

	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("diff: %#v", diff))
}

func TestCompareWithOptions_ImplicitExtensionSchemaMatchesPostgreSQLDefault(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{Extensions: []schemamodel.Extension{{Name: "pgcrypto"}}}
	current := &catalog.Database{
		Extensions: []catalog.Extension{{Name: "pgcrypto", Schema: "public"}},
	}

	diff := schemadiff.CompareWithOptions(desired, current, nil)

	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("diff: %#v", diff))
}
