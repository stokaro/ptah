package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/schemadiff"
)

func TestCompare_ImplicitExtensionSchemaMatchesPostgreSQLDefault(t *testing.T) {
	c := qt.New(t)
	generated := &goschema.Database{Extensions: []goschema.Extension{{Name: "pgcrypto"}}}
	database := &dbschematypes.DBSchema{
		Extensions: []dbschematypes.DBExtension{{Name: "pgcrypto", Schema: "public"}},
	}

	diff := schemadiff.Compare(generated, database)

	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("diff: %#v", diff))
}

func TestCompareWithOptions_ImplicitExtensionSchemaMatchesPostgreSQLDefault(t *testing.T) {
	c := qt.New(t)
	generated := &goschema.Database{Extensions: []goschema.Extension{{Name: "pgcrypto"}}}
	database := &dbschematypes.DBSchema{
		Extensions: []dbschematypes.DBExtension{{Name: "pgcrypto", Schema: "public"}},
	}

	diff := schemadiff.CompareWithOptions(generated, database, nil)

	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("diff: %#v", diff))
}
