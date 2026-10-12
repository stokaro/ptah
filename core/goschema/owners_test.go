package goschema_test

import (
	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/annotation"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgextension"
	"ptah.run/internal/mssqlpolicysource"
	"ptah.run/internal/pgextensionsource"
	"ptah.run/internal/pgpolicysource"
)

// noOwners selects no feature owner: these tests read the frontend's own
// directives, and an owner's are tested beside the owner.
var noOwners = annotation.None()

// rowSecurityOwners selects the owners that read row-level security by its
// target scope, PostgreSQL's and SQL Server's, so a test reads it the way a
// runtime that registers them does.
var rowSecurityOwners = must.Must(annotation.NewSet(pgpolicysource.Annotations(), mssqlpolicysource.Annotations()))

// extensionOwners adds the PostgreSQL extension owner to [rowSecurityOwners],
// so a test that declares extensions reads them as the owner's objects, the
// way a runtime that registers it does.
var extensionOwners = must.Must(annotation.NewSet(pgpolicysource.Annotations(), mssqlpolicysource.Annotations(),
	pgextensionsource.Annotations()))

// declaredExtension is the declaration of the named extension the parse
// handed its owner.
func declaredExtension(c *qt.C, db *schemamodel.Database, name string) *pgextension.DesiredExtension {
	c.Helper()
	object, found, err := db.FeatureObjects.Get(pgextension.Ref(name))
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	return object.Value.(*pgextension.DesiredExtension)
}
