package schemacensus

import (
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgextension"
)

// ownedExtensionFixture declares an extension through its owner with every
// declared field set, a schema of its own, a version, a comment and IF NOT
// EXISTS, so the census measures each one.
func ownedExtensionFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.FeatureObjects = must.Must(schemaext.NewObjects(must.Must(pgextension.DesiredObject(pgextension.Ref("postgis"),
		pgextension.DesiredExtension{Schema: "geo", Version: "3.4", Comment: "geometry types", IfNotExists: true}))))
	db.FeatureCoverage = must.Must(pgextension.CompleteCoverage(schemaext.Desired))
	return db
}
