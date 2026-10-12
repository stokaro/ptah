package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgextension"
	"ptah.run/migration/schemadiff"
)

// TestCompareWithOptions_ImplicitExtensionSchemaMatchesPostgreSQLDefault pins
// end to end that a declaration naming no schema agrees with an extension
// installed in the connection's default schema.
func TestCompareWithOptions_ImplicitExtensionSchemaMatchesPostgreSQLDefault(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(must.Must(pgextension.DesiredObject(pgextension.Ref("pgcrypto"), pgextension.DesiredExtension{})))),
		FeatureCoverage: must.Must(pgextension.CompleteCoverage(schemaext.Desired))}
	database := &catalog.Database{
		FeatureObjects:  must.Must(schemaext.NewObjects(must.Must(pgextension.ObservedObject(pgextension.Ref("pgcrypto"), pgextension.ObservedExtension{Schema: "public"})))),
		FeatureCoverage: must.Must(pgextension.CompleteCoverage(schemaext.Observed)),
	}

	diff := must.Must(schemadiff.CompareWithOptions(t.Context(), desired, database, &config.CompareOptions{Dialect: "postgres"}, must.Must(builtin.New())))

	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("diff: %#v", diff))
}
