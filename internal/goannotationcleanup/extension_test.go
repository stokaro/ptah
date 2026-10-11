package goannotationcleanup_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgextension"
	"ptah.run/internal/goannotationcleanup"
)

// TestValidateParsed_AcceptsAnExtensionEitherModelHolds pins the cleanup of
// an extension annotation: the export may hold the extension in the common
// model or as the extension owner's object, and either one represents it.
func TestValidateParsed_AcceptsAnExtensionEitherModelHolds(t *testing.T) {
	tests := []struct {
		name     string
		exported *schemamodel.Database
	}{
		{name: "the common model", exported: &schemamodel.Database{Extensions: []schemamodel.Extension{{Name: "pg_trgm"}}}},
		{name: "the owner's object", exported: &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(
			must.Must(pgextension.DesiredObject(pgextension.Ref("pg_trgm"), pgextension.DesiredExtension{}))))}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			plan := extensionPlan(c)

			err := plan.ValidateParsed(&schemamodel.Database{}, test.exported)

			c.Assert(err, qt.IsNil)
		})
	}
}

func TestValidateParsed_RefusesAnExtensionNeitherModelHolds(t *testing.T) {
	c := qt.New(t)
	plan := extensionPlan(c)
	exported := &schemamodel.Database{Extensions: []schemamodel.Extension{{Name: "citext"}}, FeatureObjects: must.Must(schemaext.NewObjects(
		must.Must(pgextension.DesiredObject(pgextension.Ref("PG_TRGM"), pgextension.DesiredExtension{}))))}

	err := plan.ValidateParsed(&schemamodel.Database{}, exported)

	c.Assert(err, qt.ErrorIs, goannotationcleanup.ErrUnexportedAnnotation)
	c.Assert(err, qt.ErrorMatches, `.*//ptah:schema:extension did not produce a schema object`)
}

// extensionPlan plans the cleanup of a source declaring pg_trgm.
func extensionPlan(c *qt.C) *goannotationcleanup.Plan {
	c.Helper()
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "model.go"),
		[]byte("package models\n\n//ptah:schema:extension name=\"pg_trgm\"\ntype Extensions struct{}\n"), 0o600), qt.IsNil)
	return must.Must(planDir(dir))
}
