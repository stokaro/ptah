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

// TestValidateParsed_AcceptsAnExtensionTheOwnerHolds pins the cleanup of an
// extension annotation: the export holds the extension as the owner's object,
// which represents the annotation.
func TestValidateParsed_AcceptsAnExtensionTheOwnerHolds(t *testing.T) {
	c := qt.New(t)
	plan := extensionPlan(c)
	exported := &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(
		must.Must(pgextension.DesiredObject(pgextension.Ref("pg_trgm"), pgextension.DesiredExtension{}))))}

	err := plan.ValidateParsed(&schemamodel.Database{}, exported)

	c.Assert(err, qt.IsNil)
}

// TestValidateParsed_RefusesAnExtensionTheOwnerDoesNotHold is the control: an
// export that holds another name, here one that differs only in case, does not
// represent the annotation.
func TestValidateParsed_RefusesAnExtensionTheOwnerDoesNotHold(t *testing.T) {
	c := qt.New(t)
	plan := extensionPlan(c)
	exported := &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(
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
