package mssql_test

import (
	"reflect"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
)

// TestSynonyms_ModelAndIntrospectionLandTogether is a tripwire, not a feature
// test. It passes today because neither side declares synonyms.
//
// The SQL Server renderer emits CREATE SYNONYM. Nothing can reach that arm yet,
// because no schema model carries a synonym and no converter builds the node,
// so there is no way to publish a statement the reader cannot read back.
//
// The day goschema.Database gains a Synonyms collection, that stops being true.
// A declared synonym would render, apply, and then be invisible to `db read` --
// so the next comparison would find it missing and emit CREATE SYNONYM again,
// forever. The mssql renderer's own VisitCreateSequence comment records this
// shape as the reason sequences are NOT emitted for this dialect: a renderer
// that runs ahead of its reader produces an apply loop rather than a feature.
//
// This test is what makes that failure loud and immediate instead of a bug
// somebody meets in production. Whoever adds the model must add the
// introspection type in the same change, which is what stokaro/ptah#1030 asks
// for.
func TestSynonyms_ModelAndIntrospectionLandTogether(t *testing.T) {
	c := qt.New(t)

	_, modelled := reflect.TypeFor[goschema.Database]().FieldByName("Synonyms")
	_, introspected := reflect.TypeFor[dbschematypes.DBSchema]().FieldByName("Synonyms")

	c.Assert(modelled, qt.Equals, introspected, qt.Commentf(
		"goschema.Database declares Synonyms: %t, dbschema/types.DBSchema declares them: %t. "+
			"The SQL Server renderer emits CREATE SYNONYM, so a model that declares the object "+
			"while introspection cannot read it back makes every apply re-create it",
		modelled, introspected))
}
