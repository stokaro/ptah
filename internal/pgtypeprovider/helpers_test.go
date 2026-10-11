package pgtypeprovider_test

import (
	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
	"ptah.run/feature/pgtype"
	"ptah.run/internal/pgtypeprovider"
)

// newRuntime selects the owner beside a provider of its targets and nothing
// else, so every answer below is the owner's.
func newRuntime(c *qt.C) *engine.Runtime {
	c.Helper()
	targets := make([]engine.Target, 0, len(pgtypeprovider.Targets()))
	for _, name := range pgtypeprovider.Targets() {
		targets = append(targets, engine.Target{Name: name})
	}
	return must.Must(engine.New(engine.Provider{ID: "example.org/targets", Targets: targets}, pgtypeprovider.Provider()))
}

// ref names the type t of the value's model in schema.
func ref(value schemaext.Value, schema string) objectidentity.ID {
	return pgtype.Ref(value.Kind(), schema, "t")
}

// declared is the declaration of t, which names no schema.
func declared(value schemaext.Value) schemaext.Object {
	return must.Must(pgtype.DesiredObject(ref(value, ""), value))
}

// held is the observation of t, which the catalog reports in public.
func held(value schemaext.Value) schemaext.Object {
	return must.Must(pgtype.ObservedObject(ref(value, "public"), value))
}

// objectsOf collects objects, which may be none.
func objectsOf(objects ...schemaext.Object) schemaext.Objects {
	return must.Must(schemaext.NewObjects(objects...))
}

// comparison compares desired with current on PostgreSQL 17, which stores
// the comment of every kind, both sources describing every type.
func comparison(desired, current []schemaext.Object) schemaext.ObjectComparisonRequest {
	return schemaext.ObjectComparisonRequest{
		Target: "postgres", Identifiers: identifier.ForDialect("postgres"), Capabilities: capability.Postgres17(),
		Desired: schemaext.ObjectState{Objects: objectsOf(desired...), Coverage: must.Must(pgtype.CompleteCoverage(schemaext.Desired))},
		Current: schemaext.ObjectState{Objects: objectsOf(current...), Coverage: must.Must(pgtype.CompleteCoverage(schemaext.Observed))},
	}
}

// direct is a comparison as the runtime hands it to the service, which names
// the model kinds the service is asked about.
func direct(desired, current []schemaext.Object) schemaext.ObjectComparisonRequest {
	request := comparison(desired, current)
	request.Kinds = pgtype.Kinds()
	return request
}

// comparisonOf compares the declarations of t with the observations of t,
// each list holding at most one value of a model.
func comparisonOf(declarations, observations []schemaext.Value) schemaext.ObjectComparisonRequest {
	desired := make([]schemaext.Object, 0, len(declarations))
	for _, value := range declarations {
		desired = append(desired, declared(value))
	}
	current := make([]schemaext.Object, 0, len(observations))
	for _, value := range observations {
		current = append(current, held(value))
	}
	return comparison(desired, current)
}
