package planner_test

import (
	"slices"

	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/internal/builtintest"
)

// Common-column fixtures describe an inspected MergeTree table with no key or
// TTL dependencies. The planner must not infer that fact from missing facets.
func clickhouseTableCapture(schema, name string) schemacapture.TableObservation {
	models := builtintest.Runtime().Codecs().Definitions()
	model := models[slices.IndexFunc(models, func(v schemaext.CodecIdentity) bool {
		return v.Kind == chschema.TableKind && v.Representation == schemaext.Observed
	})]
	coverage := must.Must(schemaext.NewCoverage(schemaext.Observed, []schemaext.KindCoverage{{Model: model, Knowledge: schemaext.Knowledge{State: schemaext.Complete}}}, nil))
	return schemacapture.TableObservation{
		Table:           catalog.Table{Schema: schema, Name: name, Type: "TABLE", Facets: must.Must(schemaext.NewFacets(&chschema.ObservedTable{Engine: "MergeTree", OrderBy: "tuple()"}))},
		FeatureCoverage: coverage,
	}
}
