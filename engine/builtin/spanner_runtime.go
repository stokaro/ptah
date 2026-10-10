package builtin

import (
	"slices"

	"ptah.run/dialect/spanner/spannerast"
	"ptah.run/dialect/spanner/spannercompare"
	"ptah.run/dialect/spanner/spannerconvert"
	"ptah.run/dialect/spanner/spannerdiff"
	"ptah.run/dialect/spanner/spannerplan"
	"ptah.run/dialect/spanner/spannerreport"
	"ptah.run/dialect/spanner/spannerreverse"
	"ptah.run/dialect/spanner/spannerschema"
	"ptah.run/dialect/spanner/spannersource"
	"ptah.run/engine"
)

// The Spanner row deletion policy is a table facet.
func registerSpannerServices(provider *engine.Provider, target string) {
	registerTableFacetOwner(provider, target, tableFacetOwner{
		codecs:          slices.Concat(spannerschema.Codecs(), spannerdiff.Codecs(), spannerast.Codecs()),
		properties:      spannersource.Definitions(),
		propertyService: spannersource.Service{},
		facet:           spannerschema.RowDeletionKind,
		change:          spannerdiff.RowDeletionKind,
		operation:       spannerast.AlterRowDeletionKind,
		conversion:      spannerconvert.Service{},
		comparison:      spannercompare.Service{},
		reversal:        spannerreverse.Service{},
		planning:        spannerplan.Service{},
		reports:         spannerreport.Definitions(),
		reporting:       spannerreport.Service{},
	})
}
