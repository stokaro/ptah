package builtin

import (
	"slices"

	"ptah.run/dialect/cockroachdb/crdbast"
	"ptah.run/dialect/cockroachdb/crdbcompare"
	"ptah.run/dialect/cockroachdb/crdbconvert"
	"ptah.run/dialect/cockroachdb/crdbdiff"
	"ptah.run/dialect/cockroachdb/crdbplan"
	"ptah.run/dialect/cockroachdb/crdbreport"
	"ptah.run/dialect/cockroachdb/crdbreverse"
	"ptah.run/dialect/cockroachdb/crdbschema"
	"ptah.run/dialect/cockroachdb/crdbsource"
	"ptah.run/engine"
)

// CockroachDB row-level TTL is a table facet.
func registerCockroachDBServices(provider *engine.Provider, target string) {
	registerTableFacetOwner(provider, target, tableFacetOwner{
		codecs:          slices.Concat(crdbschema.Codecs(), crdbdiff.Codecs(), crdbast.Codecs()),
		properties:      crdbsource.Definitions(),
		propertyService: crdbsource.Service{},
		facet:           crdbschema.RowTTLKind,
		change:          crdbdiff.RowTTLKind,
		operation:       crdbast.AlterRowTTLKind,
		conversion:      crdbconvert.Service{},
		comparison:      crdbcompare.Service{},
		reversal:        crdbreverse.Service{},
		planning:        crdbplan.Service{},
		reports:         crdbreport.Definitions(),
		reporting:       crdbreport.Service{},
	})
}
