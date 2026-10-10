package builtin

import (
	"slices"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbcompare"
	"ptah.run/dialect/ydb/ydbconvert"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbplan"
	"ptah.run/dialect/ydb/ydbreport"
	"ptah.run/dialect/ydb/ydbreverse"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/engine"
	"ptah.run/internal/ydbsource"
)

// A YDB table's TTL is a table facet of the YDB owner.
func registerYDBTTLServices(provider *engine.Provider, target string) {
	registerTableFacetOwner(provider, target, tableFacetOwner{
		codecs:          slices.Concat(ydbschema.TTLCodecs(), []schemaext.Codec{ydbdiff.TTLCodec(), ydbast.TTLCodec()}),
		properties:      ydbsource.TTLDefinitions(),
		propertyService: ydbsource.TTLService{},
		facet:           ydbschema.TTLKind,
		change:          ydbdiff.TTLKind,
		operation:       ydbast.AlterTTLKind,
		conversion:      ydbconvert.TTLService{},
		comparison:      ydbcompare.TTLService{},
		reversal:        ydbreverse.TTLService{},
		planning:        ydbplan.TTLService{},
		reports:         ydbreport.TTLDefinitions(),
		reporting:       ydbreport.TTLService{},
	})
}
