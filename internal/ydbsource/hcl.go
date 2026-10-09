package ydbsource

import (
	"context"
	"fmt"
	"sync"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbcoordination"
)

// ReadHCLCoverage decodes captured model claims or authored limit directives.
// Both spellings cannot govern the same source: an ambiguous second account
// must not override the first. The selected vocabulary belongs to this codec,
// independently of the runtime's installed providers.
// The boolean reports an explicit versioned header, for lossless file splitting.
func ReadHCLCoverage(document string, limits Limits) (schemaext.Coverage, bool, error) {
	registry, err := hclCoverageRegistry()
	if err != nil {
		return schemaext.Coverage{}, false, err
	}
	known, found, err := registry.DecodeCoverageHeader(context.Background(), schemaext.Desired, document)
	if err != nil {
		return schemaext.Coverage{}, false, err
	}
	if !found {
		known, err = HCLCoverage(limits)
		return known, false, err
	}
	if len(limits.Coordination) != 0 {
		return schemaext.Coverage{}, false, fmt.Errorf("%w: feature coverage header and coordination limit directives cannot be combined", schemaext.ErrDuplicate)
	}
	for _, record := range known.SubjectRecords() {
		if err := ydbcoordination.ValidateIdentity(record.Subject); err != nil {
			return schemaext.Coverage{}, false, err
		}
	}
	return known, true, nil
}

// hclCoverageRegistry builds the desired coordination-node registry once.
// Building one validates and hashes every codec definition, and every HCL load
// and export asks for it.
var hclCoverageRegistry = sync.OnceValues(func() (schemaext.Registry, error) {
	var models []schemaext.OwnedCodec
	for _, codec := range ydbcoordination.Codecs() {
		if codec.Representation == schemaext.Desired {
			models = append(models, schemaext.OwnedCodec{Owner: "ptah.run/ydb", Codec: codec})
		}
	}
	return schemaext.NewRegistry(models...)
})
