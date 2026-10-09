package ydbworkload

import (
	"fmt"

	"ptah.run/core/schemaext"
)

// Coverage enrolls one workload family's precise model definition. A pool
// receipt cannot establish classifier coverage, or the reverse. Runtime growth
// never enlarges a source's knowledge of workload objects.
func Coverage(kind schemaext.Kind, representation schemaext.Representation, knowledge schemaext.Knowledge, subjects []schemaext.SubjectCoverage) (schemaext.Coverage, error) {
	var owned []schemaext.OwnedCodec
	for _, codec := range Codecs() {
		owned = append(owned, schemaext.OwnedCodec{Owner: "ptah.run/ydb", Codec: codec})
	}
	registry, err := schemaext.NewRegistry(owned...)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	for _, definition := range registry.Definitions() {
		if definition.Kind == kind && definition.Representation == representation {
			return schemaext.NewCoverage(representation, []schemaext.KindCoverage{{Model: definition, Knowledge: knowledge}}, subjects)
		}
	}
	return schemaext.Coverage{}, fmt.Errorf("%w: workload coverage requires a pool or classifier desired/observed model", schemaext.ErrInvalidValue)
}
