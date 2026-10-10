package tsschema

import (
	"fmt"
	"sync"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
)

// Coverage enrolls one TimescaleDB model in a source's claims. knowledge is the
// namespace-wide claim and subjects override it for individual tables or
// aggregates. Enrollment names this model's exact definition, so a source
// captured before a runtime gained another provider never claims it.
func Coverage(kind schemaext.Kind, representation schemaext.Representation, knowledge schemaext.Knowledge, subjects []schemaext.SubjectCoverage) (schemaext.Coverage, error) {
	model, err := Model(kind, representation)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	return schemaext.NewCoverage(representation, []schemaext.KindCoverage{{Model: model, Knowledge: knowledge}}, subjects)
}

// CompleteCoverage enrolls both models with complete knowledge: the source
// describes every hypertable and continuous aggregate it holds, so an omitted
// one is absent. Go annotations, an HCL document and a read of a server all
// make this claim; a format with no spelling for either makes none.
func CompleteCoverage(representation schemaext.Representation) (schemaext.Coverage, error) {
	complete := schemaext.Knowledge{State: schemaext.Complete}
	hypertables, err := Coverage(HypertableKind, representation, complete, nil)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	aggregates, err := Coverage(ContinuousAggregateKind, representation, complete, nil)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	return hypertables.Combine(aggregates)
}

// RequireNoLimits refuses a coverage record that limits what is known about
// one TimescaleDB subject. A document format whose only claim is
// [CompleteCoverage] has no spelling for such a limit, so writing one would
// turn a hypertable or aggregate nobody could describe into one the document
// says is absent. The error wraps [ptaherr.ErrUnsupportedFeature] and names the
// first such subject.
func RequireNoLimits(known schemaext.Coverage) error {
	for _, record := range known.SubjectRecords() {
		if record.Kind != HypertableKind && record.Kind != ContinuousAggregateKind {
			continue
		}
		if record.Knowledge.State != schemaext.Complete && record.Knowledge.State != schemaext.Absent {
			return fmt.Errorf("%w: %s cannot be exported without losing its coverage record: %s %s",
				ptaherr.ErrUnsupportedFeature, record.Subject, record.Knowledge.State, record.Knowledge.Reason)
		}
	}
	return nil
}

// Model returns the codec identity a coverage record names for kind.
func Model(kind schemaext.Kind, representation schemaext.Representation) (schemaext.CodecIdentity, error) {
	definitions, err := modelDefinitions()
	if err != nil {
		return schemaext.CodecIdentity{}, err
	}
	for _, definition := range definitions {
		if definition.Kind == kind && definition.Representation == representation {
			return definition, nil
		}
	}
	return schemaext.CodecIdentity{}, fmt.Errorf("%w: no TimescaleDB model %q for %q", schemaext.ErrInvalidValue, kind, representation)
}

// modelDefinitions builds the codec identities once: every Go file, HCL
// document and PostgreSQL read asks for them, and the codecs never change.
var modelDefinitions = sync.OnceValues(func() ([]schemaext.CodecIdentity, error) {
	owned := make([]schemaext.OwnedCodec, 0, 4)
	for _, codec := range Codecs() {
		owned = append(owned, schemaext.OwnedCodec{Owner: Owner, Codec: codec})
	}
	registry, err := schemaext.NewRegistry(owned...)
	if err != nil {
		return nil, err
	}
	return registry.Definitions(), nil
})
