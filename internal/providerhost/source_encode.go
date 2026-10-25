package providerhost

import (
	"fmt"
	"slices"
	"strconv"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"

	"ptah.run/core/objectidentity"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
)

// encodes reports whether the engine writes its values back through
// EncodeSource, which the host calls only where the provider and the engine
// list provider.EncodeSourceFeature.
func (t *target) encodes() bool {
	return slices.Contains(t.host.description.GetFeatures(), provider.EncodeSourceFeature) && slices.Contains(t.engine.GetFeatures(), provider.EncodeSourceFeature)
}

// encodeBatch builds one EncodeSource request for an engine: the document's
// tables as bindings, each value with an ID the reply names it by, and what
// the document knows of the provider's models.
type encodeBatch struct {
	target   *target
	request  *protocol.EncodeSourceRequest
	bindings map[[2]string]string
	captured bool
}

func (t *target) encodeBatch(format protocol.SourceDocument_Format) *encodeBatch {
	return &encodeBatch{target: t, bindings: make(map[[2]string]string),
		request: &protocol.EncodeSourceRequest{Engine: t.engine.GetId(), Format: format, TargetFeatures: featureSet(t.engine.GetFeatures())}}
}

// bind returns the binding of a table, adding it once.
func (b *encodeBatch) bind(schema, name string) string {
	key := [2]string{schema, name}
	if id, bound := b.bindings[key]; bound {
		return id
	}
	id := "table-" + strconv.Itoa(len(b.bindings))
	b.bindings[key] = id
	b.request.Bindings = append(b.request.Bindings, &protocol.SourceBinding{Id: id, EntryKey: name,
		Object: &protocol.Reference{Engine: b.target.engine.GetId(), Kind: "table", Schema: schema, Name: name}})
	return id
}

// add sends one value, a native object or a facet of the bound table, and
// returns its ID. A value of a model the engine does not hold is refused.
func (b *encodeBatch) add(placement protocol.SourceModelValue_Placement, binding string, value schemaext.Value) (string, error) {
	contract := b.target.host.models[modelKey{kind: value.Kind(), representation: schemaext.Desired}]
	if contract == nil || contract.engines[b.target.engine.GetId()] == nil {
		return "", fmt.Errorf("%w: model %q of provider %s is not one the %s engine holds", schemaext.ErrInvalidValue, value.Kind(),
			b.target.host.description.GetProviderId(), b.target.engine.GetId())
	}
	resolved, err := contract.value(value)
	if err != nil {
		return "", err
	}
	if resolved.normalized == nil {
		return "", fmt.Errorf("%w: model %q holds no normalized value to encode", schemaext.ErrInvalidValue, value.Kind())
	}
	features := featureSet(resolved.wire.Features)
	if !b.captured {
		b.request.TargetFeatures, b.captured = features, true
	}
	if resolved.wire.Engine != b.target.engine.GetId() || !slices.Equal(features, b.request.GetTargetFeatures()) {
		return "", fmt.Errorf("%w: encoding requires one captured target profile", schemaext.ErrInvalidValue)
	}
	normalized := proto.CloneOf(resolved.normalized)
	normalized.Id = "value-" + strconv.Itoa(len(b.request.GetValues()))
	b.request.Values = append(b.request.Values, &protocol.EncodeSourceValue{Placement: placement, Value: normalized, BindingId: binding})
	return normalized.GetId(), nil
}

// coverage sends what the document knows of the provider's models the engine
// holds. A kind's knowledge is one claim for every namespace; a claim that
// regions narrow has no spelling in the request and is refused.
func (b *encodeBatch) coverage(known schemaext.Coverage) error {
	coverage := &protocol.ModelCoverage{Representation: protocol.ValueInput_DESIRED}
	engine := b.target.engine.GetId()
	for _, record := range known.KindRecords() {
		contract := b.target.host.models[modelKey{kind: record.Model.Kind, representation: schemaext.Desired}]
		if contract == nil || contract.engines[engine] == nil || record.Model.Owner != b.target.host.description.GetProviderId() {
			continue
		}
		sources := record.Knowledge.Sources()
		if len(sources) != 1 || len(sources[0].Regions) != 0 {
			return fmt.Errorf("%w: the document's knowledge of model %q is narrowed by namespace, which an encoding request cannot carry",
				ptaherr.ErrUnsupportedFeature, record.Model.Kind)
		}
		coverage.Kinds = append(coverage.Kinds, &protocol.ModelKindCoverage{Model: proto.CloneOf(contract.engines[engine]), Knowledge: modelKnowledgeOf(sources[0].Outside)})
	}
	for _, record := range known.SubjectRecords() {
		contract := b.target.host.models[modelKey{kind: record.Kind, representation: schemaext.Desired}]
		if contract == nil || contract.engines[engine] == nil || (len(record.Targets) != 0 && !slices.Contains(record.Targets, engine)) {
			continue
		}
		subject, normalized := subjectReferences(engine, record.Subject)
		coverage.Subjects = append(coverage.Subjects, &protocol.ModelSubjectCoverage{Kind: string(record.Kind), Subject: subject, NormalizedSubject: normalized,
			Knowledge: modelKnowledgeOf(record.Knowledge)})
	}
	if len(coverage.GetKinds())+len(coverage.GetSubjects()) > 0 {
		b.request.Coverage = coverage
	}
	return nil
}

// modelKnowledgeOf is the wire's spelling of a knowledge claim.
func modelKnowledgeOf(knowledge schemaext.Knowledge) *protocol.ModelKnowledge {
	states := map[schemaext.KnowledgeState]protocol.ModelKnowledge_State{
		schemaext.Uninspected: protocol.ModelKnowledge_UNINSPECTED, schemaext.Complete: protocol.ModelKnowledge_COMPLETE,
		schemaext.Absent: protocol.ModelKnowledge_ABSENT, schemaext.Defaulted: protocol.ModelKnowledge_DEFAULTED,
		schemaext.Unrepresentable: protocol.ModelKnowledge_UNREPRESENTABLE,
	}
	return &protocol.ModelKnowledge{State: states[knowledge.State], Reason: knowledge.Reason}
}

// subjectReferences is the wire's spelling of a subject, as authored and
// normalized: a column or an index names its table as the only path
// component, as sourceSubject reads it back.
func subjectReferences(engine string, subject objectidentity.ID) (source, normalized *protocol.Reference) {
	source = &protocol.Reference{Engine: engine, Kind: string(subject.Kind), Database: subject.Database.Source, Catalog: subject.Catalog.Source,
		Schema: subject.Schema.Source, Name: subject.Name.Source}
	normalized = &protocol.Reference{Engine: engine, Kind: string(subject.Kind), Database: subject.Database.Normalized, Catalog: subject.Catalog.Normalized,
		Schema: subject.Schema.Normalized, Name: subject.Name.Normalized}
	for _, part := range subject.Namespace.Values() {
		source.Path, normalized.Path = append(source.Path, part.Source), append(normalized.Path, part.Normalized)
	}
	for _, part := range subject.Signature.Values() {
		source.Overload, normalized.Overload = append(source.Overload, part.Source), append(normalized.Overload, part.Normalized)
	}
	if !subject.Parent.Empty() {
		source.Path, normalized.Path = []string{subject.Parent.Source}, []string{subject.Parent.Normalized}
	}
	return source, normalized
}
