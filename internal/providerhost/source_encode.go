package providerhost

import (
	"fmt"
	"maps"
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
	// tables are the bound tables by binding ID, and placed the bindings a
	// facet has named the database of.
	tables   map[string]*protocol.Reference
	placed   map[string]bool
	captured bool
	// databases are the databases the batch's values name: a read's values
	// name the one it was made in, and a source's name none.
	databases map[string]bool
}

func (t *target) encodeBatch(format protocol.SourceDocument_Format) *encodeBatch {
	return &encodeBatch{target: t, bindings: make(map[[2]string]string), tables: make(map[string]*protocol.Reference), placed: make(map[string]bool),
		databases: make(map[string]bool),
		request:   &protocol.EncodeSourceRequest{Engine: t.engine.GetId(), Format: format, TargetFeatures: featureSet(t.engine.GetFeatures())}}
}

// bind returns the binding of a table, adding it once.
func (b *encodeBatch) bind(schema, name string) string {
	key := [2]string{schema, name}
	if id, bound := b.bindings[key]; bound {
		return id
	}
	id := "table-" + strconv.Itoa(len(b.bindings))
	b.bindings[key] = id
	table := &protocol.Reference{Engine: b.target.engine.GetId(), Kind: "table", Schema: schema, Name: name}
	b.tables[id] = table
	b.request.Bindings = append(b.request.Bindings, &protocol.SourceBinding{Id: id, EntryKey: name, Object: table})
	return id
}

// place names a bound table in the database its facets name. A facet read
// from a database names that database in its owner, and the provider takes a
// table facet only from a binding that names the table as the facet does; a
// facet of a source names none, so neither does its binding. The facets of
// one table name one database.
func (b *encodeBatch) place(binding, database string) error {
	table := b.tables[binding]
	if table == nil {
		return fmt.Errorf("%w: a facet names a table binding the batch does not hold", schemaext.ErrInvalidValue)
	}
	if !b.placed[binding] {
		table.Database, b.placed[binding] = database, true
		return nil
	}
	if table.GetDatabase() != database {
		return fmt.Errorf("%w: the facets of table %q name the databases %q and %q, and a source binds a table in one", schemaext.ErrInvalidValue,
			table.GetName(), table.GetDatabase(), database)
	}
	return nil
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
	database := resolved.normalized.GetValue().GetOwner().GetDatabase()
	if placement == protocol.SourceModelValue_FACET {
		if err := b.place(binding, database); err != nil {
			return "", err
		}
	}
	if database != "" {
		b.databases[database] = true
	}
	normalized := proto.CloneOf(resolved.normalized)
	normalized.Id = "value-" + strconv.Itoa(len(b.request.GetValues()))
	b.request.Values = append(b.request.Values, &protocol.EncodeSourceValue{Placement: placement, Value: normalized, BindingId: binding})
	return normalized.GetId(), nil
}

// coverage sends what the document knows of the provider's models the engine
// holds. A kind's knowledge is one claim: what holds outside its namespace
// regions, and the regions, spelled as the provider reports them. A claim
// that independent sources make, or a region the provider's spelling cannot
// give back exactly, has no place in the request and is refused, naming the
// model.
func (b *encodeBatch) coverage(known schemaext.Coverage) error {
	coverage := &protocol.ModelCoverage{Representation: protocol.ValueInput_DESIRED}
	engine := b.target.engine.GetId()
	for _, record := range known.KindRecords() {
		contract := b.target.host.models[modelKey{kind: record.Model.Kind, representation: schemaext.Desired}]
		if contract == nil || contract.engines[engine] == nil || record.Model.Owner != b.target.host.description.GetProviderId() {
			continue
		}
		sources := record.Knowledge.Sources()
		if len(sources) != 1 {
			return fmt.Errorf("%w: the document's knowledge of model %q combines independent sources, which an encoding request cannot carry",
				ptaherr.ErrUnsupportedFeature, record.Model.Kind)
		}
		regions, err := b.regions(record.Model.Kind, sources[0], known)
		if err != nil {
			return err
		}
		coverage.Kinds = append(coverage.Kinds, &protocol.ModelKindCoverage{Model: proto.CloneOf(contract.engines[engine]), Knowledge: modelKnowledgeOf(sources[0].Outside),
			Regions: regions})
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

// regions spells the namespace regions of one kind's knowledge as the regions
// the provider reported. A region scoped to other engines says nothing of this
// one and is left out. The host holds a reported region as the scopes
// namespaceScopes builds from it: one for the kind's own objects, and, where
// the region can hold them, one for the facets of common tables and columns.
// So the scope of the kind's own objects is spelled as the region it was built
// from, and the spelled regions are read back through namespaceScopes. The
// reading has to give exactly the knowledge in hand, which keeps a complete
// region at the bounds it was read with, no wider and no narrower; anything
// else is refused, naming the model.
//
// The host holds the objects of a scope-selected kind without the database
// they were read in, and namespaceScopes reads such a region the same way
// whatever database it names. So the region names the database the document
// was read in where the document names one (sessionDatabase), as the provider
// reported it, and otherwise none, which matches the kind's objects in the
// database the provider reads them in. A region that has no other bound than
// its database then has none at all, and is refused.
func (b *encodeBatch) regions(kind schemaext.Kind, source schemaext.KnowledgeSource, known schemaext.Coverage) ([]*protocol.ModelNamespaceKnowledge, error) {
	engine := b.target.engine.GetId()
	var held []schemaext.NamespaceKnowledge
	for _, region := range source.Regions {
		if len(region.Scope.Targets) == 0 || slices.Contains(region.Scope.Targets, engine) {
			held = append(held, region)
		}
	}
	if len(held) == 0 {
		return nil, nil
	}
	refused := fmt.Errorf("%w: the document's knowledge of model %q is narrowed to a namespace that no provider region spells, which an encoding request cannot carry",
		ptaherr.ErrUnsupportedFeature, kind)
	semantics, err := identifierRules(b.target.engine.GetIdentifiers(), b.target.engine.GetDefaultSchema())
	if err != nil {
		return nil, err
	}
	selections := b.target.host.selections
	var spelled []*protocol.ModelNamespaceKnowledge
	var read []schemaext.NamespaceKnowledge
	for _, region := range held {
		if !slices.Equal(region.Scope.SubjectKinds, []objectidentity.Kind{objectidentity.Kind(kind)}) {
			continue
		}
		scope := &protocol.ModelNamespace{Database: namespaceComponent(region.Scope.Database), Catalog: namespaceComponent(region.Scope.Catalog),
			Schema: namespaceComponent(region.Scope.Schema)}
		for _, part := range region.Scope.PathPrefix.Values() {
			scope.PathPrefix = append(scope.PathPrefix, &protocol.NamespaceComponent{Source: part.Source, Normalized: part.Normalized})
		}
		if selections.selects(objectidentity.Kind(kind)) {
			scope.Database = nil
			if database, found := b.sessionDatabase(known); found {
				scope.Database = &protocol.NamespaceComponent{Source: database, Normalized: database}
			}
		}
		if scope.GetDatabase() == nil && scope.GetCatalog() == nil && scope.GetSchema() == nil && len(scope.GetPathPrefix()) == 0 {
			return nil, refused
		}
		scopes, err := namespaceScopes(selections, semantics, engine, kind, scope)
		if err != nil {
			return nil, refused
		}
		for _, bound := range scopes {
			read = append(read, schemaext.NamespaceKnowledge{Scope: bound, Knowledge: region.Knowledge})
		}
		spelled = append(spelled, &protocol.ModelNamespaceKnowledge{Scope: scope, Knowledge: modelKnowledgeOf(region.Knowledge)})
	}
	want, err := schemaext.NewScopedKnowledge(source.Outside, held...)
	if err != nil {
		return nil, err
	}
	got, err := schemaext.NewScopedKnowledge(source.Outside, read...)
	if err != nil || !got.Equal(want) {
		return nil, refused
	}
	return spelled, nil
}

// sessionDatabase is the database a read of the document was made in: the one
// database named by the batch's values and by the regions of kinds no
// selection names. A document that names none, or several, has none.
func (b *encodeBatch) sessionDatabase(known schemaext.Coverage) (string, bool) {
	databases := maps.Clone(b.databases)
	for _, record := range known.KindRecords() {
		if b.target.host.selections.selects(objectidentity.Kind(record.Model.Kind)) {
			continue
		}
		for _, source := range record.Knowledge.Sources() {
			for _, region := range source.Regions {
				if database := region.Scope.Database; database != nil && database.Source != "" && slices.Contains(region.Scope.Targets, b.target.engine.GetId()) {
					databases[database.Source] = true
				}
			}
		}
	}
	for database := range databases {
		return database, len(databases) == 1
	}
	return "", false
}

// namespaceComponent is the wire's spelling of a namespace qualifier: an
// absent one matches every name, a present empty one only an empty name.
func namespaceComponent(part *objectidentity.Part) *protocol.NamespaceComponent {
	if part == nil {
		return nil
	}
	return &protocol.NamespaceComponent{Source: part.Source, Normalized: part.Normalized}
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
