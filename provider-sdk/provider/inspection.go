package provider

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

// InspectionValues validates a complete inspection against the negotiated
// models and returns an independent observed-value normalization batch. Its
// order is native objects, each table's facets and column facets, then each
// common view's facets in captured order. An empty
// batch establishes no native absence; only explicit coverage can do that.
// Schema-filtered native coverage requires owner binding of request selectors
// and namespace limits. Callers must also match the snapshot target and features
// to the session they inspected before exposing any state.
func InspectionValues(description *protocol.Description, request *protocol.InspectRequest, snapshot *protocol.Schema) (*protocol.NormalizeValuesRequest, error) {
	if !slices.Contains(description.GetFeatures(), InspectFeature) {
		return nil, fmt.Errorf("%w: inspection is unavailable", ErrProtocol)
	}
	if err := ValidateSchema(snapshot); err != nil {
		return nil, err
	}
	if request.GetSessionId() == "" {
		return nil, fmt.Errorf("%w: inspection session is missing", ErrProtocol)
	}
	engine, _, err := valueTarget(description, snapshot.GetEngine(), snapshot.GetTargetFeatures())
	if err != nil {
		return nil, err
	}
	if err := validateInspectedTables(description, engine, snapshot); err != nil {
		return nil, err
	}
	if err := validateInspectedNotDescribed(description, engine, snapshot); err != nil {
		return nil, err
	}
	if err := validateInspectedPlanningContext(description, engine, request, snapshot); err != nil {
		return nil, err
	}
	if err := validateObservedCommonObjects(engine, snapshot.GetCommonObjects()); err != nil {
		return nil, err
	}
	if err := validateObservedViewFacets(description, engine, snapshot.GetTargetFeatures(), snapshot.GetCommonObjects().GetViews()); err != nil {
		return nil, err
	}
	coverage := snapshot.GetCoverage()
	if coverage.GetRepresentation() != protocol.ValueInput_OBSERVED {
		return nil, fmt.Errorf("%w: inspection requires observed model coverage", ErrProtocol)
	}
	if err := ValidateModelCoverage(description, snapshot.GetEngine(), coverage); err != nil {
		return nil, err
	}
	if err := validateReservedModels(description, snapshot); err != nil {
		return nil, err
	}
	if err := validateInspectionScope(engine, request, snapshot); err != nil {
		return nil, err
	}
	values, err := inspectionValues(snapshot)
	if err != nil {
		return nil, err
	}
	result := &protocol.NormalizeValuesRequest{Engine: snapshot.GetEngine(), TargetFeatures: slices.Clone(snapshot.GetTargetFeatures())}
	subjects := indexCoverageSubjects(coverage)
	for i, value := range values {
		if !subjects.covered(value) {
			return nil, fmt.Errorf("%w: inspected value lacks complete subject coverage", ErrProtocol)
		}
		result.Values = append(result.Values, &protocol.ValueInput{Id: strconv.Itoa(i), Representation: protocol.ValueInput_OBSERVED, Value: proto.CloneOf(value)})
	}
	if len(result.GetValues()) != 0 {
		if err := ValidateValueInputs(description, result); err != nil {
			return nil, err
		}
	}
	return result, nil
}

// validateInspectedTables checks what a snapshot says about its tables beyond
// their facets: the common columns, primary keys, indexes and constraints it
// observed, and the tables it found and does not describe.
func validateInspectedTables(description *protocol.Description, engine *protocol.Engine, snapshot *protocol.Schema) error {
	for _, check := range []func(*protocol.Description, *protocol.Engine, *protocol.Schema) error{
		validateInspectedColumns, validateInspectedPrimaryKeys, validateInspectedIndexes, validateInspectedIndexNulls, validateInspectedConstraints,
		validateUnreadTables, validateInspectedSeedReservations, validateInspectedIndexPredicates, validateInspectedRemovalBlockers,
	} {
		if err := check(description, engine, snapshot); err != nil {
			return err
		}
	}
	return nil
}

func inspectionValues(snapshot *protocol.Schema) ([]*protocol.Envelope, error) {
	values := slices.Clone(snapshot.GetNativeObjects())
	for _, value := range values {
		if value == nil || value.GetOwner().GetKind() != value.GetKind() {
			return nil, fmt.Errorf("%w: native object requires its model kind as identity kind", ErrProtocol)
		}
	}
	for _, table := range snapshot.GetTables() {
		owned, err := tableValues(table, snapshot.GetEngine())
		if err != nil {
			return nil, err
		}
		values = append(values, owned...)
	}
	for _, view := range snapshot.GetCommonObjects().GetViews() {
		values = append(values, view.GetFacets()...)
	}
	ordered := slices.Clone(values)
	slices.SortFunc(ordered, func(a, b *protocol.Envelope) int {
		if compared := strings.Compare(a.GetKind(), b.GetKind()); compared != 0 {
			return compared
		}
		return compareReferences(a.GetOwner(), b.GetOwner())
	})
	for i := 1; i < len(ordered); i++ {
		if ordered[i-1].GetKind() == ordered[i].GetKind() && compareReferences(ordered[i-1].GetOwner(), ordered[i].GetOwner()) == 0 {
			return nil, fmt.Errorf("%w: duplicate inspected model value", ErrProtocol)
		}
	}
	return values, nil
}

// tableValues are the facets of one inspected table in the order the host reads
// them: the table's own, its columns', and its indexes', in the order the
// indexes are listed. validateInspectedIndexes checks the owners of the index
// facets.
func tableValues(table *protocol.Table, engine string) ([]*protocol.Envelope, error) {
	if err := validateInspectedTableIdentity(table, engine); err != nil {
		return nil, err
	}
	var values []*protocol.Envelope
	for _, value := range table.GetFacets() {
		if value == nil || compareReferences(value.GetOwner(), table.GetIdentity()) != 0 {
			return nil, fmt.Errorf("%w: inspected facet belongs to another table", ErrProtocol)
		}
		values = append(values, value)
	}
	for _, column := range table.GetColumns() {
		owner := ColumnReference(table.GetIdentity(), column.GetName())
		for _, value := range column.GetFacets() {
			if value == nil || compareReferences(value.GetOwner(), owner) != 0 {
				return nil, fmt.Errorf("%w: inspected facet belongs to another column", ErrProtocol)
			}
			values = append(values, value)
		}
	}
	for _, index := range table.GetIndexes() {
		values = append(values, index.GetFacets()...)
	}
	return values, nil
}

// ColumnReference names a common column below its table. The table name is the
// final path component, and the column keeps its own name and kind. The result
// shares no writable state with table. A nil table yields nil. This convention
// is shared by inspection validation and providers that attach column facets.
func ColumnReference(table *protocol.Reference, name string) *protocol.Reference {
	if table == nil {
		return nil
	}
	result := proto.CloneOf(table)
	result.Path = append(result.Path, result.Name)
	result.Kind, result.Name, result.DisplayName = "column", name, ""
	return result
}

// coverageSubjects finds the subject record of a coverage that a scan of its
// subjects in order finds first for a kind and a subject, by a key that holds
// every field compareReferences compares. A snapshot covers each of its
// values by a record, and scanning every record for every value grew with the
// square of the tables.
type coverageSubjects map[string]*protocol.ModelSubjectCoverage

func indexCoverageSubjects(coverage *protocol.ModelCoverage) coverageSubjects {
	index := make(coverageSubjects, len(coverage.GetSubjects()))
	for _, record := range coverage.GetSubjects() {
		key := subjectKey(record.GetKind(), record.GetSubject())
		if _, seen := index[key]; !seen {
			index[key] = record
		}
	}
	return index
}

// covered reports whether the first record for value's kind and owner holds
// complete knowledge.
func (s coverageSubjects) covered(value *protocol.Envelope) bool {
	record, found := s[subjectKey(value.GetKind(), value.GetOwner())]
	return found && record.GetKnowledge().GetState() == protocol.ModelKnowledge_COMPLETE
}

// subjectKey writes kind and every field of ref compareReferences compares,
// each with its length, so two keys are equal exactly when the kinds are and
// compareReferences answers 0.
func subjectKey(kind string, ref *protocol.Reference) string {
	var key strings.Builder
	write := func(parts ...string) {
		key.WriteString(strconv.Itoa(len(parts)))
		for _, part := range parts {
			key.WriteByte('|')
			key.WriteString(strconv.Itoa(len(part)))
			key.WriteByte(':')
			key.WriteString(part)
		}
	}
	write(kind, ref.GetEngine(), ref.GetDatabase(), ref.GetCatalog(), ref.GetSchema())
	write(ref.GetPath()...)
	write(ref.GetKind(), ref.GetName())
	write(ref.GetOverload()...)
	return key.String()
}

// ValidateNormalizedInspection checks the owner's complete normalization reply
// against the observed values and their declared normalized coverage subjects.
// A caller must discard the whole snapshot if this validation fails.
func ValidateNormalizedInspection(description *protocol.Description, request *protocol.InspectRequest, snapshot *protocol.Schema, normalized *protocol.NormalizedValues) error {
	batch, err := InspectionValues(description, request, snapshot)
	if err != nil {
		return err
	}
	if err := ValidateNormalizedValues(description, batch, normalized); err != nil {
		return err
	}
	subjects := indexCoverageSubjects(snapshot.GetCoverage())
	for _, value := range normalized.GetValues() {
		record, found := subjects[subjectKey(value.GetValue().GetKind(), value.GetValue().GetOwner())]
		if !found || compareReferences(record.GetNormalizedSubject(), value.GetNormalizedOwner()) != 0 {
			return fmt.Errorf("%w: normalized inspection subject differs from coverage", ErrProtocol)
		}
	}
	return nil
}

func validateInspectedTableIdentity(table *protocol.Table, engine string) error {
	if !validValueReference(table.GetNormalizedIdentity(), engine) || table.GetNormalizedIdentity().GetKind() != "table" {
		return fmt.Errorf("%w: normalized table identity is missing", ErrProtocol)
	}
	if table.GetIdentity().GetEngine() != engine {
		return fmt.Errorf("%w: inspected table belongs to another engine", ErrProtocol)
	}
	return nil
}
