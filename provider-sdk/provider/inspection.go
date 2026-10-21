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
// order is native objects, then each table's facets and column facets. An empty
// batch establishes no native absence; only explicit coverage can do that.
// Schema-filtered native coverage is refused until the contract can retain its
// namespace limits. Callers must also match the snapshot target and features
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
	if _, _, err := valueTarget(description, snapshot.GetEngine(), snapshot.GetTargetFeatures()); err != nil {
		return nil, err
	}
	coverage := snapshot.GetCoverage()
	if coverage.GetRepresentation() != protocol.ValueInput_OBSERVED {
		return nil, fmt.Errorf("%w: inspection requires observed model coverage", ErrProtocol)
	}
	if err := ValidateModelCoverage(description, snapshot.GetEngine(), coverage); err != nil {
		return nil, err
	}
	if len(request.GetSchemas()) != 0 && len(coverage.GetKinds()) != 0 {
		return nil, fmt.Errorf("%w: schema-filtered native model coverage requires namespace scopes", ErrProtocol)
	}
	values, err := inspectionValues(snapshot)
	if err != nil {
		return nil, err
	}
	result := &protocol.NormalizeValuesRequest{Engine: snapshot.GetEngine(), TargetFeatures: slices.Clone(snapshot.GetTargetFeatures())}
	for i, value := range values {
		if !inspectionSubjectCovered(coverage, value) {
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

func inspectionValues(snapshot *protocol.Schema) ([]*protocol.Envelope, error) {
	values := slices.Clone(snapshot.GetNativeObjects())
	for _, value := range values {
		if value == nil || value.GetOwner().GetKind() != value.GetKind() {
			return nil, fmt.Errorf("%w: native object requires its model kind as identity kind", ErrProtocol)
		}
	}
	for _, table := range snapshot.GetTables() {
		if table.GetIdentity().GetEngine() != snapshot.GetEngine() {
			return nil, fmt.Errorf("%w: inspected table belongs to another engine", ErrProtocol)
		}
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

func inspectionSubjectCovered(coverage *protocol.ModelCoverage, value *protocol.Envelope) bool {
	for _, record := range coverage.GetSubjects() {
		if record.GetKind() == value.GetKind() && compareReferences(record.GetSubject(), value.GetOwner()) == 0 {
			return record.GetKnowledge().GetState() == protocol.ModelKnowledge_COMPLETE
		}
	}
	return false
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
	for _, value := range normalized.GetValues() {
		matched := false
		for _, record := range snapshot.GetCoverage().GetSubjects() {
			if record.GetKind() == value.GetValue().GetKind() && compareReferences(record.GetSubject(), value.GetValue().GetOwner()) == 0 {
				matched = compareReferences(record.GetNormalizedSubject(), value.GetNormalizedOwner()) == 0
				break
			}
		}
		if !matched {
			return fmt.Errorf("%w: normalized inspection subject differs from coverage", ErrProtocol)
		}
	}
	return nil
}
