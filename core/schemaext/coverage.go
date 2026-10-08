package schemaext

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"ptah.run/core/objectidentity"
)

// KnowledgeState records what a source established, independently of whether
// the value collection contains an object. The zero value is uninspected.
type KnowledgeState string

const (
	// Uninspected means the source made no claim about the relevant state.
	Uninspected KnowledgeState = ""
	// Complete means the source describes this scope; an omitted object is absent.
	Complete KnowledgeState = "complete"
	// Absent records an explicit absence assertion for one subject.
	Absent KnowledgeState = "absent"
	// Defaulted records a declaration that requests owner-defined defaults.
	Defaulted KnowledgeState = "defaulted"
	// Unrepresentable means the source encountered state it cannot fully describe.
	Unrepresentable KnowledgeState = "unrepresentable"
)

// Knowledge retains an explicit unknown/default/absence distinction. Reason
// explains an uninspected or unrepresentable record. Defaulted is author intent,
// not an observation that a server has no value.
type Knowledge struct {
	State  KnowledgeState `json:"state"`
	Reason string         `json:"reason,omitempty"`
}

// KindCoverage enrolls one precise model definition in a source's claims.
// Registry growth cannot add entries to an already captured source description.
type KindCoverage struct {
	Model     CodecIdentity `json:"model"`
	Knowledge Knowledge     `json:"knowledge"`
}

// SubjectCoverage overrides a kind-wide claim for one structured subject.
// A facet uses its common owner identity; a feature object uses its own identity.
// A table identity also records a namespace claim for its named children of Kind.
type SubjectCoverage struct {
	Kind      Kind              `json:"kind"`
	Subject   objectidentity.ID `json:"subject"`
	Knowledge Knowledge         `json:"knowledge"`
}

type subjectKey struct {
	kind Kind
	ref  objectidentity.Key
}

// Coverage is an immutable positive account of source knowledge. Its zero
// value describes no kinds, unlike the legacy common-object coverage.Set zero
// value. The distinction is explicit: this type governs feature models only.
type Coverage struct {
	representation Representation
	kinds          map[Kind]KindCoverage
	subjects       map[subjectKey]SubjectCoverage
}

// NewCoverage captures claims from one desired or observed source. Unknown
// models must stay unenrolled or explicitly uninspected. Conflicting duplicate
// records and subject records without an enrolled model are refused.
func NewCoverage(representation Representation, kinds []KindCoverage, subjects []SubjectCoverage) (Coverage, error) {
	if representation != Desired && representation != Observed {
		return Coverage{}, fmt.Errorf("%w: coverage requires desired or observed representation", ErrInvalidValue)
	}
	result := Coverage{representation: representation, kinds: make(map[Kind]KindCoverage), subjects: make(map[subjectKey]SubjectCoverage)}
	for _, record := range kinds {
		if err := validateKindCoverage(representation, record); err != nil {
			return Coverage{}, err
		}
		if _, found := result.kinds[record.Model.Kind]; found {
			return Coverage{}, fmt.Errorf("%w: coverage for kind %q", ErrDuplicate, record.Model.Kind)
		}
		result.kinds[record.Model.Kind] = record
	}
	for _, record := range subjects {
		if _, found := result.kinds[record.Kind]; !found {
			return Coverage{}, fmt.Errorf("%w: subject coverage for unenrolled kind %q", ErrInvalidValue, record.Kind)
		}
		if err := validateKnowledge(representation, record.Knowledge); err != nil {
			return Coverage{}, err
		}
		if record.Subject.Kind == "" || record.Subject.Name.Source == "" || record.Subject.Name.Normalized == "" {
			return Coverage{}, fmt.Errorf("%w: coverage subject has no structured identity", ErrInvalidValue)
		}
		key := subjectKey{kind: record.Kind, ref: record.Subject.Key()}
		if _, found := result.subjects[key]; found {
			return Coverage{}, fmt.Errorf("%w: subject coverage for %s", ErrDuplicate, record.Subject)
		}
		result.subjects[key] = record
	}
	return result, nil
}

func validateKindCoverage(representation Representation, record KindCoverage) error {
	model := record.Model
	if !model.Kind.Valid() || !Kind(model.Owner).Valid() || model.Version == 0 || model.Definition == "" || model.Representation != representation {
		return fmt.Errorf("%w: incomplete or mismatched coverage model", ErrInvalidValue)
	}
	if record.Knowledge.State == Absent || record.Knowledge.State == Defaulted {
		return fmt.Errorf("%w: absence/default assertions require a specific subject", ErrInvalidValue)
	}
	return validateKnowledge(representation, record.Knowledge)
}

func validateKnowledge(representation Representation, knowledge Knowledge) error {
	switch knowledge.State {
	case Uninspected, Unrepresentable:
		if strings.TrimSpace(knowledge.Reason) == "" {
			return fmt.Errorf("%w: unknown feature state requires a reason", ErrInvalidValue)
		}
	case Complete, Absent:
	case Defaulted:
		if representation != Desired {
			return fmt.Errorf("%w: an observation cannot request defaults", ErrInvalidValue)
		}
	default:
		return fmt.Errorf("%w: unknown knowledge state %q", ErrInvalidValue, knowledge.State)
	}
	return nil
}

// Lookup returns a subject override or the enrolled kind's claim. A kind the
// source did not enroll remains uninspected even if the current runtime knows it.
func (c Coverage) Lookup(kind Kind, subject objectidentity.ID) Knowledge {
	if record, found := c.subjects[subjectKey{kind: kind, ref: subject.Key()}]; found {
		return record.Knowledge
	}
	// Named feature objects use their value kind as their identity kind. Their
	// Parent component names a table; common column/index facets do not take
	// this path because their subject kind remains the common envelope kind.
	if subject.Kind == objectidentity.Kind(kind) && !subject.Parent.Empty() {
		parent := objectidentity.ID{Kind: objectidentity.KindTable, Catalog: subject.Catalog, Schema: subject.Schema, Name: subject.Parent}
		if record, found := c.subjects[subjectKey{kind: kind, ref: parent.Key()}]; found {
			return record.Knowledge
		}
	}
	if record, found := c.kinds[kind]; found {
		return record.Knowledge
	}
	return Knowledge{State: Uninspected, Reason: "this source did not describe the feature kind"}
}

// SubjectKnowledge returns only an explicit subject claim. A concrete observed
// object can be known even when enumeration of its namespace was incomplete;
// callers use this method to distinguish that case from an unreadable object.
func (c Coverage) SubjectKnowledge(kind Kind, subject objectidentity.ID) (Knowledge, bool) {
	record, found := c.subjects[subjectKey{kind: kind, ref: subject.Key()}]
	return record.Knowledge, found
}

// Representation reports the captured source direction. The zero value has no
// direction and makes no authority claims.
func (c Coverage) Representation() Representation { return c.representation }

// KindRecords returns independent claims ordered by kind.
func (c Coverage) KindRecords() []KindCoverage {
	result := make([]KindCoverage, 0, len(c.kinds))
	for _, kind := range slices.Sorted(maps.Keys(c.kinds)) {
		result = append(result, c.kinds[kind])
	}
	return result
}

// SubjectRecords returns independent overrides ordered by kind and identity.
func (c Coverage) SubjectRecords() []SubjectCoverage {
	result := slices.Collect(maps.Values(c.subjects))
	slices.SortFunc(result, func(a, b SubjectCoverage) int {
		if a.Kind < b.Kind {
			return -1
		}
		if a.Kind > b.Kind {
			return 1
		}
		return CompareRefs(a.Subject, b.Subject)
	})
	return result
}

// IsZero reports an empty account, which has no authority over feature absence.
func (c Coverage) IsZero() bool { return len(c.kinds) == 0 && len(c.subjects) == 0 }

// MarshalJSON requires registry validation of the captured model definitions.
func (Coverage) MarshalJSON() ([]byte, error) { return nil, ErrExplicitCodec }

// UnmarshalJSON refuses an account detached from its selected definitions.
func (*Coverage) UnmarshalJSON([]byte) error { return ErrExplicitCodec }
