package schemaext

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"unicode/utf8"

	"ptah.run/core/objectidentity"
)

// KnowledgeDomain is an immutable description of namespace knowledge. Each
// source has an outside claim and bounded regions. Intersected sources must all
// establish completeness; a partial source cannot borrow another's authority.
// Its zero value describes no namespace and is not an enrolled model claim.
type KnowledgeDomain struct {
	sources []knowledgeSource
}

type knowledgeSource struct {
	Outside Knowledge            `json:"outside"`
	Regions []NamespaceKnowledge `json:"regions,omitempty"`
}

const maxKnowledgeSources = 256
const maxKnowledgeRegions = 4096

// GlobalKnowledge describes an unrestricted namespace. NewCoverage validates
// the claim when enrolling a model, including refusals of namespace-wide absence
// and defaults. This constructor captures values and retains no writable input.
func GlobalKnowledge(knowledge Knowledge) KnowledgeDomain {
	return KnowledgeDomain{sources: []knowledgeSource{{Outside: knowledge}}}
}

// NewScopedKnowledge captures an outside claim and bounded namespace claims.
// Matching regions combine conservatively; an unknown or unrepresentable region
// takes priority over a complete overlapping region. An explicit subject claim
// in Coverage remains more specific than any namespace claim.
func NewScopedKnowledge(outside Knowledge, regions ...NamespaceKnowledge) (KnowledgeDomain, error) {
	return normalizeKnowledgeDomain([]knowledgeSource{{Outside: outside, Regions: regions}})
}

// Lookup reports namespace knowledge for subject. Project target-qualified
// claims with ForTarget before using them for a selected engine, as for Coverage.
func (d KnowledgeDomain) Lookup(subject objectidentity.ID) Knowledge {
	if len(d.sources) == 0 {
		return Knowledge{State: Uninspected, Reason: "this source describes no namespace"}
	}
	result := Knowledge{State: Complete}
	for _, source := range d.sources {
		current, matched := Knowledge{State: Complete}, false
		for _, region := range source.Regions {
			if region.Scope.matches(subject) {
				current = intersectNamespaceKnowledge(current, region.Knowledge)
				matched = true
			}
		}
		if !matched {
			current = source.Outside
		}
		result = intersectNamespaceKnowledge(result, current)
	}
	return result
}

// Unscoped returns the conservative claim without selecting a namespace. A
// complete subspace cannot make this answer complete outside its bounds.
func (d KnowledgeDomain) Unscoped() Knowledge {
	if len(d.sources) == 0 {
		return Knowledge{State: Uninspected, Reason: "this source describes no namespace"}
	}
	result := Knowledge{State: Complete}
	for _, source := range d.sources {
		result = intersectNamespaceKnowledge(result, source.Outside)
		for _, region := range source.Regions {
			result = intersectNamespaceKnowledge(result, region.Knowledge)
		}
	}
	return result
}

// IsUninspected reports whether every captured claim is uninspected. A domain
// with complete regions does not qualify even when its outside is unknown.
func (d KnowledgeDomain) IsUninspected() bool {
	for _, source := range d.sources {
		if source.Outside.State != Uninspected {
			return false
		}
		for _, region := range source.Regions {
			if region.Knowledge.State != Uninspected {
				return false
			}
		}
	}
	return true
}

// Equal compares the captured claims, including source spellings and scope
// boundaries. Intersections and region order have deterministic representations.
func (d KnowledgeDomain) Equal(other KnowledgeDomain) bool {
	a, err := json.Marshal(d.sources)
	if err != nil {
		return false
	}
	b, err := json.Marshal(other.sources)
	return err == nil && bytes.Equal(a, b)
}

// Intersect retains pointwise knowledge from both sources. Sources remain
// separate so disjoint engine or database claims cannot be unioned into a
// complete namespace. Duplicate sources do not accumulate on repeated merges.
func (d KnowledgeDomain) Intersect(other KnowledgeDomain) (KnowledgeDomain, error) {
	left, right := d.sources, other.sources
	if len(left) == 0 {
		left = GlobalKnowledge(d.Unscoped()).sources
	}
	if len(right) == 0 {
		right = GlobalKnowledge(other.Unscoped()).sources
	}
	return normalizeKnowledgeDomain(slices.Concat(left, right))
}

// ForTarget retains each source's outside knowledge and only regions selected
// by target. An excluded complete region cannot affect the resulting namespace.
func (d KnowledgeDomain) ForTarget(target TargetSelection) (KnowledgeDomain, error) {
	if err := target.Validate(); err != nil {
		return KnowledgeDomain{}, err
	}
	sources := make([]knowledgeSource, len(d.sources))
	for i, source := range d.sources {
		sources[i].Outside = source.Outside
		for _, region := range source.Regions {
			if target.Includes(region.Scope.Targets) {
				sources[i].Regions = append(sources[i].Regions, region)
			}
		}
	}
	return normalizeKnowledgeDomain(sources)
}

// HasNamespaceScopes reports whether any claim carries an explicit boundary.
// A format that only preserves global knowledge must refuse such a domain.
func (d KnowledgeDomain) HasNamespaceScopes() bool {
	for _, source := range d.sources {
		if len(source.Regions) != 0 {
			return true
		}
	}
	return false
}

// HasTargetScopes reports a namespace claim bound to explicit target names.
func (d KnowledgeDomain) HasTargetScopes() bool {
	for _, source := range d.sources {
		for _, region := range source.Regions {
			if len(region.Scope.Targets) != 0 {
				return true
			}
		}
	}
	return false
}

// MarshalJSON serializes the captured sources, not a flattened absence claim.
func (d KnowledgeDomain) MarshalJSON() ([]byte, error) {
	if err := d.validate(); err != nil {
		return nil, err
	}
	return json.Marshal(d.sources)
}

// UnmarshalJSON restores explicit sources and refuses scalar knowledge records.
func (d *KnowledgeDomain) UnmarshalJSON(data []byte) error {
	if !utf8.Valid(data) {
		return fmt.Errorf("%w: namespace knowledge JSON is not UTF-8", ErrInvalidValue)
	}
	shape, err := DecodeJSON[any](data)
	if err != nil {
		return err
	}
	if err := validateCoverageHeaderAliases(shape); err != nil {
		return err
	}
	sources, err := DecodeJSON[[]knowledgeSource](data)
	if err != nil {
		return err
	}
	result, err := normalizeKnowledgeDomain(sources)
	if err != nil {
		return err
	}
	*d = result
	return nil
}

func normalizeKnowledgeDomain(input []knowledgeSource) (KnowledgeDomain, error) {
	if len(input) == 0 || len(input) > 2*maxKnowledgeSources {
		return KnowledgeDomain{}, fmt.Errorf("%w: namespace knowledge source count is outside the supported bounds", ErrInvalidValue)
	}
	sources := make([]knowledgeSource, len(input))
	for i, source := range input {
		if err := validateNamespaceKnowledge(source.Outside); err != nil {
			return KnowledgeDomain{}, err
		}
		if len(source.Regions) > maxKnowledgeRegions {
			return KnowledgeDomain{}, fmt.Errorf("%w: too many namespace regions", ErrInvalidValue)
		}
		sources[i].Outside = source.Outside
		for _, region := range source.Regions {
			scope, err := region.Scope.normalized()
			if err != nil {
				return KnowledgeDomain{}, err
			}
			if err := validateNamespaceKnowledge(region.Knowledge); err != nil {
				return KnowledgeDomain{}, err
			}
			sources[i].Regions = append(sources[i].Regions, NamespaceKnowledge{Scope: scope, Knowledge: region.Knowledge})
		}
		slices.SortFunc(sources[i].Regions, compareKnowledgeJSON[NamespaceKnowledge])
		sources[i].Regions = slices.CompactFunc(sources[i].Regions, equalKnowledgeJSON[NamespaceKnowledge])
	}
	slices.SortFunc(sources, compareKnowledgeJSON[knowledgeSource])
	sources = slices.CompactFunc(sources, equalKnowledgeJSON[knowledgeSource])
	if len(sources) > maxKnowledgeSources {
		return KnowledgeDomain{}, fmt.Errorf("%w: too many namespace knowledge sources", ErrInvalidValue)
	}
	return KnowledgeDomain{sources: sources}, nil
}

func (d KnowledgeDomain) validate() error {
	_, err := normalizeKnowledgeDomain(d.sources)
	return err
}

func validateNamespaceKnowledge(knowledge Knowledge) error {
	if !utf8.ValidString(knowledge.Reason) {
		return fmt.Errorf("%w: namespace knowledge reason is not UTF-8", ErrInvalidValue)
	}
	if knowledge.State == Absent || knowledge.State == Defaulted {
		return fmt.Errorf("%w: absence and defaults require a specific subject", ErrInvalidValue)
	}
	return validateKnowledge(Observed, knowledge)
}

func intersectNamespaceKnowledge(a, b Knowledge) Knowledge {
	result, err := mergeKnowledge(a, b)
	if err != nil {
		return Knowledge{State: Unrepresentable, Reason: err.Error()}
	}
	return result
}

// The concrete source and scope records contain no values with fallible JSON
// encoders. JSON is only a deterministic structural ordering here, never an
// owner-defined model projection or an identifier comparison key.
func compareKnowledgeJSON[T any](a, b T) int {
	left, _ := json.Marshal(a)
	right, _ := json.Marshal(b)
	return bytes.Compare(left, right)
}

func equalKnowledgeJSON[T any](a, b T) bool { return compareKnowledgeJSON(a, b) == 0 }

// withLimit preserves more specific unknown reasons while limiting every
// complete region, including regions beneath an already unknown outside.
func (d KnowledgeDomain) withLimit(reason string) (KnowledgeDomain, error) {
	sources := make([]knowledgeSource, len(d.sources))
	limit := func(k Knowledge) Knowledge {
		if k.State == Complete {
			return Knowledge{State: Uninspected, Reason: reason}
		}
		return k
	}
	for i, source := range d.sources {
		sources[i].Outside = limit(source.Outside)
		sources[i].Regions = slices.Clone(source.Regions)
		for j := range sources[i].Regions {
			sources[i].Regions[j].Knowledge = limit(sources[i].Regions[j].Knowledge)
		}
	}
	return normalizeKnowledgeDomain(sources)
}
