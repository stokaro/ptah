package pgtable

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
)

// ExclusionKind identifies an independently named PostgreSQL EXCLUDE constraint.
const ExclusionKind schemaext.Kind = "ptah.run/postgres/exclusion-constraint"

// Exclusion describes the constraint's index, predicate, timing and comment.
// The owning table and constraint name belong to its structured object identity.
type Exclusion struct {
	// UsingMethod is the index access method in SQL identifier syntax.
	UsingMethod string `json:"using_method"`
	// Elements is the ordered expression and operator list without outer parentheses.
	Elements string `json:"elements"`
	// WhereCondition is the optional index predicate without outer parentheses.
	WhereCondition string `json:"where_condition,omitempty"`
	// Deferrable allows checking the constraint at transaction commit.
	Deferrable bool `json:"deferrable,omitempty"`
	// Initially is "immediate", "deferred", or empty for the declared default.
	Initially string `json:"initially,omitempty"`
	// Comment is the COMMENT ON CONSTRAINT text.
	Comment string `json:"comment,omitempty"`
	// Dependencies is a set of dependencies of the backing index, including extensions
	// whose operator classes the catalog definition does not spell out.
	Dependencies []objectidentity.ID `json:"requires,omitempty"`
}

// DesiredExclusion declares one EXCLUDE constraint. Source encoders preserve
// StructName and omit Normalized, which belongs to one comparison request.
type DesiredExclusion struct {
	Exclusion
	// StructName identifies the Go struct the declaration belongs to.
	StructName string `json:"struct_name,omitempty"`
	// Normalized records the connected server's expression spelling, bound to
	// the SQL definition it interprets. Nil uses the declared expressions.
	Normalized *NormalizedExclusion `json:"normalized,omitempty"`
}

// ObservedExclusion records a catalog constraint and its backing dependencies.
type ObservedExclusion Exclusion

// NormalizedExclusion is a server interpretation of a desired SQL definition.
// SourceKey is ExclusionComparisonKey before replacing the expressions.
type NormalizedExclusion struct {
	SourceKey      string `json:"source_key"`
	Elements       string `json:"elements"`
	WhereCondition string `json:"where_condition,omitempty"`
}

// SupportsExclusion reports whether this owner creates EXCLUDE constraints on
// target. Only PostgreSQL is such a target: planning refuses a declaration for
// any other, and a reader of a target outside it that cannot query
// pg_constraint records the namespace as described rather than unknown.
func SupportsExclusion(target string) bool {
	return platform.NormalizeDialect(target) == platform.Postgres
}

// ExclusionRef builds a table-scoped identity using the target's identifier rules.
// Quoted names retain their source spelling and component boundaries.
func ExclusionRef(table, name string, semantics identifier.Semantics) objectidentity.ID {
	ref := objectidentity.NewBuilder(semantics).Constraint(table, name)
	ref.Kind = objectidentity.Kind(ExclusionKind)
	return ref
}

// Kind returns the exclusion model identity.
func (*DesiredExclusion) Kind() schemaext.Kind { return ExclusionKind }

// Kind returns the exclusion model identity.
func (*ObservedExclusion) Kind() schemaext.Kind { return ExclusionKind }

// Clone returns an independent declaration. A nil receiver remains typed nil.
func (p *DesiredExclusion) Clone() schemaext.Value { return copyDesiredExclusion(p) }

func copyDesiredExclusion(p *DesiredExclusion) *DesiredExclusion {
	if p == nil {
		return (*DesiredExclusion)(nil)
	}
	cloned := *p
	cloned.Dependencies = slices.Clone(p.Dependencies)
	if p.Normalized != nil {
		cloned.Normalized = new(*p.Normalized)
	}
	return &cloned
}

// Clone returns an independent observation. A nil receiver remains typed nil.
func (p *ObservedExclusion) Clone() schemaext.Value { return copyObservedExclusion(p) }

func copyObservedExclusion(p *ObservedExclusion) *ObservedExclusion {
	if p == nil {
		return (*ObservedExclusion)(nil)
	}
	cloned := *p
	cloned.Dependencies = slices.Clone(p.Dependencies)
	return &cloned
}

// Equal compares declaration data, including source metadata and normalization.
// It does not evaluate SQL. Other representations are unequal.
func (p *DesiredExclusion) Equal(value schemaext.Value) bool {
	other, ok := value.(*DesiredExclusion)
	if !ok || p == nil || other == nil {
		return ok && p == nil && other == nil
	}
	return sameExclusion(p.Exclusion, other.Exclusion) && p.StructName == other.StructName && sameNormalizedExclusion(p.Normalized, other.Normalized)
}

// Equal compares catalog data without evaluating SQL. Other representations
// are unequal, and a nil observation equals only a typed nil observation.
func (p *ObservedExclusion) Equal(value schemaext.Value) bool {
	other, ok := value.(*ObservedExclusion)
	if !ok || p == nil || other == nil {
		return ok && p == nil && other == nil
	}
	return sameExclusion(Exclusion(*p), Exclusion(*other))
}

func sameExclusion(a, b Exclusion) bool {
	return a.UsingMethod == b.UsingMethod && a.Elements == b.Elements && a.WhereCondition == b.WhereCondition && a.Deferrable == b.Deferrable &&
		a.Initially == b.Initially && a.Comment == b.Comment && slices.Equal(sortedExclusionDependencies(a.Dependencies), sortedExclusionDependencies(b.Dependencies))
}

func sameNormalizedExclusion(a, b *NormalizedExclusion) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

// ValidateDesiredExclusion rejects absent definitions, invalid check timing,
// incomplete dependencies and stale normalization. Errors wrap
// schemaext.ErrInvalidValue. Expression semantics require a PostgreSQL server.
func ValidateDesiredExclusion(p *DesiredExclusion) error {
	if p == nil {
		return exclusionModelError(schemaext.Desired, "EXCLUDE requires a non-nil declaration")
	}
	if err := validateExclusion(p.Exclusion, schemaext.Desired); err != nil {
		return err
	}
	if p.Normalized != nil && (strings.TrimSpace(p.Normalized.Elements) == "" || p.Normalized.SourceKey != ExclusionComparisonKey(p.Exclusion)) {
		return exclusionModelError(schemaext.Desired, "normalized EXCLUDE expressions do not describe the declaration")
	}
	return nil
}

// ValidateObservedExclusion validates catalog data under the same structural
// rules as ValidateDesiredExclusion, with a definite initial check timing.
func ValidateObservedExclusion(p *ObservedExclusion) error {
	if p == nil {
		return exclusionModelError(schemaext.Observed, "EXCLUDE requires a non-nil observation")
	}
	if err := validateExclusion(Exclusion(*p), schemaext.Observed); err != nil {
		return err
	}
	if p.Deferrable && p.Initially == "" || !p.Deferrable && p.Initially != "" {
		return exclusionModelError(schemaext.Observed, "observed EXCLUDE timing must match its deferral state")
	}
	return nil
}

func validateExclusion(p Exclusion, representation schemaext.Representation) error {
	if strings.TrimSpace(p.UsingMethod) == "" || strings.TrimSpace(p.Elements) == "" {
		return exclusionModelError(representation, "EXCLUDE requires an index method and elements")
	}
	if p.Initially != "" && p.Initially != "immediate" && p.Initially != "deferred" {
		return exclusionModelError(representation, "EXCLUDE initial timing must be immediate or deferred")
	}
	if p.Initially == "deferred" && !p.Deferrable {
		return exclusionModelError(representation, "INITIALLY DEFERRED requires DEFERRABLE")
	}
	seen := make(map[objectidentity.Key]bool, len(p.Dependencies))
	for _, ref := range p.Dependencies {
		if strings.TrimSpace(string(ref.Kind)) == "" || ref.Name.Source == "" || ref.Name.Normalized == "" {
			return exclusionModelError(representation, "EXCLUDE dependency requires an object kind and name")
		}
		if seen[ref.Key()] {
			return exclusionModelError(representation, "EXCLUDE dependency identity is duplicated")
		}
		seen[ref.Key()] = true
		for _, part := range []objectidentity.Part{ref.Catalog, ref.Schema, ref.Parent, ref.Name} {
			if part != (objectidentity.Part{}) && (part.Source == "" || part.Normalized == "") {
				return exclusionModelError(representation, "EXCLUDE dependency component requires source and normalized names")
			}
		}
	}
	return nil
}

func sortedExclusionDependencies(refs []objectidentity.ID) []objectidentity.ID {
	result := slices.Clone(refs)
	slices.SortFunc(result, schemaext.CompareRefs)
	return result
}

func exclusionModelError(representation schemaext.Representation, message string) error {
	return &schemaext.InvalidModelError{Kind: ExclusionKind, Representation: representation, Message: message}
}

// ExclusionComparisonKey compares lexical SQL definitions and effective timing.
// It ignores comments, source metadata and dependency observations. It does not
// establish equivalence of expressions that require a server's interpretation.
func ExclusionComparisonKey(p Exclusion) string {
	initially := ""
	if p.Deferrable {
		initially = "immediate"
		if p.Initially == "deferred" {
			initially = "deferred"
		}
	}
	return fmt.Sprintf("%q/%q/%q/%t/%q", partitionExpressionKey(p.UsingMethod), partitionExpressionKey(p.Elements), partitionExpressionKey(p.WhereCondition), p.Deferrable, initially)
}

// ExclusionTable returns the table identity carried by an exclusion reference.
// It retains resolved catalog and schema components, including default markers.
func ExclusionTable(ref objectidentity.ID) objectidentity.ID {
	return objectidentity.ID{Kind: objectidentity.KindTable, Catalog: ref.Catalog, Schema: ref.Schema, Name: ref.Parent}
}

// ValidateExclusionRef requires this model's kind, a named table and constraint,
// and no signature. Each present identity component needs its source spelling
// and normalized name. Errors wrap schemaext.ErrInvalidValue.
func ValidateExclusionRef(ref objectidentity.ID) error {
	if ref.Kind != objectidentity.Kind(ExclusionKind) || ref.Parent.Source == "" || ref.Parent.Normalized == "" || ref.Name.Source == "" || ref.Name.Normalized == "" || ref.Signature != "" {
		return exclusionModelError(schemaext.Desired, "EXCLUDE requires a table-scoped constraint identity")
	}
	for _, part := range []objectidentity.Part{ref.Catalog, ref.Schema} {
		if part != (objectidentity.Part{}) && (part.Source == "" || part.Normalized == "") {
			return exclusionModelError(schemaext.Desired, "EXCLUDE scope requires source and normalized names")
		}
	}
	return nil
}

func comparedExclusion(p *DesiredExclusion) Exclusion {
	result := p.Exclusion
	if p.Normalized != nil {
		result.Elements, result.WhereCondition = p.Normalized.Elements, p.Normalized.WhereCondition
	}
	return result
}

// Requires returns the declaration's captured object dependencies in identity
// order. The returned slice is independent; a nil receiver requires nothing.
func (p *DesiredExclusion) Requires() []objectidentity.ID {
	if p == nil {
		return nil
	}
	return sortedExclusionDependencies(p.Dependencies)
}
