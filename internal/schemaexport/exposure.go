package schemaexport

import (
	"fmt"
	"slices"
	"strings"

	"go.5x5.cz/ptah/core/goschema"
)

// Shape is the direction of an API contract: what a caller reads back, or what
// a caller sends.
//
// It exists because a column can belong to one and not the other. A
// server-owned identifier is readable and not writable; a credential is
// writable and must never be readable. One visibility for both cannot say
// either (stokaro/ptah#904).
type Shape string

// The two contract directions. Every exporter asks for one at a time.
const (
	// ShapeRead is what an API returns.
	ShapeRead Shape = "read"
	// ShapeWrite is what an API accepts.
	ShapeWrite Shape = "write"
)

// Exposure is the declared API visibility of one column.
//
// It is deliberately NOT called a projection. That word already means two other
// things here: [ProjectedField] substitutes a column's api_type, and the
// GraphQL renderer calls its input shape a write projection. A third meaning
// would make every one of them ambiguous.
type Exposure string

// The declared values of the api_expose attribute, plus the absent one.
//
// ExposureUnset is the state of every column that says nothing, which is every
// column in every schema written before this existed. What it RESOLVES to is
// the policy's decision, not this type's -- see [FieldPolicy].
const (
	ExposureUnset     Exposure = ""
	ExposureNone      Exposure = "none"
	ExposureRead      Exposure = "read"
	ExposureWrite     Exposure = "write"
	ExposureReadWrite Exposure = "read-write"
)

var declaredExposures = []Exposure{ExposureNone, ExposureRead, ExposureWrite, ExposureReadWrite}

// ParseExposure resolves a declared api_expose value.
//
// An unrecognized value is refused rather than treated as absent. Silently
// ignoring it would publish a column the author wrote a declaration to hide,
// which is the one outcome this attribute exists to prevent.
func ParseExposure(value string) (Exposure, error) {
	trimmed := Exposure(strings.TrimSpace(value))
	if trimmed == ExposureUnset {
		return ExposureUnset, nil
	}
	if slices.Contains(declaredExposures, trimmed) {
		return trimmed, nil
	}
	return "", fmt.Errorf("unknown api_expose value %q: valid values are %s",
		value, joinExposures(declaredExposures))
}

func joinExposures(values []Exposure) string {
	names := make([]string, 0, len(values))
	for _, value := range values {
		names = append(names, string(value))
	}
	return strings.Join(names, ", ")
}

// covers reports whether a declared exposure reaches a contract shape.
func (e Exposure) covers(shape Shape) bool {
	switch e {
	case ExposureReadWrite:
		return true
	case ExposureRead:
		return shape == ShapeRead
	case ExposureWrite:
		return shape == ShapeWrite
	default:
		return false
	}
}

// FieldPolicy decides what an UNDECLARED column means. It is the whole
// difference between a convenience and a boundary.
type FieldPolicy string

const (
	// FieldPolicyAll exports every column of an exported table unless it
	// declares otherwise. It is the default and the behavior every schema had
	// before this existed, so adding the attribute changes nothing until an
	// author uses it.
	FieldPolicyAll FieldPolicy = "all"
	// FieldPolicyAllowlist exports only columns that declare an exposure. An
	// undeclared column is not exported, and is reported.
	//
	// This is what makes an additive migration safe: a column added to the
	// database tomorrow enters no contract until somebody says it should.
	FieldPolicyAllowlist FieldPolicy = "allowlist"
)

var declaredFieldPolicies = []FieldPolicy{FieldPolicyAll, FieldPolicyAllowlist}

// ParseFieldPolicy resolves the --api-field-policy value.
func ParseFieldPolicy(value string) (FieldPolicy, error) {
	trimmed := FieldPolicy(strings.TrimSpace(value))
	if trimmed == "" {
		return FieldPolicyAll, nil
	}
	if slices.Contains(declaredFieldPolicies, trimmed) {
		return trimmed, nil
	}
	names := make([]string, 0, len(declaredFieldPolicies))
	for _, policy := range declaredFieldPolicies {
		names = append(names, string(policy))
	}
	return "", fmt.Errorf("unknown API field policy %q: valid values are %s",
		value, strings.Join(names, ", "))
}

// ExposedFields returns the columns of a table that reach one contract shape,
// with a diagnostic for every column the policy withheld.
//
// It is the single place the decision is made. Every exporter reaches its
// columns through here, so OpenAPI, GraphQL and Protobuf cannot disagree about
// what is published -- which is the requirement, not an implementation
// convenience (stokaro/ptah#904).
//
// The diagnostics name the table and the column because a policy that hides
// something without saying so is indistinguishable from a schema that never had
// it.
func ExposedFields(
	db *goschema.Database,
	table goschema.Table,
	shape Shape,
	policy FieldPolicy,
) ([]goschema.Field, []Diagnostic, error) {
	fields := FieldsFor(db, table)
	exposed := make([]goschema.Field, 0, len(fields))
	diagnostics := make([]Diagnostic, 0)
	for _, field := range fields {
		declared, err := ParseExposure(field.APIExpose)
		if err != nil {
			return nil, nil, fmt.Errorf("table %q column %q: %w", table.Name, field.Name, err)
		}
		keep, diagnostic := decideExposure(table, field, declared, shape, policy)
		if diagnostic != nil {
			diagnostics = append(diagnostics, *diagnostic)
		}
		if keep {
			exposed = append(exposed, field)
		}
	}
	return exposed, diagnostics, nil
}

// decideExposure answers one column, and reports the withholding an author
// would otherwise have to infer from an absence.
func decideExposure(
	table goschema.Table,
	field goschema.Field,
	declared Exposure,
	shape Shape,
	policy FieldPolicy,
) (keep bool, diagnostic *Diagnostic) {
	path := table.Name + "." + field.Name
	if declared == ExposureUnset {
		if policy == FieldPolicyAll {
			return true, nil
		}
		return false, &Diagnostic{
			Severity: SeverityWarning,
			Path:     path,
			Message: fmt.Sprintf(
				"column declares no api_expose and the field policy is %s, so it is not exported; "+
					"declare api_expose to publish it", FieldPolicyAllowlist),
		}
	}
	if declared.covers(shape) {
		return true, nil
	}
	return false, &Diagnostic{
		Severity: SeverityWarning,
		Path:     path,
		Message: fmt.Sprintf("column declares api_expose=%q, so it is omitted from the %s shape",
			declared, shape),
	}
}

// ExposesAnyShape reports whether a column reaches either contract, which is
// what a cross-shape concern such as a Protobuf field number depends on.
func ExposesAnyShape(field goschema.Field, policy FieldPolicy) bool {
	declared, err := ParseExposure(field.APIExpose)
	if err != nil {
		return false
	}
	if declared == ExposureUnset {
		return policy == FieldPolicyAll
	}
	return declared.covers(ShapeRead) || declared.covers(ShapeWrite)
}
