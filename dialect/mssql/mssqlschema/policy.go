package mssqlschema

import (
	"cmp"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
)

// PredicateType is what a predicate does to the rows of its table.
type PredicateType string

const (
	// Filter silently leaves out of a read the rows its function admits no
	// row for.
	Filter PredicateType = "FILTER"
	// Block refuses a write whose rows its function admits no row for.
	Block PredicateType = "BLOCK"
)

// BlockOperation is the write a block predicate is evaluated for, and whether
// on the rows before or after it. AFTER UPDATE and BEFORE UPDATE are
// different predicates: the first checks the new values, the second the old.
type BlockOperation string

const (
	// AfterInsert checks the rows an INSERT writes.
	AfterInsert BlockOperation = "AFTER INSERT"
	// AfterUpdate checks the values an UPDATE writes.
	AfterUpdate BlockOperation = "AFTER UPDATE"
	// BeforeUpdate checks the values an UPDATE replaces.
	BeforeUpdate BlockOperation = "BEFORE UPDATE"
	// BeforeDelete checks the rows a DELETE removes.
	BeforeDelete BlockOperation = "BEFORE DELETE"
)

// ObjectName is a schema-qualified name as SQL Server stores it: both parts
// are required, and each keeps its exact text.
type ObjectName struct {
	Schema string `json:"schema"`
	Name   string `json:"name"`
}

// Predicate binds one inline table-valued function to one table.
//
// Arguments are the function's arguments in its parameter order, each a
// column name of Table or an expression, as the statement writes them. A block
// predicate with no Operation is evaluated for every write; a filter
// predicate never has one.
type Predicate struct {
	Type      PredicateType  `json:"type"`
	Function  ObjectName     `json:"function"`
	Arguments []string       `json:"arguments,omitempty"`
	Table     ObjectName     `json:"table"`
	Operation BlockOperation `json:"operation,omitempty"`
}

// DesiredSecurityPolicy declares one SQL Server security policy. Its schema
// and name are its object identity (see [SecurityPolicyRef]), never fields
// here.
//
// An omitted value requests SQL Server's documented default: nil Enabled is
// STATE = ON, and nil SchemaBinding is SCHEMABINDING = ON.
type DesiredSecurityPolicy struct {
	// Predicates are the policy's bindings. Order carries no meaning, so
	// equality and the canonical encoding treat them as a set.
	Predicates    []Predicate `json:"predicates"`
	Enabled       *bool       `json:"enabled,omitempty"`
	SchemaBinding *bool       `json:"schema_binding,omitempty"`
	// NotForReplication leaves the policy out of the writes a replication
	// agent makes.
	NotForReplication bool `json:"not_for_replication,omitempty"`
	// StructName preserves the Go struct a declaration was read from. It has no
	// server counterpart and does not change the policy.
	StructName string `json:"struct_name,omitempty"`
}

// ObservedSecurityPolicy is a policy as sys.security_policies and
// sys.security_predicates report it. Every value is definite: a reader that
// cannot establish one records a coverage limit for the policy instead of
// filling in a default.
type ObservedSecurityPolicy struct {
	Predicates        []Predicate `json:"predicates"`
	Enabled           bool        `json:"enabled"`
	SchemaBinding     bool        `json:"schema_binding"`
	NotForReplication bool        `json:"not_for_replication"`
}

// Kind returns the security policy model identity.
func (*DesiredSecurityPolicy) Kind() schemaext.Kind { return SecurityPolicyKind }

// Kind returns the security policy model identity.
func (*ObservedSecurityPolicy) Kind() schemaext.Kind { return SecurityPolicyKind }

// Clone returns an independent declaration. A nil receiver remains typed nil.
func (v *DesiredSecurityPolicy) Clone() schemaext.Value { return v.Copy() }

// Copy is [DesiredSecurityPolicy.Clone] without the interface: it shares no
// slice or pointer with v, and a nil receiver returns nil.
func (v *DesiredSecurityPolicy) Copy() *DesiredSecurityPolicy {
	if v == nil {
		return nil
	}
	cloned := *v
	cloned.Predicates = clonePredicates(v.Predicates)
	cloned.Enabled, cloned.SchemaBinding = cloneFlag(v.Enabled), cloneFlag(v.SchemaBinding)
	return &cloned
}

// Clone returns an independent observation. A nil receiver remains typed nil.
func (v *ObservedSecurityPolicy) Clone() schemaext.Value { return v.Copy() }

// Copy is [ObservedSecurityPolicy.Clone] without the interface: it shares no
// slice with v, and a nil receiver returns nil.
func (v *ObservedSecurityPolicy) Copy() *ObservedSecurityPolicy {
	if v == nil {
		return nil
	}
	cloned := *v
	cloned.Predicates = clonePredicates(v.Predicates)
	return &cloned
}

// Equal compares declarations field by field, the predicates as a set,
// without resolving defaults: an omitted state is not equal to STATE = ON
// here.
func (v *DesiredSecurityPolicy) Equal(other schemaext.Value) bool {
	right, ok := other.(*DesiredSecurityPolicy)
	if !ok || v == nil || right == nil {
		return ok && v == nil && right == nil
	}
	return samePredicates(v.Predicates, right.Predicates) && equalFlag(v.Enabled, right.Enabled) &&
		equalFlag(v.SchemaBinding, right.SchemaBinding) && v.NotForReplication == right.NotForReplication &&
		v.StructName == right.StructName
}

// Equal compares observations field by field, the predicates as a set.
func (v *ObservedSecurityPolicy) Equal(other schemaext.Value) bool {
	right, ok := other.(*ObservedSecurityPolicy)
	if !ok || v == nil || right == nil {
		return ok && v == nil && right == nil
	}
	return samePredicates(v.Predicates, right.Predicates) && v.Enabled == right.Enabled &&
		v.SchemaBinding == right.SchemaBinding && v.NotForReplication == right.NotForReplication
}

// SecurityPolicyRef builds a policy identity from its schema and name under
// SQL Server's offline identifier rules. A policy belongs to its schema, so
// the identity has no table parent: one policy may bind several tables. An
// empty schema takes the default and is marked as defaulted.
//
// The parts are the names the catalog stores and are used as they are,
// without trimming: a bracketed name keeps its spaces.
func SecurityPolicyRef(schema, name string) objectidentity.ID {
	return SecurityPolicyRefWith(identifier.ForDialect(platform.SQLServer), schema, name)
}

// SecurityPolicyRefWith builds the identity under explicit identifier rules,
// which is how a comparison pairs a declaration and an observation under a
// database's collation. Like [SecurityPolicyRef], it does not trim the parts.
func SecurityPolicyRefWith(semantics identifier.Semantics, schema, name string) objectidentity.ID {
	ref := objectidentity.NewBuilder(semantics).TablePartsVerbatim(schema, name)
	ref.Kind = objectidentity.Kind(SecurityPolicyKind)
	return ref
}

// ValidateSecurityPolicyRef requires an identity of this kind with a schema
// and a name, and no parent, catalog or signature.
func ValidateSecurityPolicyRef(ref objectidentity.ID) error {
	if ref.Kind != objectidentity.Kind(SecurityPolicyKind) || ref.Name.Source == "" || ref.Name.Normalized == "" ||
		ref.Schema.Source == "" || ref.Schema.Normalized == "" ||
		!ref.Parent.Empty() || !ref.Catalog.Empty() || ref.Signature != "" {
		return fmt.Errorf("%w: a security policy requires a schema and a name, and no table parent", schemaext.ErrInvalidValue)
	}
	return nil
}

// DesiredSecurityPolicyObject captures one authored policy under ref.
func DesiredSecurityPolicyObject(ref objectidentity.ID, policy DesiredSecurityPolicy) (schemaext.Object, error) {
	if err := ValidateSecurityPolicyRef(ref); err != nil {
		return schemaext.Object{}, err
	}
	if err := ValidateDesiredSecurityPolicy(&policy); err != nil {
		return schemaext.Object{}, err
	}
	return schemaext.Object{Ref: ref, Value: policy.Copy()}, nil
}

// ObservedSecurityPolicyObject captures one policy a read reported under ref.
func ObservedSecurityPolicyObject(ref objectidentity.ID, policy ObservedSecurityPolicy) (schemaext.Object, error) {
	if err := ValidateSecurityPolicyRef(ref); err != nil {
		return schemaext.Object{}, err
	}
	if err := ValidateObservedSecurityPolicy(&policy); err != nil {
		return schemaext.Object{}, err
	}
	return schemaext.Object{Ref: ref, Value: policy.Copy()}, nil
}

// ValidateDesiredSecurityPolicy checks representation invariants: each
// predicate well formed, and no two predicates for one operation on one
// table, which SQL Server refuses. A policy may hold no predicate: CREATE
// SECURITY POLICY without one succeeds, measured on SQL Server 2025.
func ValidateDesiredSecurityPolicy(v *DesiredSecurityPolicy) error {
	if v == nil {
		return modelError(schemaext.Desired, fmt.Errorf("%w: nil security policy declaration", schemaext.ErrInvalidValue))
	}
	if err := validatePredicates(v.Predicates); err != nil {
		return modelError(schemaext.Desired, err)
	}
	if err := validText("struct name", v.StructName); err != nil {
		return modelError(schemaext.Desired, err)
	}
	return nil
}

// ValidateObservedSecurityPolicy checks that each predicate of an observation
// is well formed and that no two cover one operation on one table. An
// observation may hold no predicate, as a declaration may.
func ValidateObservedSecurityPolicy(v *ObservedSecurityPolicy) error {
	if v == nil {
		return modelError(schemaext.Observed, fmt.Errorf("%w: nil security policy observation", schemaext.ErrInvalidValue))
	}
	if err := validatePredicates(v.Predicates); err != nil {
		return modelError(schemaext.Observed, err)
	}
	return nil
}

// validatePredicates checks each predicate and the predicates of each table
// together. SQL Server allows at most one predicate of a policy for a
// particular operation against a particular table: one filter, and one block
// predicate per operation, where a block predicate without an operation
// covers them all. Measured on SQL Server 2025, either second predicate is
// refused with Msg 33262.
func validatePredicates(predicates []Predicate) error {
	type slot struct {
		table     ObjectName
		kind      PredicateType
		operation BlockOperation
	}
	seen := make(map[slot]bool, len(predicates))
	for _, predicate := range predicates {
		if err := validatePredicate(predicate); err != nil {
			return err
		}
		key := slot{table: predicate.Table, kind: predicate.Type, operation: predicate.Operation}
		if seen[key] {
			return fmt.Errorf("%w: table %s has two %s predicates", schemaext.ErrInvalidValue, predicate.Table, describe(predicate))
		}
		seen[key] = true
	}
	for _, predicate := range predicates {
		if predicate.Type == Block && predicate.Operation == "" && blockCount(predicates, predicate.Table) > 1 {
			return fmt.Errorf("%w: table %s has a BLOCK predicate for every operation beside another BLOCK predicate", schemaext.ErrInvalidValue, predicate.Table)
		}
	}
	return nil
}

func blockCount(predicates []Predicate, table ObjectName) int {
	n := 0
	for _, predicate := range predicates {
		if predicate.Type == Block && predicate.Table == table {
			n++
		}
	}
	return n
}

// validatePredicate checks one binding: a known type, an operation only on a
// block predicate and only one of the four, qualified function and table
// names, and arguments that are not empty.
func validatePredicate(predicate Predicate) error {
	switch predicate.Type {
	case Filter:
		if predicate.Operation != "" {
			return fmt.Errorf("%w: a FILTER predicate takes no operation, got %q", schemaext.ErrInvalidValue, predicate.Operation)
		}
	case Block:
		if predicate.Operation != "" && !slices.Contains([]BlockOperation{AfterInsert, AfterUpdate, BeforeUpdate, BeforeDelete}, predicate.Operation) {
			return fmt.Errorf("%w: unknown BLOCK predicate operation %q", schemaext.ErrInvalidValue, predicate.Operation)
		}
	default:
		return fmt.Errorf("%w: unknown predicate type %q", schemaext.ErrInvalidValue, predicate.Type)
	}
	if err := validName("function", predicate.Function); err != nil {
		return err
	}
	if err := validName("table", predicate.Table); err != nil {
		return err
	}
	for _, argument := range predicate.Arguments {
		if strings.TrimSpace(argument) == "" {
			return fmt.Errorf("%w: a predicate argument cannot be empty", schemaext.ErrInvalidValue)
		}
		if err := validText("predicate argument", argument); err != nil {
			return err
		}
	}
	return nil
}

// validName requires both parts of a qualified name, which SQL Server binds by
// schema.
func validName(field string, name ObjectName) error {
	if name.Schema == "" || name.Name == "" {
		return fmt.Errorf("%w: a predicate %s needs its schema and its name", schemaext.ErrInvalidValue, field)
	}
	if err := validText(field+" schema", name.Schema); err != nil {
		return err
	}
	return validText(field+" name", name.Name)
}

// describe names a predicate's slot for messages: FILTER, or BLOCK with its
// operation.
func describe(predicate Predicate) string {
	if predicate.Operation == "" {
		return string(predicate.Type)
	}
	return string(predicate.Type) + " " + string(predicate.Operation)
}

// String renders a name for messages as schema.name, each part bracketed.
func (n ObjectName) String() string {
	return "[" + n.Schema + "].[" + n.Name + "]"
}

// comparePredicates orders predicates by table, type, operation, function and
// arguments, each by its bytes.
func comparePredicates(a, b Predicate) int {
	return cmp.Or(
		cmp.Compare(a.Table.Schema, b.Table.Schema), cmp.Compare(a.Table.Name, b.Table.Name),
		cmp.Compare(a.Type, b.Type), cmp.Compare(a.Operation, b.Operation),
		cmp.Compare(a.Function.Schema, b.Function.Schema), cmp.Compare(a.Function.Name, b.Function.Name),
		slices.Compare(a.Arguments, b.Arguments),
	)
}

// sortedPredicates returns predicates in their canonical order without
// changing predicates.
func sortedPredicates(predicates []Predicate) []Predicate {
	sorted := clonePredicates(predicates)
	slices.SortFunc(sorted, comparePredicates)
	return sorted
}

// samePredicates compares two predicate lists as sets.
func samePredicates(left, right []Predicate) bool {
	if len(left) != len(right) {
		return false
	}
	return slices.EqualFunc(sortedPredicates(left), sortedPredicates(right), func(a, b Predicate) bool {
		return comparePredicates(a, b) == 0
	})
}

func clonePredicates(predicates []Predicate) []Predicate {
	if predicates == nil {
		return nil
	}
	cloned := slices.Clone(predicates)
	for i := range cloned {
		cloned[i].Arguments = slices.Clone(predicates[i].Arguments)
	}
	return cloned
}

func cloneFlag(value *bool) *bool {
	if value == nil {
		return nil
	}
	return new(*value)
}

func equalFlag(left, right *bool) bool {
	if left == nil || right == nil {
		return left == right
	}
	return *left == *right
}
