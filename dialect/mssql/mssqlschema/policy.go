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
	// Normalized is the connected server's spelling of Predicates, one for
	// each, as sys.security_predicates reports them for a policy created from
	// this declaration. A normalization probe attaches it and a comparison
	// reads it in place of the declared arguments; nothing renders it. Nil
	// means no server answered.
	Normalized []Predicate `json:"normalized,omitempty"`
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
	cloned.Normalized = clonePredicates(v.Normalized)
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
		v.StructName == right.StructName && samePredicates(v.Normalized, right.Normalized)
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

// ValidateSecurityPolicyRef requires an identity of this kind with no parent,
// catalog or signature, and a schema and a name that are valid text holding
// more than white space. SQL Server accepts a policy named `[ ]`, measured on
// SQL Server 2025, but compares names without their trailing spaces, so such
// a name equals the empty one; it is refused here. The refusal wraps
// [schemaext.ErrInvalidValue]. It is not a [schemaext.InvalidModelError],
// since an identity belongs to no representation; the object constructors
// type it.
func ValidateSecurityPolicyRef(ref objectidentity.ID) error {
	if ref.Kind != objectidentity.Kind(SecurityPolicyKind) || ref.Name.Normalized == "" || ref.Schema.Normalized == "" ||
		!ref.Parent.Empty() || !ref.Catalog.Empty() || ref.Signature != "" {
		return fmt.Errorf("%w: a security policy requires a schema and a name, and no table parent", schemaext.ErrInvalidValue)
	}
	return validName("policy", ObjectName{Schema: ref.Schema.Source, Name: ref.Name.Source})
}

// DesiredSecurityPolicyObject captures a copy of one authored policy under
// ref. An invalid ref or policy is refused with a desired
// [schemaext.InvalidModelError] and the zero Object.
func DesiredSecurityPolicyObject(ref objectidentity.ID, policy DesiredSecurityPolicy) (schemaext.Object, error) {
	if err := ValidateSecurityPolicyRef(ref); err != nil {
		return schemaext.Object{}, modelError(schemaext.Desired, err)
	}
	if err := ValidateDesiredSecurityPolicy(&policy); err != nil {
		return schemaext.Object{}, err
	}
	return schemaext.Object{Ref: ref, Value: policy.Copy()}, nil
}

// ObservedSecurityPolicyObject captures a copy of one policy a read reported
// under ref. An invalid ref or policy is refused with an observed
// [schemaext.InvalidModelError] and the zero Object.
func ObservedSecurityPolicyObject(ref objectidentity.ID, policy ObservedSecurityPolicy) (schemaext.Object, error) {
	if err := ValidateSecurityPolicyRef(ref); err != nil {
		return schemaext.Object{}, modelError(schemaext.Observed, err)
	}
	if err := ValidateObservedSecurityPolicy(&policy); err != nil {
		return schemaext.Object{}, err
	}
	return schemaext.Object{Ref: ref, Value: policy.Copy()}, nil
}

// ValidateDesiredSecurityPolicy checks representation invariants: each
// predicate well formed, no two predicates for one operation on one table,
// which SQL Server refuses, and a struct name that is valid text. A policy may hold
// no predicate: CREATE SECURITY POLICY without one succeeds, measured on SQL
// Server 2025. A nil v is refused. A refusal is a desired
// [schemaext.InvalidModelError] wrapping [schemaext.ErrInvalidValue].
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
	if v.Normalized != nil {
		if err := validatePredicates(v.Normalized); err != nil {
			return modelError(schemaext.Desired, err)
		}
		if !sameSlots(v.Predicates, v.Normalized) {
			return modelError(schemaext.Desired, fmt.Errorf("%w: a normalized security policy holds one predicate for each declared one, in its slot",
				schemaext.ErrInvalidValue))
		}
	}
	return nil
}

// sameSlots reports whether two predicate lists hold the same slots, a type
// and an operation on a table each, compared by their exact spelling.
func sameSlots(left, right []Predicate) bool {
	if len(left) != len(right) {
		return false
	}
	for _, want := range left {
		if !slices.ContainsFunc(right, func(got Predicate) bool {
			return got.Type == want.Type && got.Operation == want.Operation && got.Table == want.Table
		}) {
			return false
		}
	}
	return true
}

// ValidateObservedSecurityPolicy checks that each predicate of an observation
// is well formed and that no two cover one operation on one table. An
// observation may hold no predicate, as a declaration may. A nil v is
// refused. A refusal is an observed [schemaext.InvalidModelError] wrapping
// [schemaext.ErrInvalidValue].
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
//
// Tables are told apart by [ObjectName.ConflictKey], since the collation that
// decides which names are one table is not known offline. On a
// case-insensitive database `app.orders`, `APP.Orders` and `app.orders  ` are
// one table, and SQL Server refuses a second filter on any of them with the
// same Msg 33262.
func validatePredicates(predicates []Predicate) error {
	type slot struct {
		table     ObjectName
		kind      PredicateType
		operation BlockOperation
	}
	type blocks struct {
		count, everyOperation int
	}
	seen := make(map[slot]bool, len(predicates))
	perTable := make(map[ObjectName]*blocks)
	for _, predicate := range predicates {
		if err := validatePredicate(predicate); err != nil {
			return err
		}
		table := predicate.Table.ConflictKey()
		key := slot{table: table, kind: predicate.Type, operation: predicate.Operation}
		if seen[key] {
			return fmt.Errorf("%w: table %s has two %s predicates", schemaext.ErrInvalidValue, predicate.Table, describe(predicate))
		}
		seen[key] = true
		if predicate.Type != Block {
			continue
		}
		counted := perTable[table]
		if counted == nil {
			counted = &blocks{}
			perTable[table] = counted
		}
		counted.count++
		if predicate.Operation == "" {
			counted.everyOperation++
		}
		if counted.everyOperation > 0 && counted.count > 1 {
			return fmt.Errorf("%w: table %s has a BLOCK predicate for every operation beside another BLOCK predicate", schemaext.ErrInvalidValue, predicate.Table)
		}
	}
	return nil
}

// ConflictKey returns the key under which two spellings of a name may be one
// object on some SQL Server database: each part with its trailing spaces
// removed, since SQL Server compares names without them, and its letter case
// folded, as every case-insensitive collation does. It is deliberately broad.
// A case-sensitive database holds `Orders` and `orders` as two tables, and a
// rule keyed by this treats them as one, although that server would not; the
// narrower key let a case-insensitive server refuse at apply instead.
func (n ObjectName) ConflictKey() ObjectName {
	return ObjectName{Schema: conflictName(n.Schema), Name: conflictName(n.Name)}
}

func conflictName(name string) string {
	return strings.ToLower(strings.TrimRight(name, " "))
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
	if err := validName("predicate function", predicate.Function); err != nil {
		return err
	}
	if err := validName("predicate table", predicate.Table); err != nil {
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
// schema, each holding more than white space.
func validName(field string, name ObjectName) error {
	if strings.TrimSpace(name.Schema) == "" || strings.TrimSpace(name.Name) == "" {
		return fmt.Errorf("%w: a %s needs its schema and its name", schemaext.ErrInvalidValue, field)
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

// String renders the name as T-SQL writes it: each part bracketed, with a
// closing bracket inside a part doubled, as QUOTENAME does.
func (n ObjectName) String() string {
	return bracket(n.Schema) + "." + bracket(n.Name)
}

func bracket(part string) string {
	return "[" + strings.ReplaceAll(part, "]", "]]") + "]"
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

// sortPredicates puts predicates in their canonical order in place.
func sortPredicates(predicates []Predicate) {
	slices.SortFunc(predicates, comparePredicates)
}

// samePredicates compares two predicate lists as sets. Sorting moves whole
// predicates, so shallow copies leave the arguments shared and unchanged.
func samePredicates(left, right []Predicate) bool {
	if len(left) != len(right) {
		return false
	}
	left, right = slices.Clone(left), slices.Clone(right)
	sortPredicates(left)
	sortPredicates(right)
	return slices.EqualFunc(left, right, func(a, b Predicate) bool {
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
