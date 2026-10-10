package chschema

import (
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
)

// RowPolicyKind identifies one ClickHouse row policy on one table, a feature
// object of its own rather than a setting of the table.
const RowPolicyKind schemaext.Kind = "ptah.run/clickhouse/row-policy"

// Composition decides how a row policy combines with the other policies that
// apply to a user on the same table. Measured on 24.10 and 26.9, the rows a
// user sees are those that pass at least one permissive policy and every
// restrictive one; with no permissive policy for the user, the restrictive
// ones alone decide. A restrictive policy recorded as permissive therefore
// grants rows it was written to withhold.
type Composition string

const (
	// Permissive policies are combined with OR. It is ClickHouse's default.
	Permissive Composition = "permissive"
	// Restrictive policies are combined with AND over the permissive result.
	Restrictive Composition = "restrictive"
)

// RoleSelection is a row policy's TO clause: the users and roles it applies
// to. ClickHouse resolves a name to a user or a role, so one list holds both.
//
// The zero value names nobody, which is what a policy without a TO clause and
// one written TO NONE both apply to: such a policy exists and filters no one.
// Names and Except are sets, compared and encoded without regard to order, and
// a name keeps its exact bytes, so a role named "ALL" is a name and never the
// keyword. CURRENT_USER is not a selector: ClickHouse records the user it
// resolves to, so a declaration holding it would never match what is read back.
type RoleSelection struct {
	// All applies the policy to every user, the one that created it included,
	// except those Except names.
	All bool `json:"all,omitempty"`
	// Names lists the users and roles the policy applies to. It is empty when
	// All is set.
	Names []string `json:"names,omitempty"`
	// Except lists the users and roles TO ALL EXCEPT leaves out. It is empty
	// unless All is set.
	Except []string `json:"except,omitempty"`
}

// IsZero reports a selection that names nobody.
func (s RoleSelection) IsZero() bool { return !s.All && len(s.Names) == 0 && len(s.Except) == 0 }

// Equal compares two selections, each list as a set. A nil list and an empty
// one name the same nobody.
func (s RoleSelection) Equal(other RoleSelection) bool {
	return s.All == other.All && sameNames(s.Names, other.Names) && sameNames(s.Except, other.Except)
}

// Clone returns a selection that shares no list with s.
func (s RoleSelection) Clone() RoleSelection {
	return RoleSelection{All: s.All, Names: slices.Clone(s.Names), Except: slices.Clone(s.Except)}
}

// canonical orders both lists and drops empty ones, so equal selections encode
// to the same bytes.
func (s RoleSelection) canonical() RoleSelection {
	return RoleSelection{All: s.All, Names: sortedNames(s.Names), Except: sortedNames(s.Except)}
}

// DesiredRowPolicy declares one ClickHouse row policy. Its database, table
// and name are its object identity (see [RowPolicyRef]), never fields here.
//
// An omitted value requests ClickHouse's documented default: a nil Filter is a
// policy without USING, an empty Composition is permissive, and a zero Roles
// is a policy without TO, which applies to nobody. A policy filters SELECT and
// nothing else, so the model has no command: FOR SELECT and FOR ALL create the
// same policy, and a write check is a clause ClickHouse accepts and discards.
type DesiredRowPolicy struct {
	// Filter is the USING condition. A policy without one admits every row to
	// the users it applies to, which matters for a permissive policy beside
	// others: measured, it lets those users see the whole table.
	Filter      *string       `json:"filter,omitempty"`
	Composition Composition   `json:"composition,omitempty"`
	Roles       RoleSelection `json:"roles,omitzero"`
	// StructName preserves the Go struct a declaration was read from. It has no
	// server counterpart and does not change the policy.
	StructName string `json:"struct_name,omitempty"`
	// NormalizedFilter is the connected server's own spelling of Filter,
	// attached by a live comparison before the comparison runs: the server
	// stores a filter reformatted, and differently on different release lines,
	// so a declaration and the catalog differ for a policy that has not
	// changed. Nil means no server rewrote this declaration. A source never
	// writes it, and it is set only beside a Filter.
	NormalizedFilter *string `json:"normalized_filter,omitempty"`
}

// ObservedRowPolicy is a row policy as system.row_policies reports it. Every
// value is definite: a reader that cannot establish one records a coverage
// limit for the policy instead of filling in a default.
type ObservedRowPolicy struct {
	// Filter is select_filter: nil for a policy without USING, and otherwise
	// the condition in the spelling the server stores, which differs between
	// release lines.
	Filter      *string     `json:"filter,omitempty"`
	Composition Composition `json:"composition"`
	// Roles is the selection apply_to_all, apply_to_list and apply_to_except
	// report. The zero value is a policy that applies to nobody.
	Roles RoleSelection `json:"roles"`
}

// Kind returns the row policy model identity.
func (*DesiredRowPolicy) Kind() schemaext.Kind { return RowPolicyKind }

// Kind returns the row policy model identity.
func (*ObservedRowPolicy) Kind() schemaext.Kind { return RowPolicyKind }

// Clone returns an independent declaration. A nil receiver remains typed nil.
func (v *DesiredRowPolicy) Clone() schemaext.Value { return v.Copy() }

// Copy is [DesiredRowPolicy.Clone] without the interface: it shares no list or
// pointer with v, and a nil receiver returns nil.
func (v *DesiredRowPolicy) Copy() *DesiredRowPolicy {
	if v == nil {
		return nil
	}
	cloned := *v
	cloned.Filter, cloned.NormalizedFilter = cloneText(v.Filter), cloneText(v.NormalizedFilter)
	cloned.Roles = v.Roles.Clone()
	return &cloned
}

// Clone returns an independent observation. A nil receiver remains typed nil.
func (v *ObservedRowPolicy) Clone() schemaext.Value { return v.Copy() }

// Copy is [ObservedRowPolicy.Clone] without the interface: it shares no list
// or pointer with v, and a nil receiver returns nil.
func (v *ObservedRowPolicy) Copy() *ObservedRowPolicy {
	if v == nil {
		return nil
	}
	cloned := *v
	cloned.Filter = cloneText(v.Filter)
	cloned.Roles = v.Roles.Clone()
	return &cloned
}

// Equal compares declarations field by field, the role lists as sets, without
// resolving defaults: an omitted composition is not equal to permissive here.
// Filters compare as written.
func (v *DesiredRowPolicy) Equal(other schemaext.Value) bool {
	right, ok := other.(*DesiredRowPolicy)
	if !ok || v == nil || right == nil {
		return ok && v == nil && right == nil
	}
	return equalText(v.Filter, right.Filter) && v.Composition == right.Composition &&
		v.Roles.Equal(right.Roles) && v.StructName == right.StructName && equalText(v.NormalizedFilter, right.NormalizedFilter)
}

// Equal compares observations field by field, the role lists as sets.
func (v *ObservedRowPolicy) Equal(other schemaext.Value) bool {
	right, ok := other.(*ObservedRowPolicy)
	if !ok || v == nil || right == nil {
		return ok && v == nil && right == nil
	}
	return equalText(v.Filter, right.Filter) && v.Composition == right.Composition && v.Roles.Equal(right.Roles)
}

// Desired captures an observed policy as the declaration that would create
// it, every default stated. A nil receiver remains nil.
func (v *ObservedRowPolicy) Desired() *DesiredRowPolicy {
	if v == nil {
		return nil
	}
	return &DesiredRowPolicy{Filter: cloneText(v.Filter), Composition: v.Composition, Roles: v.Roles.Clone()}
}

// Observed projects a declaration as the policy a server holds once it is
// created: an omitted composition is permissive, and the filter is the
// server's spelling where one was attached and the declaration's otherwise. A
// nil or invalid declaration is refused.
func (v *DesiredRowPolicy) Observed() (*ObservedRowPolicy, error) {
	if err := ValidateDesiredRowPolicy(v); err != nil {
		return nil, err
	}
	composition := v.Composition
	if composition == "" {
		composition = Permissive
	}
	filter := v.Filter
	if v.NormalizedFilter != nil {
		filter = v.NormalizedFilter
	}
	return &ObservedRowPolicy{Filter: cloneText(filter), Composition: composition, Roles: v.Roles.Clone()}, nil
}

// RowPolicyRef builds the identity of the row policy name on table in
// database under ClickHouse's identifier rules. The table is part of the
// identity, as it is of the server's own name for the policy (`name ON
// db.table`): two tables may each hold a policy of one name. An empty database
// is the connection's, as an unqualified ON clause is.
//
// The parts are used as they are, without trimming: ClickHouse keeps a policy
// named " p" beside one named "p".
func RowPolicyRef(database, table, name string) objectidentity.ID {
	return RowPolicyRefWith(identifier.ForDialect(platform.ClickHouse), database, table, name)
}

// RowPolicyRefWith builds the identity under explicit identifier rules, which
// is how a comparison resolves an empty database to the connection's. Like
// [RowPolicyRef], it does not trim the parts.
func RowPolicyRefWith(semantics identifier.Semantics, database, table, name string) objectidentity.ID {
	owner := objectidentity.NewBuilder(semantics).TablePartsVerbatim(database, table)
	return objectidentity.ID{
		Kind: objectidentity.Kind(RowPolicyKind), Schema: owner.Schema, Parent: owner.Name,
		Name: objectidentity.Part{Source: name, Normalized: semantics.TableIdentityKey(name)},
	}
}

// RowPolicyDatabase returns the database a reference names as the source
// wrote it: empty when the source left it to the connection.
func RowPolicyDatabase(ref objectidentity.ID) string {
	if ref.Schema.Defaulted {
		return ""
	}
	return ref.Schema.Source
}

// ResolveRowPolicyRef rebuilds a captured reference under the identifier
// rules of a comparison, so a database the source left out takes the target's
// default rather than the one the source assumed. That is what joins an
// unqualified declaration to the policy a read reported in the connection's
// database.
func ResolveRowPolicyRef(semantics identifier.Semantics, ref objectidentity.ID) objectidentity.ID {
	return RowPolicyRefWith(semantics, RowPolicyDatabase(ref), ref.Parent.Source, ref.Name.Source)
}

// RowPolicyTable returns the identity of the table a row policy reference is
// on.
func RowPolicyTable(ref objectidentity.ID) objectidentity.ID {
	return objectidentity.ID{Kind: objectidentity.KindTable, Catalog: ref.Catalog, Schema: ref.Schema, Name: ref.Parent}
}

// ValidateRowPolicyRef requires an identity of this kind with a table and a
// name, and no catalog or signature.
//
// A policy written ON db.* applies to every table of the database, and the
// server reports it with an empty table. It is a different variant from a
// table policy, and this model does not hold it: such a reference is refused
// by name rather than taken for a policy on some table, and a reader records
// the database's policies as not described instead of listing the tables it
// happened to see.
func ValidateRowPolicyRef(ref objectidentity.ID) error {
	if ref.Kind != objectidentity.Kind(RowPolicyKind) || !ref.Catalog.Empty() || ref.Signature != "" {
		return fmt.Errorf("%w: a ClickHouse row policy reference has kind %q and no catalog or signature", schemaext.ErrInvalidValue, RowPolicyKind)
	}
	if ref.Name.Source == "" || ref.Name.Normalized == "" {
		return fmt.Errorf("%w: a ClickHouse row policy requires a name", schemaext.ErrInvalidValue)
	}
	if ref.Parent.Source == "" || ref.Parent.Normalized == "" {
		return fmt.Errorf("%w: ClickHouse row policy %q has no table; a database-wide policy (ON db.*) is not supported, only a policy on one table",
			schemaext.ErrInvalidValue, ref.Name.Source)
	}
	for _, part := range []struct{ field, value string }{
		{"database", ref.Schema.Source}, {"table", ref.Parent.Source}, {"name", ref.Name.Source},
	} {
		if err := validText("row policy "+part.field, part.value); err != nil {
			return err
		}
	}
	return nil
}

// DesiredRowPolicyObject captures one authored policy under ref.
func DesiredRowPolicyObject(ref objectidentity.ID, policy DesiredRowPolicy) (schemaext.Object, error) {
	if err := ValidateRowPolicyRef(ref); err != nil {
		return schemaext.Object{}, err
	}
	if err := ValidateDesiredRowPolicy(&policy); err != nil {
		return schemaext.Object{}, err
	}
	return schemaext.Object{Ref: ref, Value: policy.Copy()}, nil
}

// ObservedRowPolicyObject captures one policy a read reported under ref.
func ObservedRowPolicyObject(ref objectidentity.ID, policy ObservedRowPolicy) (schemaext.Object, error) {
	if err := ValidateRowPolicyRef(ref); err != nil {
		return schemaext.Object{}, err
	}
	if err := ValidateObservedRowPolicy(&policy); err != nil {
		return schemaext.Object{}, err
	}
	return schemaext.Object{Ref: ref, Value: policy.Copy()}, nil
}

// ValidateDesiredRowPolicy checks representation invariants: a known
// composition, a filter that is not empty, and a well-formed role selection.
func ValidateDesiredRowPolicy(v *DesiredRowPolicy) error {
	if v == nil {
		return rowPolicyError(schemaext.Desired, fmt.Errorf("%w: nil row policy declaration", schemaext.ErrInvalidValue))
	}
	if v.Composition != "" && v.Composition != Permissive && v.Composition != Restrictive {
		return rowPolicyError(schemaext.Desired, fmt.Errorf("%w: unknown row policy composition %q", schemaext.ErrInvalidValue, v.Composition))
	}
	if err := validateRowPolicyState(v.Filter, v.Roles); err != nil {
		return rowPolicyError(schemaext.Desired, err)
	}
	if v.NormalizedFilter != nil {
		if v.Filter == nil {
			return rowPolicyError(schemaext.Desired, fmt.Errorf("%w: a normalized row policy filter needs the declared filter it normalizes", schemaext.ErrInvalidValue))
		}
		if err := validateRowPolicyState(v.NormalizedFilter, RoleSelection{}); err != nil {
			return rowPolicyError(schemaext.Desired, err)
		}
	}
	if err := validText("row policy struct name", v.StructName); err != nil {
		return rowPolicyError(schemaext.Desired, err)
	}
	return nil
}

// ValidateObservedRowPolicy checks that an observation is definite: a
// composition, a filter that is not empty, and a well-formed role selection.
func ValidateObservedRowPolicy(v *ObservedRowPolicy) error {
	if v == nil {
		return rowPolicyError(schemaext.Observed, fmt.Errorf("%w: nil row policy observation", schemaext.ErrInvalidValue))
	}
	if v.Composition != Permissive && v.Composition != Restrictive {
		return rowPolicyError(schemaext.Observed, fmt.Errorf("%w: unknown row policy composition %q", schemaext.ErrInvalidValue, v.Composition))
	}
	if err := validateRowPolicyState(v.Filter, v.Roles); err != nil {
		return rowPolicyError(schemaext.Observed, err)
	}
	return nil
}

func validateRowPolicyState(filter *string, roles RoleSelection) error {
	if filter != nil {
		if strings.TrimSpace(*filter) == "" {
			return fmt.Errorf("%w: a row policy filter cannot be empty; omit it for a policy without USING", schemaext.ErrInvalidValue)
		}
		if err := validText("row policy filter", *filter); err != nil {
			return err
		}
	}
	return validateRoleSelection(roles)
}

// validateRoleSelection requires the lists TO ALL, TO ALL EXCEPT and a named
// list allow, each name to be non-empty text, and no name twice in a list.
func validateRoleSelection(s RoleSelection) error {
	if s.All && len(s.Names) > 0 {
		return fmt.Errorf("%w: a row policy applies TO ALL or to named users and roles, not both", schemaext.ErrInvalidValue)
	}
	if !s.All && len(s.Except) > 0 {
		return fmt.Errorf("%w: a row policy names exceptions only beside TO ALL", schemaext.ErrInvalidValue)
	}
	for _, list := range []struct {
		clause string
		names  []string
	}{{"TO", s.Names}, {"ALL EXCEPT", s.Except}} {
		seen := make(map[string]bool, len(list.names))
		for _, name := range list.names {
			if name == "" {
				return fmt.Errorf("%w: a row policy %s list holds an empty name", schemaext.ErrInvalidValue, list.clause)
			}
			if err := validText("role name", name); err != nil {
				return err
			}
			if seen[name] {
				return fmt.Errorf("%w: a row policy %s list names %q twice", schemaext.ErrInvalidValue, list.clause, name)
			}
			seen[name] = true
		}
	}
	return nil
}

// rowPolicyError names the model and representation a refusal is about, as
// the typed error the codec boundary recognizes.
func rowPolicyError(representation schemaext.Representation, err error) error {
	return &schemaext.InvalidModelError{Kind: RowPolicyKind, Representation: representation, Message: err.Error()}
}

// validText refuses text a statement or a catalog cannot carry faithfully.
func validText(field, value string) error {
	if !utf8.ValidString(value) {
		return fmt.Errorf("%w: ClickHouse %s is not valid UTF-8", schemaext.ErrInvalidValue, field)
	}
	if strings.ContainsRune(value, '\x00') {
		return fmt.Errorf("%w: ClickHouse %s contains a NUL byte", schemaext.ErrInvalidValue, field)
	}
	return nil
}

// sameNames compares two name lists as sets, so order carries no meaning and
// nil equals empty. Validation refuses a repeat, and counting keeps an invalid
// list with one comparing correctly too.
func sameNames(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for _, name := range left {
		if occurrences(left, name) != occurrences(right, name) {
			return false
		}
	}
	return true
}

func occurrences(names []string, name string) int {
	n := 0
	for _, candidate := range names {
		if candidate == name {
			n++
		}
	}
	return n
}

// sortedNames returns names in byte order, and nil for an empty list.
func sortedNames(names []string) []string {
	if len(names) == 0 {
		return nil
	}
	sorted := slices.Clone(names)
	slices.Sort(sorted)
	return sorted
}

func cloneText(value *string) *string {
	if value == nil {
		return nil
	}
	return new(*value)
}

func equalText(left, right *string) bool {
	if left == nil || right == nil {
		return left == right
	}
	return *left == *right
}
