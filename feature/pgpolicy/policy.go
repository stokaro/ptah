package pgpolicy

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

// Command is the class of statement a policy applies to.
type Command string

const (
	// CommandAll applies a policy to every command.
	CommandAll Command = "ALL"
	// CommandSelect applies a policy to SELECT.
	CommandSelect Command = "SELECT"
	// CommandInsert applies a policy to INSERT.
	CommandInsert Command = "INSERT"
	// CommandUpdate applies a policy to UPDATE.
	CommandUpdate Command = "UPDATE"
	// CommandDelete applies a policy to DELETE.
	CommandDelete Command = "DELETE"
)

// Composition decides how a policy combines with the other policies of its
// table. A row passes when at least one permissive policy admits it and every
// restrictive policy does, so a restrictive policy recorded as permissive grants
// what it was written to withhold.
type Composition string

const (
	// Permissive policies are combined with OR. It is PostgreSQL's default.
	Permissive Composition = "permissive"
	// Restrictive policies are combined with AND over the permissive result.
	Restrictive Composition = "restrictive"
)

// RoleKeyword is a role specification PostgreSQL resolves itself rather than
// the name of a role.
type RoleKeyword string

const (
	// Public is every role.
	Public RoleKeyword = "PUBLIC"
	// CurrentRole is the role running the statement that creates the policy.
	CurrentRole RoleKeyword = "CURRENT_ROLE"
	// CurrentUser is CURRENT_ROLE's other spelling.
	CurrentUser RoleKeyword = "CURRENT_USER"
	// SessionUser is the role that opened the session creating the policy.
	SessionUser RoleKeyword = "SESSION_USER"
)

// RoleSelector is one entry of a policy's TO list: exactly one of a keyword or
// a role name. A name keeps its exact bytes, spaces and commas included, and is
// never a comma-separated list of names.
//
// PostgreSQL reserves the role names public and none, and stores TO "public" as
// PUBLIC, so neither is accepted as a name: PUBLIC is the keyword. The catalog
// records the role a CURRENT_ROLE, CURRENT_USER or SESSION_USER keyword
// resolved to when the policy was created, so an observation carries only the
// PUBLIC keyword and role names.
type RoleSelector struct {
	Keyword RoleKeyword `json:"keyword,omitempty"`
	Name    string      `json:"name,omitempty"`
}

// DesiredPolicy declares one PostgreSQL row-security policy. Its table, schema
// and name are its object identity (see [PolicyRef]), never fields here.
//
// An omitted value requests PostgreSQL's documented default: an empty Command
// is ALL, nil Roles is PUBLIC, and an empty Composition is permissive. A nil
// Using or WithCheck declares no such clause, which is not the same as an
// expression.
type DesiredPolicy struct {
	Command Command `json:"command,omitempty"`
	// Roles is the TO list. Order carries no meaning, so equality and the
	// canonical encoding treat it as a set. PUBLIC stands alone: PostgreSQL
	// keeps only PUBLIC from a list that holds it, so a list naming PUBLIC
	// beside other roles is refused rather than reduced.
	Roles []RoleSelector `json:"roles,omitempty"`
	// Using is the USING expression, which filters the rows a command sees.
	Using *string `json:"using,omitempty"`
	// WithCheck is the WITH CHECK expression, which a new or changed row must
	// satisfy.
	WithCheck   *string     `json:"with_check,omitempty"`
	Composition Composition `json:"composition,omitempty"`
	// Comment is the policy's COMMENT ON POLICY text.
	Comment string `json:"comment,omitempty"`
	// StructName preserves the Go struct a declaration was read from. It has no
	// server counterpart and does not change the policy.
	StructName string `json:"struct_name,omitempty"`
	// Normalized is the connected server's spelling of this declaration, which
	// a normalization probe attaches before a comparison. Nil means no server
	// answered, and a comparison then compares the declared text.
	Normalized *NormalizedPolicy `json:"normalized,omitempty"`
}

// ObservedPolicy is a policy as pg_policy reports it. Every value is definite:
// a reader that cannot establish one records a coverage limit for the policy
// instead of filling in a default.
type ObservedPolicy struct {
	Command Command `json:"command"`
	// Roles is never empty: a policy for every role reports the PUBLIC keyword
	// alone. It is a set. pg_policy.polroles can list one role twice, for a
	// policy created TO CURRENT_USER, SESSION_USER by that user, and a reader
	// records such a role once.
	Roles []RoleSelector `json:"roles"`
	// Using and WithCheck are nil for a policy without the clause.
	Using       *string     `json:"using,omitempty"`
	WithCheck   *string     `json:"with_check,omitempty"`
	Composition Composition `json:"composition"`
	Comment     string      `json:"comment,omitempty"`
}

// Kind returns the policy model identity.
func (*DesiredPolicy) Kind() schemaext.Kind { return PolicyKind }

// Kind returns the policy model identity.
func (*ObservedPolicy) Kind() schemaext.Kind { return PolicyKind }

// Clone returns an independent declaration. A nil receiver remains typed nil.
func (v *DesiredPolicy) Clone() schemaext.Value { return v.Copy() }

// Copy is [DesiredPolicy.Clone] without the interface: it shares no slice or
// pointer with v, and a nil receiver returns nil.
func (v *DesiredPolicy) Copy() *DesiredPolicy {
	if v == nil {
		return nil
	}
	cloned := *v
	cloned.Roles = slices.Clone(v.Roles)
	cloned.Using, cloned.WithCheck = cloneText(v.Using), cloneText(v.WithCheck)
	cloned.Normalized = v.Normalized.Copy()
	return &cloned
}

// Clone returns an independent observation. A nil receiver remains typed nil.
func (v *ObservedPolicy) Clone() schemaext.Value { return v.Copy() }

// Copy is [ObservedPolicy.Clone] without the interface: it shares no slice or
// pointer with v, and a nil receiver returns nil.
func (v *ObservedPolicy) Copy() *ObservedPolicy {
	if v == nil {
		return nil
	}
	cloned := *v
	cloned.Roles = slices.Clone(v.Roles)
	cloned.Using, cloned.WithCheck = cloneText(v.Using), cloneText(v.WithCheck)
	return &cloned
}

// Equal compares declarations field by field, the role list as a set, without
// resolving defaults: an omitted command is not equal to ALL here.
func (v *DesiredPolicy) Equal(other schemaext.Value) bool {
	right, ok := other.(*DesiredPolicy)
	if !ok || v == nil || right == nil {
		return ok && v == nil && right == nil
	}
	return v.Command == right.Command && sameRoles(v.Roles, right.Roles) && equalText(v.Using, right.Using) &&
		equalText(v.WithCheck, right.WithCheck) && v.Composition == right.Composition &&
		v.Comment == right.Comment && v.StructName == right.StructName && v.Normalized.equal(right.Normalized)
}

// Equal compares observations field by field, the role list as a set.
func (v *ObservedPolicy) Equal(other schemaext.Value) bool {
	right, ok := other.(*ObservedPolicy)
	if !ok || v == nil || right == nil {
		return ok && v == nil && right == nil
	}
	return v.Command == right.Command && sameRoles(v.Roles, right.Roles) && equalText(v.Using, right.Using) &&
		equalText(v.WithCheck, right.WithCheck) && v.Composition == right.Composition && v.Comment == right.Comment
}

// PolicyRef builds a policy identity from separate schema, table and policy
// name parts under PostgreSQL's identifier rules. The table is a component of
// the identity: a policy name is scoped to its table, so two tables may each
// hold a policy of the same name. An empty schema takes the default and is
// marked as defaulted.
//
// The parts are the bare names the catalog stores and are used as they are,
// without trimming: PostgreSQL keeps a policy named " p" beside one named "p"
// on the same table.
func PolicyRef(schema, table, name string) objectidentity.ID {
	return PolicyRefWith(identifier.ForDialect(platform.Postgres), schema, table, name)
}

// PolicyRefWith builds the identity under explicit identifier rules, which is
// how a comparison pairs a declaration and an observation that spelled the same
// table differently. Like [PolicyRef], it does not trim the parts.
func PolicyRefWith(semantics identifier.Semantics, schema, table, name string) objectidentity.ID {
	owner := objectidentity.NewBuilder(semantics).TablePartsVerbatim(schema, table)
	return objectidentity.ID{
		Kind: objectidentity.Kind(PolicyKind), Schema: owner.Schema, Parent: owner.Name,
		Name: objectidentity.Part{Source: name, Normalized: semantics.TableIdentityKey(name)},
	}
}

// Table returns the identity of the table a policy reference is on.
func Table(ref objectidentity.ID) objectidentity.ID {
	return objectidentity.ID{Kind: objectidentity.KindTable, Catalog: ref.Catalog, Schema: ref.Schema, Name: ref.Parent}
}

// ValidatePolicyRef requires an identity of this kind with a schema, a table
// and a name, and no catalog or signature. A schema the source left out is the
// resolved default, so [Table] always returns a qualified table identity.
func ValidatePolicyRef(ref objectidentity.ID) error {
	if ref.Kind != objectidentity.Kind(PolicyKind) || ref.Name.Source == "" || ref.Name.Normalized == "" ||
		ref.Parent.Source == "" || ref.Parent.Normalized == "" || ref.Schema.Source == "" || ref.Schema.Normalized == "" ||
		!ref.Catalog.Empty() || ref.Signature != "" {
		return fmt.Errorf("%w: a row-security policy requires a schema, a table and a name", schemaext.ErrInvalidValue)
	}
	return nil
}

// DesiredPolicyObject captures one authored policy under ref.
func DesiredPolicyObject(ref objectidentity.ID, policy DesiredPolicy) (schemaext.Object, error) {
	if err := ValidatePolicyRef(ref); err != nil {
		return schemaext.Object{}, err
	}
	if err := ValidateDesiredPolicy(&policy); err != nil {
		return schemaext.Object{}, err
	}
	return schemaext.Object{Ref: ref, Value: policy.Copy()}, nil
}

// ObservedPolicyObject captures one policy a read reported under ref.
func ObservedPolicyObject(ref objectidentity.ID, policy ObservedPolicy) (schemaext.Object, error) {
	if err := ValidatePolicyRef(ref); err != nil {
		return schemaext.Object{}, err
	}
	if err := ValidateObservedPolicy(&policy); err != nil {
		return schemaext.Object{}, err
	}
	return schemaext.Object{Ref: ref, Value: policy.Copy()}, nil
}

// ValidateDesiredPolicy checks representation invariants: a known command,
// role list and composition, and the clauses the command takes. PostgreSQL
// refuses a USING expression on an INSERT policy and a WITH CHECK expression on
// a SELECT or DELETE one, so such a declaration is refused here rather than
// planned.
func ValidateDesiredPolicy(v *DesiredPolicy) error {
	if v == nil {
		return modelError(PolicyKind, schemaext.Desired, fmt.Errorf("%w: nil policy declaration", schemaext.ErrInvalidValue))
	}
	if v.Command != "" && !validCommand(v.Command) {
		return modelError(PolicyKind, schemaext.Desired, fmt.Errorf("%w: unknown policy command %q", schemaext.ErrInvalidValue, v.Command))
	}
	if v.Composition != "" && v.Composition != Permissive && v.Composition != Restrictive {
		return modelError(PolicyKind, schemaext.Desired, fmt.Errorf("%w: unknown policy composition %q", schemaext.ErrInvalidValue, v.Composition))
	}
	if v.Roles != nil && len(v.Roles) == 0 {
		return modelError(PolicyKind, schemaext.Desired,
			fmt.Errorf("%w: a declared role list names at least one role; omit it for PUBLIC", schemaext.ErrInvalidValue))
	}
	if err := validateRoles(v.Roles, []RoleKeyword{Public, CurrentRole, CurrentUser, SessionUser}); err != nil {
		return modelError(PolicyKind, schemaext.Desired, err)
	}
	if err := validateClauses(v.Command, v.Using, v.WithCheck); err != nil {
		return modelError(PolicyKind, schemaext.Desired, err)
	}
	if err := validTexts("comment", v.Comment, "struct name", v.StructName); err != nil {
		return modelError(PolicyKind, schemaext.Desired, err)
	}
	if err := v.Normalized.validate(v); err != nil {
		return modelError(PolicyKind, schemaext.Desired, err)
	}
	return nil
}

// ValidateObservedPolicy checks that an observation is definite: a command, a
// composition and at least one role selector, the clauses its command takes,
// and no keyword the catalog resolves to a role name.
func ValidateObservedPolicy(v *ObservedPolicy) error {
	if v == nil {
		return modelError(PolicyKind, schemaext.Observed, fmt.Errorf("%w: nil policy observation", schemaext.ErrInvalidValue))
	}
	if !validCommand(v.Command) {
		return modelError(PolicyKind, schemaext.Observed, fmt.Errorf("%w: unknown policy command %q", schemaext.ErrInvalidValue, v.Command))
	}
	if v.Composition != Permissive && v.Composition != Restrictive {
		return modelError(PolicyKind, schemaext.Observed, fmt.Errorf("%w: unknown policy composition %q", schemaext.ErrInvalidValue, v.Composition))
	}
	if len(v.Roles) == 0 {
		return modelError(PolicyKind, schemaext.Observed, fmt.Errorf("%w: a policy observation names at least one role", schemaext.ErrInvalidValue))
	}
	if err := validateRoles(v.Roles, []RoleKeyword{Public}); err != nil {
		return modelError(PolicyKind, schemaext.Observed, err)
	}
	if err := validateClauses(v.Command, v.Using, v.WithCheck); err != nil {
		return modelError(PolicyKind, schemaext.Observed, err)
	}
	if err := validText("comment", v.Comment); err != nil {
		return modelError(PolicyKind, schemaext.Observed, err)
	}
	return nil
}

func validCommand(command Command) bool {
	return slices.Contains([]Command{CommandAll, CommandSelect, CommandInsert, CommandUpdate, CommandDelete}, command)
}

// validateRoles requires each selector to be exactly one of an accepted
// keyword or a name that is not reserved, PUBLIC to stand alone, and no
// selector to appear twice. A name is any non-empty text: PostgreSQL accepts a
// role named " ".
func validateRoles(roles []RoleSelector, keywords []RoleKeyword) error {
	seen := make(map[RoleSelector]bool, len(roles))
	for _, role := range roles {
		switch {
		case role.Keyword != "" && role.Name != "":
			return fmt.Errorf("%w: a role selector is a keyword or a name, not both", schemaext.ErrInvalidValue)
		case role.Keyword != "" && !slices.Contains(keywords, role.Keyword):
			return fmt.Errorf("%w: role keyword %q is not accepted here", schemaext.ErrInvalidValue, role.Keyword)
		case role.Keyword == "" && role.Name == "":
			return fmt.Errorf("%w: a role selector needs a keyword or a name", schemaext.ErrInvalidValue)
		case role.Name == "public" || role.Name == "none":
			return fmt.Errorf("%w: PostgreSQL reserves the role name %q; use the PUBLIC keyword for every role", schemaext.ErrInvalidValue, role.Name)
		}
		if err := validText("role name", role.Name); err != nil {
			return err
		}
		if seen[role] {
			return fmt.Errorf("%w: role selector %s appears twice", schemaext.ErrInvalidValue, role)
		}
		seen[role] = true
	}
	if len(roles) > 1 && seen[RoleSelector{Keyword: Public}] {
		return fmt.Errorf("%w: PUBLIC stands alone: PostgreSQL keeps only PUBLIC from a list that names other roles beside it", schemaext.ErrInvalidValue)
	}
	return nil
}

// validateClauses checks the expressions a command takes. An empty command is
// ALL, which takes both.
func validateClauses(command Command, using, withCheck *string) error {
	for _, clause := range []struct {
		name  string
		value *string
	}{{"USING expression", using}, {"WITH CHECK expression", withCheck}} {
		if clause.value == nil {
			continue
		}
		if strings.TrimSpace(*clause.value) == "" {
			return fmt.Errorf("%w: a policy %s cannot be empty; omit it instead", schemaext.ErrInvalidValue, clause.name)
		}
		if err := validText(clause.name, *clause.value); err != nil {
			return err
		}
	}
	if using != nil && command == CommandInsert {
		return fmt.Errorf("%w: an INSERT policy takes no USING expression", schemaext.ErrInvalidValue)
	}
	if withCheck != nil && (command == CommandSelect || command == CommandDelete) {
		return fmt.Errorf("%w: a %s policy takes no WITH CHECK expression", schemaext.ErrInvalidValue, command)
	}
	return nil
}

// String renders a selector for messages: a keyword bare and a name quoted.
func (s RoleSelector) String() string {
	if s.Keyword != "" {
		return string(s.Keyword)
	}
	return fmt.Sprintf("%q", s.Name)
}

// compareRoles orders keywords before names, each by its bytes.
func compareRoles(a, b RoleSelector) int {
	if (a.Keyword == "") != (b.Keyword == "") {
		if a.Keyword != "" {
			return -1
		}
		return 1
	}
	return cmp.Or(cmp.Compare(a.Keyword, b.Keyword), cmp.Compare(a.Name, b.Name))
}

// CanonicalRoles returns a copy of roles in canonical order: keywords before
// names, each by its bytes. The codecs encode a role list in this order, and a
// statement that writes one in it reads the same before and after a round trip.
// A nil list stays nil.
func CanonicalRoles(roles []RoleSelector) []RoleSelector { return sortedRoles(roles) }

// sortedRoles returns roles in their canonical order without changing roles.
func sortedRoles(roles []RoleSelector) []RoleSelector {
	if roles == nil {
		return nil
	}
	sorted := slices.Clone(roles)
	slices.SortFunc(sorted, compareRoles)
	return sorted
}

// sameRoles compares two role lists as sets, counting each selector on both
// sides so an invalid list with a repeat compares correctly too. The lists are
// short, so counting allocates nothing and costs less than sorting copies.
func sameRoles(left, right []RoleSelector) bool {
	if (left == nil) != (right == nil) || len(left) != len(right) {
		return false
	}
	for _, role := range left {
		if count(left, role) != count(right, role) {
			return false
		}
	}
	return true
}

func count(roles []RoleSelector, role RoleSelector) int {
	n := 0
	for _, candidate := range roles {
		if candidate == role {
			n++
		}
	}
	return n
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
