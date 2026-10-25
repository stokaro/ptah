package pgdefaultprivilege

import (
	"cmp"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/schemaext"
)

// Grant is one granted privilege, with its grant option. Repeated declarations
// of a privilege combine their grant options: a grantable declaration wins.
type Grant struct {
	Privilege  string `json:"privilege"`
	WithOption bool   `json:"with_option,omitempty"`
}

// Desired declares privileges for an identity, including explicit revokes.
// Comment documents the source declaration; PostgreSQL stores no comment on a
// default ACL. Empty lists still declare the identity and authorize trimming its
// additional grants. Unknown source knowledge belongs to feature coverage.
type Desired struct {
	Privileges []Grant  `json:"privileges,omitempty"`
	Revoked    []string `json:"revoked,omitempty"`
	Comment    string   `json:"comment,omitempty"`
}

// Observed holds the privileges a catalog describes. Global entries describe
// differences from the built-in default: Privileges adds rights and Revoked
// removes built-in rights. Scoped entries describe additional grants. Missing
// catalog knowledge belongs to feature coverage and is never an empty value.
type Observed struct {
	Privileges []Grant  `json:"privileges,omitempty"`
	Revoked    []string `json:"revoked,omitempty"`
}

// Kind returns the model's owner-qualified kind.
func (*Desired) Kind() schemaext.Kind { return DefaultPrivilegeKind }

// Kind returns the model's owner-qualified kind.
func (*Observed) Kind() schemaext.Kind { return DefaultPrivilegeKind }

// ReadsRelation reports false: a default privilege applies to objects created
// later and depends on no table or view, so a plan that drops one before
// revoking it relies on nothing for it. It implements
// [schemaext.RelationReader].
func (*Observed) ReadsRelation(string) bool { return false }

// Clone returns an independent declaration; a nil receiver remains nil.
func (v *Desired) Clone() schemaext.Value { return v.Copy() }

// Clone returns an independent observation; a nil receiver remains nil.
func (v *Observed) Clone() schemaext.Value { return v.Copy() }

// Copy returns an independent declaration, or nil for a nil receiver.
func (v *Desired) Copy() *Desired {
	if v == nil {
		return nil
	}
	copied := *v
	copied.Privileges = slices.Clone(v.Privileges)
	copied.Revoked = slices.Clone(v.Revoked)
	return &copied
}

// Copy returns an independent observation, or nil for a nil receiver.
func (v *Observed) Copy() *Observed {
	if v == nil {
		return nil
	}
	copied := *v
	copied.Privileges = slices.Clone(v.Privileges)
	copied.Revoked = slices.Clone(v.Revoked)
	return &copied
}

// Equal compares declarations after folding privilege keywords and ordering
// their sets. The source comment participates. Typed nil declarations are equal.
func (v *Desired) Equal(other schemaext.Value) bool {
	w, ok := other.(*Desired)
	if !ok || v == nil || w == nil {
		return ok && v == nil && w == nil
	}
	return v.Comment == w.Comment && equalPrivileges(v.Privileges, w.Privileges, v.Revoked, w.Revoked)
}

// Equal compares observations after folding privilege keywords and ordering
// their sets. Typed nil observations are equal; declarations are never equal.
func (v *Observed) Equal(other schemaext.Value) bool {
	w, ok := other.(*Observed)
	if !ok || v == nil || w == nil {
		return ok && v == nil && w == nil
	}
	return equalPrivileges(v.Privileges, w.Privileges, v.Revoked, w.Revoked)
}

func equalPrivileges(left, right []Grant, leftRevoked, rightRevoked []string) bool {
	return slices.Equal(canonicalGrants(left), canonicalGrants(right)) && slices.Equal(canonicalNames(leftRevoked), canonicalNames(rightRevoked))
}

func canonicalGrants(grants []Grant) []Grant {
	byName := make(map[string]bool, len(grants))
	for _, grant := range grants {
		name := strings.ToUpper(strings.TrimSpace(grant.Privilege))
		byName[name] = byName[name] || grant.WithOption
	}
	var result []Grant
	for name, withOption := range byName {
		result = append(result, Grant{Privilege: name, WithOption: withOption})
	}
	slices.SortFunc(result, func(a, b Grant) int { return cmp.Compare(a.Privilege, b.Privilege) })
	return result
}

func canonicalNames(names []string) []string {
	result := slices.Clone(names)
	for i := range result {
		result[i] = strings.ToUpper(strings.TrimSpace(result[i]))
	}
	slices.Sort(result)
	return slices.Compact(result)
}

// Observed predicts the catalog delta described by v. It makes independent
// copies of both lists and leaves source-only documentation out. Nil stays nil.
func (v *Desired) Observed() *Observed {
	if v == nil {
		return nil
	}
	return &Observed{Privileges: slices.Clone(v.Privileges), Revoked: slices.Clone(v.Revoked)}
}

// Desired declares the observed delta without inventing a source comment. Both
// lists are independently copied. A nil receiver returns nil.
func (v *Observed) Desired() *Desired {
	if v == nil {
		return nil
	}
	return &Desired{Privileges: slices.Clone(v.Privileges), Revoked: slices.Clone(v.Revoked)}
}

// ValidateDesired refuses nil, invalid text, empty privilege names, and a
// privilege both granted and revoked. It returns an InvalidModelError.
func ValidateDesired(v *Desired) error {
	if v == nil {
		return invalid(schemaext.Desired, fmt.Errorf("nil default privilege declaration"))
	}
	if err := validText("comment", v.Comment); err != nil {
		return invalid(schemaext.Desired, err)
	}
	if err := validatePrivileges(v.Privileges, v.Revoked); err != nil {
		return invalid(schemaext.Desired, err)
	}
	return nil
}

// ValidateObserved refuses nil, invalid text, empty privilege names, and a
// privilege both granted and revoked. It returns an InvalidModelError.
func ValidateObserved(v *Observed) error {
	if v == nil {
		return invalid(schemaext.Observed, fmt.Errorf("nil default privilege observation"))
	}
	if err := validatePrivileges(v.Privileges, v.Revoked); err != nil {
		return invalid(schemaext.Observed, err)
	}
	return nil
}

func validatePrivileges(grants []Grant, revoked []string) error {
	for _, grant := range grants {
		if err := requiredText("privilege", strings.TrimSpace(grant.Privilege)); err != nil {
			return err
		}
	}
	for _, name := range revoked {
		if err := requiredText("revoked privilege", strings.TrimSpace(name)); err != nil {
			return err
		}
	}
	revokes := canonicalNames(revoked)
	for _, grant := range canonicalGrants(grants) {
		if slices.Contains(revokes, grant.Privilege) || slices.Contains(revokes, "ALL") || grant.Privilege == "ALL" && len(revokes) != 0 {
			return fmt.Errorf("%w: default privilege %s is both granted and revoked", schemaext.ErrInvalidValue, grant.Privilege)
		}
	}
	return nil
}

// Canonical returns an independent declaration with ordered privilege sets,
// folded keywords, and duplicate grant options combined. A nil receiver returns
// nil. It does not validate or resolve any built-in default.
func (v *Desired) Canonical() *Desired {
	if v == nil {
		return nil
	}
	return canonicalDesired(v)
}

// Canonical returns an independent observation with ordered privilege sets,
// folded keywords, and duplicate grant options combined. A nil receiver returns
// nil. It does not validate or resolve any built-in default.
func (v *Observed) Canonical() *Observed {
	if v == nil {
		return nil
	}
	return canonicalObserved(v)
}
