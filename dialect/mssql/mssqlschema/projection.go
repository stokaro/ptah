package mssqlschema

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/platform/identifier"
)

// Resolved returns the state, schema binding and replication behavior a
// declaration asks for, with an omitted state or schema binding read as SQL
// Server's default, ON.
func (v *DesiredSecurityPolicy) Resolved() (enabled, schemaBinding, notForReplication bool) {
	enabled, schemaBinding = true, true
	if v.Enabled != nil {
		enabled = *v.Enabled
	}
	if v.SchemaBinding != nil {
		schemaBinding = *v.SchemaBinding
	}
	return enabled, schemaBinding, v.NotForReplication
}

// Observed predicts what the catalog reports for a server built from the
// declaration: every default resolved, the predicates as declared. It is a
// prediction, never a claim that a server was inspected. An invalid
// declaration is refused.
func (v *DesiredSecurityPolicy) Observed() (*ObservedSecurityPolicy, error) {
	if err := ValidateDesiredSecurityPolicy(v); err != nil {
		return nil, err
	}
	enabled, schemaBinding, notForReplication := v.Resolved()
	return &ObservedSecurityPolicy{
		Predicates: clonePredicates(v.Predicates), Enabled: enabled, SchemaBinding: schemaBinding, NotForReplication: notForReplication,
	}, nil
}

// Desired declares exactly what an observation holds, every value named, so a
// declaration built from it rebuilds the same policy whatever the defaults.
// An invalid observation is refused.
func (v *ObservedSecurityPolicy) Desired() (*DesiredSecurityPolicy, error) {
	if err := ValidateObservedSecurityPolicy(v); err != nil {
		return nil, err
	}
	return &DesiredSecurityPolicy{
		Predicates: clonePredicates(v.Predicates), Enabled: new(v.Enabled), SchemaBinding: new(v.SchemaBinding),
		NotForReplication: v.NotForReplication,
	}, nil
}

// SameName reports whether two names denote one object under semantics, part
// by part, so a comparison pairs spellings a collation folds together.
func SameName(semantics identifier.Semantics, a, b ObjectName) bool {
	return semantics.TableIdentityKey(a.Schema) == semantics.TableIdentityKey(b.Schema) &&
		semantics.TableIdentityKey(a.Name) == semantics.TableIdentityKey(b.Name)
}

// SameSlot reports whether two predicates occupy one slot under semantics.
func SameSlot(semantics identifier.Semantics, a, b Predicate) bool {
	return a.Type == b.Type && a.Operation == b.Operation && SameName(semantics, a.Table, b.Table)
}

// CompareInvocation compares the function and arguments of a declared
// predicate with those of the observed predicate in its slot. A function of
// another name, or another number of arguments, Differs. Otherwise the
// arguments decide, each by [CompareArgument]: one that Differs makes the
// invocation Differ, and one that is Undecided leaves it Undecided.
func CompareInvocation(semantics identifier.Semantics, declared, observed Predicate) Agreement {
	if !SameName(semantics, declared.Function, observed.Function) || len(declared.Arguments) != len(observed.Arguments) {
		return Differ
	}
	result := Agree
	for i, argument := range declared.Arguments {
		switch CompareArgument(argument, observed.Arguments[i]) {
		case Differ:
			return Differ
		case Undecided:
			result = Undecided
		}
	}
	return result
}

// ComparePolicy compares a declaration with an observation under semantics.
// It Differs when the resolved state, schema binding or replication behavior
// differs, when a slot is held on one side only, or when an invocation
// Differs. It is Undecided when every slot pairs and some invocation is
// Undecided; the reason then names that predicate and both spellings.
func ComparePolicy(semantics identifier.Semantics, declared *DesiredSecurityPolicy, observed *ObservedSecurityPolicy) (Agreement, string) {
	enabled, schemaBinding, notForReplication := declared.Resolved()
	if enabled != observed.Enabled || schemaBinding != observed.SchemaBinding || notForReplication != observed.NotForReplication ||
		len(declared.Predicates) != len(observed.Predicates) {
		return Differ, ""
	}
	result, reason := Agree, ""
	for _, want := range SortedPredicates(declared.Predicates) {
		index := slices.IndexFunc(observed.Predicates, func(got Predicate) bool { return SameSlot(semantics, want, got) })
		if index < 0 {
			return Differ, ""
		}
		got := observed.Predicates[index]
		switch CompareInvocation(semantics, want, got) {
		case Differ:
			return Differ, ""
		case Undecided:
			if result == Agree {
				result, reason = Undecided, fmt.Sprintf("the %s predicate on %s is declared as %s(%s), and SQL Server stores %s(%s); "+
					"the server rewrites argument expressions, so only it can tell whether they are one invocation",
					describe(want), want.Table, want.Function, strings.Join(want.Arguments, ", "), got.Function, strings.Join(got.Arguments, ", "))
			}
		}
	}
	return result, reason
}

// SortedPredicates returns an independent copy of predicates in the canonical
// order the codecs encode: by table, type, operation, function and arguments.
func SortedPredicates(predicates []Predicate) []Predicate {
	sorted := clonePredicates(predicates)
	sortPredicates(sorted)
	return sorted
}
