// Package constraintscope derives the identity that makes two constraint
// records one object.
//
// It exists because more than one producer writes constraint changes into a
// [difftypes.SchemaDiff]: the comparator does, and the generator does again when
// it reverses a diff into a down migration. A derivation each would be two rules
// for one question, and the answer has to be the same one or a down migration
// pairs its drops differently from the up migration it undoes.
//
// It is the constraint counterpart of [go.5x5.cz/ptah/internal/indexscope], and
// lives outside [difftypes] for the same reason that one does: the fold needs
// the target's rules and the identity model, and the diff types are the surface
// an embedder builds by hand.
package constraintscope

import (
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/internal/objectidentity"
	"go.5x5.cz/ptah/internal/tableref"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

// Identity folds a constraint's owning table and its own name into the form two
// records are compared by.
//
// The table arrives as one string because that is how both a description and a
// catalog report it, and it may or may not carry a schema. Parsing is delegated
// to [tableref] so a table whose own name contains a dot -- quoted as
// `"tenant.data"` -- is not mistaken for a schema-qualified one.
//
// An empty name yields the zero identity rather than an identity of empty parts.
// The two are the same value, and a consumer that keys on identity has to treat
// it as "nobody answered" rather than as a constraint in no schema on no table.
func Identity(
	semantics identifier.Semantics,
	table, constraint string,
) difftypes.ConstraintIdentity {
	if constraint == "" {
		return difftypes.ConstraintIdentity{}
	}
	ref, ok := tableref.Parse(table)
	if !ok {
		ref.Name = table
	}
	id := objectidentity.NewBuilder(semantics).ConstraintParts(ref.Schema, ref.Name, constraint)
	return difftypes.ConstraintIdentity{
		Schema: id.Schema.Normalized,
		Table:  id.Parent.Normalized,
		Name:   id.Name.Normalized,
	}
}

// Normalize fills the identity of every constraint record that does not carry
// one, and leaves every record that does untouched.
//
// A diff built by the comparator arrives with identities already resolved. One
// built by hand does not: [difftypes.SchemaDiff] is the surface an embedder
// constructs directly, and a planner that keyed on an unfilled identity would
// read the zero value as a single key and pair every such constraint with every
// other. Normalizing once at the door is what lets everything inside compare
// identities and nothing re-derive them.
//
// It never rewrites a spelling and never replaces an identity a producer
// resolved, so running it twice changes nothing.
//
// It used to do a second job: synthesize a record for a name the diff carried
// with none. There are no bare names any more -- a constraint change IS its
// record -- so that half is gone (stokaro/ptah#2315).
func Normalize(diff *difftypes.SchemaDiff, semantics identifier.Semantics) {
	if diff == nil {
		return
	}
	for i := range diff.ConstraintsAdded {
		add := &diff.ConstraintsAdded[i]
		if add.Identity == (difftypes.ConstraintIdentity{}) {
			add.Identity = Identity(semantics, add.TableName, add.Name)
		}
	}
	for i := range diff.ForeignKeysRemovedWithTables {
		removal := &diff.ForeignKeysRemovedWithTables[i]
		if removal.Identity == (difftypes.ConstraintIdentity{}) {
			removal.Identity = Identity(semantics, removal.TableName, removal.Name)
		}
	}
	for i := range diff.ConstraintsRemoved {
		removal := &diff.ConstraintsRemoved[i]
		if removal.Identity == (difftypes.ConstraintIdentity{}) {
			removal.Identity = Identity(semantics, removal.TableName, removal.Name)
		}
	}
}

// AdditionNames lists the constraints a diff adds, by name, from the records.
//
// It replaces reading the bare ConstraintsAdded list. The two carry the same
// names with the same multiplicity once [Normalize] has run, which is what
// covers a diff that arrived with names and no records -- and the records are
// the ones that also say which table, so a consumer reading them can stop
// asking two questions to answer one (stokaro/ptah#1663).
//
// Every addition is listed, not only the ones with no table. For a non-FK
// modify the name-only path is what emits the DROP that has to precede the
// re-add, and it does that for a host it knows as well as for one it does not;
// listing only the hostless ones loses that drop, measured.
func AdditionNames(diff *difftypes.SchemaDiff) []string {
	if diff == nil {
		return nil
	}
	names := make([]string, 0, len(diff.ConstraintsAdded))
	for _, info := range diff.ConstraintsAdded {
		names = append(names, info.Name)
	}
	return names
}

// RemovalNames lists the constraints a diff removes, by name, from the records.
//
// It is [AdditionNames] for the other direction, and it exists for the same
// reason: the bare ConstraintsRemoved list and the records answer one question,
// and only the records also say which table. Once [Normalize] has run the two
// carry the same names with the same multiplicity, so a consumer can stop
// asking twice (stokaro/ptah#1663).
func RemovalNames(diff *difftypes.SchemaDiff) []string {
	if diff == nil {
		return nil
	}
	names := make([]string, 0, len(diff.ConstraintsRemoved))
	for _, info := range diff.ConstraintsRemoved {
		names = append(names, info.Name)
	}
	return names
}
