package identifier_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform/identifier"
)

// catalogResolvedSemantics is a target that resolved exactly one name. Everything
// else is unresolved, which is the ordinary state of a desired schema naming
// objects the database does not have yet.
func catalogResolvedSemantics() identifier.Semantics {
	return identifier.Semantics{
		DefaultSchema: "dbo",
		TableNames:    identifier.ComparisonCatalogResolved,
		ColumnNames:   identifier.ComparisonCatalogResolved,
		IndexNames:    identifier.ComparisonCatalogResolved,
		ResolvedNames: []identifier.ResolvedName{
			{Name: "known", Key: "KNOWN"},
		},
	}
}

// unresolvedIdentityCase is one pair of names and whether the two questions --
// "same object" and "could collide" -- should answer alike.
type unresolvedIdentityCase struct {
	name          string
	left          string
	right         string
	wantSameID    bool
	wantSameClash bool
}

// TestSemantics_UnresolvedNamesKeepTheirIdentity pins that a name the catalog
// did not resolve keeps its spelling for identity while staying conservative for
// conflicts.
//
// Both keys used to return one shared constant for anything unresolved, so every
// unresolved name compared equal to every other. A map keyed by identity kept
// exactly one of them: two grants declared on two tables that do not exist yet
// became one grant, and the other was silently dropped from the plan
// (stokaro/ptah#1290).
//
// The split is the fix. Identity distinguishes them, because they are different
// objects; conflict detection still lumps them together, because the target's
// collation has not spoken and either may collide with anything.
func TestSemantics_UnresolvedNamesKeepTheirIdentity(t *testing.T) {
	c := qt.New(t)
	semantics := catalogResolvedSemantics()

	tests := []unresolvedIdentityCase{
		{
			// The shape that dropped a grant.
			name:          "two unresolved names are different objects that may still collide",
			left:          "alpha",
			right:         "beta",
			wantSameID:    false,
			wantSameClash: true,
		},
		{
			name:          "an unresolved name is the same object as itself",
			left:          "alpha",
			right:         "alpha",
			wantSameID:    true,
			wantSameClash: true,
		},
		{
			// The control that keeps the fix from becoming "never match": a
			// resolved name still answers with the target's own key.
			name:          "a resolved name does not become an unresolved one",
			left:          "known",
			right:         "alpha",
			wantSameID:    false,
			wantSameClash: false,
		},
		{
			// And a resolved name still matches itself through that key rather
			// than through its spelling.
			name:          "a resolved name matches itself",
			left:          "known",
			right:         "known",
			wantSameID:    true,
			wantSameClash: true,
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			sameID := semantics.TableIdentityKey(test.left) == semantics.TableIdentityKey(test.right)
			c.Assert(sameID, qt.Equals, test.wantSameID,
				qt.Commentf("identity keys: %q -> %q, %q -> %q",
					test.left, semantics.TableIdentityKey(test.left),
					test.right, semantics.TableIdentityKey(test.right)))

			sameClash := semantics.TableConflictKey(test.left) == semantics.TableConflictKey(test.right)
			c.Assert(sameClash, qt.Equals, test.wantSameClash,
				qt.Commentf("conflict keys: %q -> %q, %q -> %q",
					test.left, semantics.TableConflictKey(test.left),
					test.right, semantics.TableConflictKey(test.right)))
		})
	}
}

// qualifiedUnresolvedCase is one pair of schema-qualified names.
type qualifiedUnresolvedCase struct {
	name       string
	left       string
	right      string
	wantSameID bool
}

// TestSemantics_QualifiedUnresolvedNamesKeepTheirIdentity is the same property
// through the qualified entry point, which is what the comparators actually
// call: [Semantics.QualifiedTableIdentityKey] keys both halves of a
// schema.table separately, so the collapse reached tables in different schemas
// as well as tables in one.
func TestSemantics_QualifiedUnresolvedNamesKeepTheirIdentity(t *testing.T) {
	c := qt.New(t)
	semantics := catalogResolvedSemantics()

	tests := []qualifiedUnresolvedCase{
		{
			name:       "two unresolved tables in one unresolved schema",
			left:       "app.alpha",
			right:      "app.beta",
			wantSameID: false,
		},
		{
			name:       "one unresolved table name in two unresolved schemas",
			left:       "app.alpha",
			right:      "other.alpha",
			wantSameID: false,
		},
		{
			name:       "the same qualified name twice",
			left:       "app.alpha",
			right:      "app.alpha",
			wantSameID: true,
		},
		{
			// An unqualified name resolves through DefaultSchema, so it is the
			// same object as the explicitly qualified spelling. This is the row
			// #1232 and #1283 exist for, and it must survive the fix.
			name:       "an unqualified name is its default-schema self",
			left:       "alpha",
			right:      "dbo.alpha",
			wantSameID: true,
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			left := semantics.QualifiedTableIdentityKey(test.left)
			right := semantics.QualifiedTableIdentityKey(test.right)
			c.Assert(left == right, qt.Equals, test.wantSameID,
				qt.Commentf("%q -> %q, %q -> %q", test.left, left, test.right, right))
		})
	}
}
