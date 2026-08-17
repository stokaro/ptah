package objectidentity_test

import (
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/objectidentity"
)

// TestResolve_RefusalClasses pins the four ways a reference fails to name one
// object, as four DISTINCT errors.
//
// They are separate because they ask the author for different things: an
// ambiguous reference needs qualifying, a dangling one needs the object created
// or the name corrected, a collision needs one of two objects renamed, and a
// missing component needs the source to carry it. Collapsing them into one
// "cannot resolve" would tell an operator that something is wrong and not what.
func TestResolve_RefusalClasses(t *testing.T) {
	builder := objectidentity.NewBuilder(postgresSemantics())

	tests := []struct {
		name       string
		reference  objectidentity.Reference
		candidates []objectidentity.ID
		wantErr    error
	}{
		{
			name:       "a name nothing carries is dangling",
			reference:  objectidentity.Reference{ID: builder.Table("public.missing"), Origin: "table.orders"},
			candidates: []objectidentity.ID{builder.Table("public.orders")},
			wantErr:    objectidentity.ErrDanglingReference,
		},
		{
			// Two candidates that fold together and spell differently cannot
			// both exist on the target, so no answer about the reference would
			// be meaningful and the candidate list is what gets reported.
			name:      "two candidates folding together is a collision",
			reference: objectidentity.Reference{ID: builder.Table("public.orders")},
			candidates: []objectidentity.ID{
				builder.Table("public.orders"),
				builder.Table("public.ORDERS"),
			},
			wantErr: objectidentity.ErrNormalizedCollision,
		},
		{
			// A policy without its table names nothing in particular. Picking
			// the first match would be the guess the fail-closed rule refuses.
			name: "a policy without its table is missing a component",
			reference: objectidentity.Reference{ID: objectidentity.ID{
				Kind: objectidentity.KindPolicy,
				Name: objectidentity.Part{Source: "tenant_isolation", Normalized: "tenant_isolation"},
			}},
			candidates: []objectidentity.ID{builder.Policy("public.orders", "tenant_isolation")},
			wantErr:    objectidentity.ErrMissingComponent,
		},
		{
			name:       "a reference with no name at all",
			reference:  objectidentity.Reference{ID: objectidentity.ID{Kind: objectidentity.KindTable}},
			candidates: []objectidentity.ID{builder.Table("public.orders")},
			wantErr:    objectidentity.ErrMissingComponent,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := objectidentity.Resolve(test.reference, test.candidates)

			c.Assert(err, qt.ErrorIs, test.wantErr)
		})
	}
}

// TestResolve_FindsTheOneMatch is the control for the table above: a resolver
// that refused everything would satisfy every refusal row.
//
// It also pins which spelling comes back. The candidate's is the schema's own,
// and that is what a renderer must emit -- returning the reference's spelling
// would put the CALLER's casing into DDL.
func TestResolve_FindsTheOneMatch(t *testing.T) {
	c := qt.New(t)
	builder := objectidentity.NewBuilder(postgresSemantics())
	candidates := []objectidentity.ID{
		builder.Table(`public."Orders"`),
		builder.Table("public.Orders"),
		builder.Table("billing.orders"),
	}

	resolved, err := objectidentity.Resolve(
		objectidentity.Reference{ID: builder.Table("public.ORDERS")}, candidates)

	c.Assert(err, qt.IsNil)
	c.Assert(resolved.Name.Source, qt.Equals, "Orders")
}

// TestResolve_AmbiguityNeedsTwoIdenticalCandidates separates ambiguity from
// collision, which is the pair most easily conflated: identical candidates are
// ambiguous, differently-spelled ones that fold together are a collision.
func TestResolve_AmbiguityNeedsTwoIdenticalCandidates(t *testing.T) {
	c := qt.New(t)
	builder := objectidentity.NewBuilder(postgresSemantics())
	orders := builder.Table("public.orders")

	_, err := objectidentity.Resolve(
		objectidentity.Reference{ID: orders}, []objectidentity.ID{orders, orders})

	c.Assert(err, qt.ErrorIs, objectidentity.ErrAmbiguousReference)
}

// TestRequireScope pins the fourth refusal: a reference leaving the schema it
// is defined to stay within is an invalid scope transition rather than a
// dangling name, and saying "dangling" would send the author looking for a
// missing object instead of a misplaced reference.
func TestRequireScope(t *testing.T) {
	builder := objectidentity.NewBuilder(postgresSemantics())

	tests := []struct {
		name    string
		table   string
		scope   string
		wantErr error
	}{
		{name: "a reference inside its scope", table: "public.orders", scope: "public"},
		{name: "a reference leaving its scope", table: "billing.orders", scope: "public", wantErr: objectidentity.ErrInvalidScope},
		{name: "no scope constrains nothing", table: "billing.orders"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			scoped := builder.Table(test.scope + ".x")
			scope := pickScope(test.scope, scoped.Schema)

			err := objectidentity.RequireScope(
				objectidentity.Reference{ID: builder.Table(test.table)}, scope)

			c.Assert(errorIs1345(err, test.wantErr), qt.IsTrue,
				qt.Commentf("got %v, want %v", err, test.wantErr))
		})
	}
}

// TestCollisions_ReportsEveryPair pins that a whole-schema validation gets every
// collision at once rather than the first one a lookup happened to hit.
func TestCollisions_ReportsEveryPair(t *testing.T) {
	c := qt.New(t)
	builder := objectidentity.NewBuilder(postgresSemantics())

	reports := objectidentity.Collisions([]objectidentity.ID{
		builder.Table("public.orders"),
		builder.Table("public.ORDERS"),
		builder.Table("public.invoices"),
		builder.Table("public.Invoices"),
		builder.Table("public.customers"),
	})

	c.Assert(reports, qt.HasLen, 2)
	c.Assert(reports[0], qt.Contains, "fold to one identity")
}

// pickScope returns the scope part for a row, or the empty part for the row
// that constrains nothing. It is a helper so the test body stays branch-free.
func pickScope(scope string, part objectidentity.Part) objectidentity.Part {
	parts := map[bool]objectidentity.Part{true: {}, false: part}
	return parts[scope == ""]
}

// errorIs1345 answers both the "want an error" and "want none" rows with one
// expression.
func errorIs1345(err, want error) bool {
	checks := map[bool]func() bool{
		true:  func() bool { return err == nil },
		false: func() bool { return err != nil && errors.Is(err, want) },
	}
	return checks[want == nil]()
}
