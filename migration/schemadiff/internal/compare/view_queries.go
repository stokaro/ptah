package compare

import (
	"cmp"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacomparison"
	"ptah.run/core/schemamodel"
)

// ViewQueryPairs returns the queries [ViewsWithSemantics] and
// [MaterializedViewsWithSemantics] compare: one pair for each declared view
// or materialized view and the one the database holds that the walks match it
// with, by the same walks, so a target asked about these pairs is asked about
// every query the comparison reads and no other. A materialized view's
// declared query is without its data clause, as the comparison reads it. The
// pairs are ordered by kind, schema and name, and each held view is named
// once: a second declaration the name walk matches with the same view is left
// out, and its query is compared as text. A pair with an empty query on
// either side is left out too.
func ViewQueryPairs(desired *schemamodel.Database, database *catalog.Database, dialect string, semantics identifier.Semantics) []schemacomparison.ViewQueryPair {
	if desired == nil || database == nil {
		return nil
	}
	var pairs []schemacomparison.ViewQueryPair
	add := func(pair schemacomparison.ViewQueryPair) {
		if strings.TrimSpace(pair.Held) != "" && strings.TrimSpace(pair.Declared) != "" {
			pairs = append(pairs, pair)
		}
	}
	views := plainViewFamily
	views.Compare = func(declared schemamodel.View, held catalog.View, _ string) difftypes.ViewDiff {
		add(schemacomparison.ViewQueryPair{Kind: objectidentity.KindView, Schema: held.Schema, Name: held.Name, Held: held.Body, Declared: declared.Body})
		return difftypes.ViewDiff{}
	}
	compareViewLikesByIdentity(views, desired, database, &difftypes.SchemaDiff{}, dialect, semantics)
	materialized := materializedViewFamily
	materialized.Compare = func(declared schemamodel.MaterializedView, held catalog.MaterializedView, _ string) difftypes.MaterializedViewDiff {
		add(schemacomparison.ViewQueryPair{Kind: objectidentity.KindMatView, Schema: held.Schema, Name: held.Name, Held: held.Body,
			Declared: MaterializedViewQuery(declared.Body)})
		return difftypes.MaterializedViewDiff{}
	}
	compareViewLikesByIdentity(materialized, desired, database, &difftypes.SchemaDiff{}, dialect, semantics)
	slices.SortStableFunc(pairs, func(a, b schemacomparison.ViewQueryPair) int {
		return cmp.Or(cmp.Compare(a.Kind, b.Kind), cmp.Compare(a.Schema, b.Schema), cmp.Compare(a.Name, b.Name), cmp.Compare(a.Declared, b.Declared))
	})
	return slices.CompactFunc(pairs, func(a, b schemacomparison.ViewQueryPair) bool {
		return a.Kind == b.Kind && a.Schema == b.Schema && a.Name == b.Name
	})
}

// AnsweredViewQueries is the view rule a comparison reads once a target
// answered pairs, one answer for each pair, in order: two queries are one
// query when the target called them equivalent. A query pair the target was
// not asked about is one query only when its texts are the same, and a query
// pair it answered twice in two ways is no longer one. The queries are read
// as the comparison reads them: a materialized view's without its data
// clause on both sides.
func AnsweredViewQueries(pairs []schemacomparison.ViewQueryPair, answers []bool) schemacomparison.Views {
	answered := make(answeredViews, len(pairs))
	for i, pair := range pairs {
		held := pair.Held
		if pair.Kind == objectidentity.KindMatView {
			held = MaterializedViewQuery(held)
		}
		key := [2]string{pair.Declared, held}
		equivalent, seen := answered[key]
		answered[key] = answers[i] && (!seen || equivalent)
	}
	return answered
}

// answeredViews holds a target's answers, keyed by the declared and the held
// query.
type answeredViews map[[2]string]bool

func (a answeredViews) QueryEqual(desired, current string) bool {
	if equivalent, asked := a[[2]string{desired, current}]; asked {
		return equivalent
	}
	return desired == current
}
