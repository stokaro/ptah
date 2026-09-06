package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/exprkey"
	"ptah.run/migration/schemadiff/difftypes"
	"ptah.run/migration/schemadiff/internal/compare"
)

// TestIndexes_TwoIndexesDifferingOnlyByCaseKeepTheirOwnPredicate pins that a
// resolved index predicate belongs to the index it was resolved for.
//
// PostgreSQL keeps index names in the schema namespace and preserves their
// case, so `Idx` and `idx` are two indexes. A key that folds the name with
// strings.ToLower spells one key for both, so the predicate the server resolved
// for one answers the lookup for the other, and a genuine difference on the
// second is reported as no change.
func TestIndexes_TwoIndexesDifferingOnlyByCaseKeepTheirOwnPredicate(t *testing.T) {
	c := qt.New(t)

	// What PostgreSQL stores for `unit >= 0` over a numeric column.
	const stored = "(unit >= (0)::numeric)"

	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t"}},
		Fields: []schemamodel.Field{{StructName: "T", Name: "unit", Type: "NUMERIC"}},
		Indexes: []schemamodel.Index{
			{StructName: "T", Name: "Idx", Fields: []string{"unit"}, Condition: "unit >= 0"},
			// Genuinely different from what the catalog holds, so this pair has
			// to be reported.
			{StructName: "T", Name: "idx", Fields: []string{"unit"}, Condition: "unit >= 5"},
		},
	}
	current := &catalog.Database{
		Tables: []catalog.Table{{Name: "t", Schema: "public"}},
		Indexes: []catalog.Index{
			{Name: "Idx", TableName: "t", Schema: "public", Columns: []string{"unit"}, Condition: stored},
			{Name: "idx", TableName: "t", Schema: "public", Columns: []string{"unit"}, Condition: stored},
		},
	}

	// Only `Idx` was resolved, and it agrees with the catalog.
	indexes := map[string]config.IndexExpression{
		indexKey("Idx"): {Predicate: stored, Resolved: true},
	}

	diff := &difftypes.SchemaDiff{}
	compare.IndexesWithSemantics(desired, current, diff, platform.Postgres, indexSemantics(), indexes)

	c.Assert(indexNames(diff.IndexAdditions()), qt.DeepEquals, []string{"idx"},
		qt.Commentf("idx's declared predicate differs from its catalog one and must be reported"))
}

func indexNames(refs []difftypes.IndexRef) []string {
	names := make([]string, 0, len(refs))
	for _, ref := range refs {
		names = append(names, ref.Name)
	}
	return names
}

func indexSemantics() identifier.Semantics {
	semantics := identifier.ForDialect(platform.Postgres)
	semantics.DefaultSchema = "public"
	return semantics
}

// indexKey builds a [config.CompareOptions.IndexExpressions] key the way the
// resolver that fills that map does. The spelling is an opaque identity, so a
// literal here would pin the encoding rather than the behavior.
func indexKey(name string) string {
	return exprkey.Index(indexSemantics(), "public.t", name)
}
