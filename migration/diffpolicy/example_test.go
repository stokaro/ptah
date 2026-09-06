package diffpolicy_test

import (
	"fmt"

	"ptah.run/core/schemamodel"
	"ptah.run/migration/diffpolicy"
	"ptah.run/migration/schemadiff/difftypes"
)

// ExampleApply filters a schema diff through a policy that skips table drops.
// The table drop disappears together with the dependent index removal a kept
// table must retain, the removal on another table survives, and the returned
// skipped changes carry the comment a caller emits in place of the omitted
// statement. This is the entry point for an embedder that wants to keep a
// migration plan from destroying tables the target schema does not declare.
//
// SkippedChange.Comment is the single source of truth for that wording, for
// the planner, the generator and an embedder alike; it carries no SQL comment
// marker, so a caller writing its own migration output prefixes the string
// rather than composing a second wording.
func ExampleApply() {
	diff := &difftypes.SchemaDiff{
		TablesRemoved: []string{"legacy_orders"},
		IndexesRemoved: []difftypes.IndexRef{
			{Name: "idx_legacy_orders_ref", TableName: "legacy_orders"},
			{Name: "idx_users_email", TableName: "users"},
		},
	}

	filtered, skipped := diffpolicy.Apply(diff, diffpolicy.NewSkipSet(diffpolicy.DropTable))

	fmt.Println("table drops left:", len(filtered.TablesRemoved))
	for _, ref := range filtered.IndexRemovals() {
		fmt.Println("index drop left:", ref.TableName+"."+ref.Name)
	}
	for _, change := range skipped {
		fmt.Println(change.Comment())
	}

	// Output:
	// table drops left: 0
	// index drop left: users.idx_users_email
	// SKIP: DROP TABLE of legacy_orders omitted by diff policy (skip: drop_table)
}

// ExampleParseChangeKind validates the strings a project config (ptah.yaml
// diff.skip) carries before they become a SkipSet. Matching is
// case-insensitive and tolerant of surrounding whitespace; an unknown kind is
// refused with an error naming the kinds that are supported, so a typo
// surfaces instead of silently skipping nothing.
func ExampleParseChangeKind() {
	kind, err := diffpolicy.ParseChangeKind("  Drop_Table ")
	if err != nil {
		fmt.Println("refused:", err)
		return
	}
	fmt.Println("parsed:", kind)

	_, err = diffpolicy.ParseChangeKind("drop_sequence")
	fmt.Println("drop_sequence accepted:", err == nil)

	// Output:
	// parsed: drop_table
	// drop_sequence accepted: false
}

// ExampleApplyForDialect skips index drops for a PostgreSQL target, where
// index names are schema-scoped. The removal of idx_app_ref is preserved
// because an addition recreates that name in the same schema — on a different
// table — so the drop must still run for the CREATE INDEX to succeed; only the
// standalone removal is skipped. Plain Apply uses conservative table-scoped
// rules and would have skipped the replacement's drop too, leaving a plan
// whose create collides with the index it never dropped.
func ExampleApplyForDialect() {
	diff := &difftypes.SchemaDiff{}
	diff.SetIndexAdditions(difftypes.IndexChanges{
		{Index: schemamodel.Index{Name: "idx_app_ref", Fields: []string{"ref"}}, TableName: "app.orders"},
	})
	diff.SetIndexRemovals([]difftypes.IndexRef{
		{Name: "idx_app_ref", TableName: "app.archive"},
		{Name: "idx_dead", TableName: "app.archive"},
	})

	filtered, skipped := diffpolicy.ApplyForDialect(
		diff,
		diffpolicy.NewSkipSet(diffpolicy.DropIndex),
		"postgres",
	)

	for _, ref := range filtered.IndexRemovals() {
		fmt.Println("kept for replacement:", ref.TableName+"."+ref.Name)
	}
	for _, change := range skipped {
		fmt.Println(change.Comment())
	}

	// Output:
	// kept for replacement: app.archive.idx_app_ref
	// SKIP: DROP INDEX of app.archive.idx_dead omitted by diff policy (skip: drop_index)
}
