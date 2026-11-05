package planner

import (
	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/planext"
	"ptah.run/migration/diffpolicy"
)

// omitSkippedChanges returns diff without the changes of the kinds listed, and
// one comment for each change it left out, in the order
// [diffpolicy.ApplyForDialect] reports them.
//
// It runs here, ahead of the target's planner, so a plan omits the same
// changes whichever planner builds it: a bundled dialect, another family's
// planner, or a provider's. The planner is never handed a skipped change, and
// a planner that read the kinds itself would apply them a second time.
func omitSkippedChanges(diff *difftypes.SchemaDiff, kinds []planext.ChangeKind, dialect string) (*difftypes.SchemaDiff, []ast.Node) {
	skip := diffpolicy.NewSkipSet(kinds...)
	if skip.Empty() {
		return diff, nil
	}
	filtered, skipped := diffpolicy.ApplyForDialect(diff, skip, dialect)
	comments := make([]ast.Node, 0, len(skipped))
	for _, change := range skipped {
		comments = append(comments, ast.NewComment(change.Comment()))
	}
	return filtered, comments
}
