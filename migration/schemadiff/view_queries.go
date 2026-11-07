package schemadiff

import (
	"context"
	"fmt"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacomparison"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/internal/compare"
)

// viewLikeComparison is what [compareViewLikes] reads: both sides, the diff
// it adds to, the comparison's options, identifier rules and capabilities,
// the target's rules, and the changes the feature comparison reported.
type viewLikeComparison struct {
	desired   *schemamodel.Database
	database  *catalog.Database
	diff      *difftypes.SchemaDiff
	opts      *config.CompareOptions
	semantics identifier.Semantics
	caps      capability.Capabilities
	rules     schemacomparison.Rules
	changes   []schemaext.ChangeRecord
}

// compareViewLikes compares the views and the materialized views of both
// sides, their queries by the rule [decideViewQueries] returns, and attaches
// the changes of materialized views the feature comparison reported.
func compareViewLikes(ctx context.Context, comparison viewLikeComparison) error {
	views, err := decideViewQueries(ctx, comparison.rules, comparison.desired, comparison.database, comparison.opts.Dialect, comparison.semantics, comparison.caps)
	if err != nil {
		return err
	}
	compare.ViewsWithSemantics(comparison.desired, comparison.database, comparison.diff, comparison.opts.Dialect, comparison.semantics, comparison.opts.ViewBodies, views)
	compare.MaterializedViewsWithSemantics(comparison.desired, comparison.database, comparison.diff, comparison.opts.Dialect, comparison.semantics,
		comparison.opts.ViewBodies, views)
	return attachMaterializedViewChanges(comparison.diff, comparison.desired, comparison.database, comparison.changes, comparison.semantics)
}

// decideViewQueries returns the view rule the comparison of views and
// materialized views reads. A target that decides view queries itself
// ([schemacomparison.Rules.ViewQueries]) is asked about every query the
// comparison pairs, in one batch, and the comparison then reads its answers
// alone: a held view whose query the target calls equivalent stays, and one
// it does not is replaced, whatever the shared reading of the two texts
// says. Any other target keeps its own rule, nil where it registers none.
func decideViewQueries(
	ctx context.Context,
	rules schemacomparison.Rules,
	desired *schemamodel.Database,
	database *catalog.Database,
	dialect string,
	semantics identifier.Semantics,
	caps capability.Capabilities,
) (schemacomparison.Views, error) {
	if rules.ViewQueries == nil {
		return rules.Views, nil
	}
	pairs := compare.ViewQueryPairs(desired, database, dialect, semantics)
	if len(pairs) == 0 {
		return compare.AnsweredViewQueries(nil, nil), nil
	}
	answers, err := rules.ViewQueries.EquivalentViewQueries(ctx, schemacomparison.ViewQueriesRequest{
		Target: dialect, Identifiers: semantics, Capabilities: caps, Pairs: pairs,
	})
	if err != nil {
		return nil, fmt.Errorf("compare schemas: %w", err)
	}
	if len(answers) != len(pairs) {
		return nil, fmt.Errorf("compare schemas: the target answered %d of %d view query comparisons", len(answers), len(pairs))
	}
	return compare.AnsweredViewQueries(pairs, answers), nil
}
