package generator

import (
	"fmt"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/objectidentity"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/internal/tableidentity"
	"ptah.run/internal/viewcapture"
)

// refuseUnrestorableMaterializedViews refuses a removal whose inverse would
// recreate the view without a setting the read could not capture. A view's
// query does not establish its storage: a ClickHouse sorting key or TO target
// cannot be restored from it. The comparison protects replacements; this
// boundary protects the inverse of a removal before either file is written.
// A retained view's unread settings do not affect another view's rollback.
func refuseUnrestorableMaterializedViews(
	diff *difftypes.SchemaDiff,
	current *catalog.Database,
	dialect string,
	runtime schemaext.TargetResolver,
) error {
	if len(diff.MaterializedViewsRemoved) == 0 || current.FeatureCoverage.IsZero() {
		return nil
	}
	selected, err := runtime.ResolveTarget(dialect)
	if err != nil {
		return err
	}
	semantics := diff.EffectiveIdentifierSemantics(dialect)
	coverage, err := tableidentity.BindComparisonCoverage(current.FeatureCoverage, selected, semantics)
	if err != nil {
		return fmt.Errorf("capture materialized view rollback settings: %w", err)
	}
	identities := objectidentity.NewBuilder(semantics)
	for _, removal := range diff.MaterializedViewsRemoved {
		subject := identities.SchemaScoped(objectidentity.KindMatView, removal.Name)
		unread := viewcapture.UnreadSettings(coverage, subject)
		if len(unread) == 0 {
			continue
		}
		record := unread[0]
		return fmt.Errorf("%w: cannot plan the rollback of removing materialized view %s: its %q could not be captured (%s); "+
			"restoring it would omit that setting. Keep the view or write migration SQL that restores its complete definition",
			ptaherr.ErrUnsupportedFeature, removal.Name, record.Kind, record.Knowledge.Reason)
	}
	return nil
}
