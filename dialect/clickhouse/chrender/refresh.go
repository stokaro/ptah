package chrender

import (
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/dialect/clickhouse/chast"
)

func validateRefresh(ctx renderer.ExtensionContext, op *chast.ModifyRefresh) error {
	if ctx.Target != platform.ClickHouse {
		return fmt.Errorf("%w: ClickHouse MODIFY REFRESH operation on %q", ptaherr.ErrUnsupportedDialect, ctx.Target)
	}
	return op.Validate()
}

// renderRefresh changes a refreshable materialized view's schedule in place,
// which is the one way to change it without losing the rows the view holds
// (stokaro/ptah#1802).
func renderRefresh(ctx renderer.ExtensionContext, op *chast.ModifyRefresh) ([]string, error) {
	return []string{fmt.Sprintf("ALTER TABLE %s MODIFY REFRESH %s;", quoteTable(ctx.Parent.Name), op.Schedule.Clause())}, nil
}
