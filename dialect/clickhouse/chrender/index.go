package chrender

import (
	"fmt"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/dialect/clickhouse/chast"
	"ptah.run/internal/sqlident"
	"ptah.run/internal/tableref"
)

func validateIndex(ctx renderer.ExtensionContext, op *chast.AddSkippingIndex) error {
	if ctx.Target != platform.ClickHouse {
		return fmt.Errorf("%w: ClickHouse ADD INDEX operation on %q", ptaherr.ErrUnsupportedDialect, ctx.Target)
	}
	return op.Validate()
}

func renderIndex(ctx renderer.ExtensionContext, op *chast.AddSkippingIndex) ([]string, error) {
	indexType := op.IndexType
	if indexType == "" {
		indexType = "minmax"
	}
	granularity := op.Granularity
	if granularity == 0 {
		granularity = 1
	}
	return []string{fmt.Sprintf("ALTER TABLE %s ADD INDEX %s %s TYPE %s GRANULARITY %d;",
		quoteTable(ctx.Parent.Name), quoteIndex(op.Name), op.Expression, indexType, granularity)}, nil
}

func quoteIndex(name string) string {
	if strings.HasPrefix(name, "`") && strings.HasSuffix(name, "`") && len(name) >= 2 {
		name = strings.ReplaceAll(name[1:len(name)-1], "``", "`")
	}
	return sqlident.Quote(platform.ClickHouse, name)
}

func quoteTable(name string) string {
	ref, ok := tableref.Parse(name)
	if !ok {
		return quoteIndex(name)
	}
	quoted := sqlident.Quote(platform.ClickHouse, ref.Name)
	if ref.Qualified {
		quoted = sqlident.Quote(platform.ClickHouse, ref.Schema) + "." + quoted
	}
	return quoted
}
