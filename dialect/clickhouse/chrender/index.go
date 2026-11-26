package chrender

import (
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/dialect/clickhouse/chast"
	"ptah.run/internal/chkey"
	"ptah.run/internal/sqlident"
)

func validateIndex(ctx renderer.ExtensionContext, op *chast.AddSkippingIndex) error {
	if ctx.Target != platform.ClickHouse {
		return fmt.Errorf("%w: ClickHouse ADD INDEX operation on %q", ptaherr.ErrUnsupportedDialect, ctx.Target)
	}
	return op.Validate()
}

func renderIndex(ctx renderer.ExtensionContext, op *chast.AddSkippingIndex) ([]string, error) {
	if err := chkey.RefuseQuotedNot(fmt.Sprintf("the key of index %q on table %q", op.Name, ctx.Parent.Name), op.Expression); err != nil {
		return nil, err
	}
	indexType := op.IndexType
	if indexType == "" {
		indexType = "minmax"
	}
	granularity := max(op.Granularity, 1)
	return []string{fmt.Sprintf("ALTER TABLE %s ADD INDEX %s %s TYPE %s GRANULARITY %d;",
		quoteTable(ctx.Parent.Name), quoteIndex(op.Name), op.Expression, indexType, granularity)}, nil
}

func validateDropIndex(ctx renderer.ExtensionContext, op *chast.DropSkippingIndex) error {
	if ctx.Target != platform.ClickHouse {
		return fmt.Errorf("%w: ClickHouse DROP INDEX operation on %q", ptaherr.ErrUnsupportedDialect, ctx.Target)
	}
	return op.Validate()
}

func renderDropIndex(ctx renderer.ExtensionContext, op *chast.DropSkippingIndex) ([]string, error) {
	return []string{fmt.Sprintf("ALTER TABLE %s DROP INDEX %s;", quoteTable(ctx.Parent.Name), quoteIndex(op.Name))}, nil
}

// quoteIndex quotes an index name, as the ClickHouse renderer quotes every
// name it writes.
func quoteIndex(name string) string { return sqlident.Quote(platform.ClickHouse, name) }

// quoteTable quotes the model name of the table an operation alters.
func quoteTable(name string) string { return sqlident.TableReference(platform.ClickHouse, name) }
