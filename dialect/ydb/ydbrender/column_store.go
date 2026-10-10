package ydbrender

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/dialect/ydb/ydbscheme"
	"ptah.run/internal/sqlident"
	"ptah.run/internal/ydbcolumn"
)

// ColumnStoreTTLHandler renders an in-place change of a column table's tiered
// TTL inside its ALTER TABLE.
func ColumnStoreTTLHandler() renderer.ExtensionHandler {
	return renderer.TypedHandler(&ydbast.AlterColumnStoreTTL{}, ast.AlterExtension, validateColumnStoreTTL, renderColumnStoreTTL)
}

func validateColumnStoreTTL(ctx renderer.ExtensionContext, op *ydbast.AlterColumnStoreTTL) error {
	subject := fmt.Sprintf("the tiered TTL of table %q", parentName(ctx))
	if platform.NormalizeDialect(ctx.Target) != platform.YDB {
		return &ptaherr.CapabilityError{Dialect: ctx.Target, Feature: string(ydbschema.ColumnStoreKind), Err: ptaherr.ErrUnsupportedDialect,
			Message: fmt.Sprintf("a YDB tiered TTL cannot be rendered for %q", ctx.Target)}
	}
	if !ctx.Capabilities.Has(capability.ColumnStoreTables) {
		return refuseTTLKey(capability.ColumnStoreTables, subject)
	}
	if op.Policy != nil && !ctx.Capabilities.Has(capability.TieredTTL) {
		return refuseTTLKey(capability.TieredTTL, subject)
	}
	if err := op.Validate(); err != nil {
		return fmt.Errorf("%s: %w", subject, err)
	}
	return nil
}

// renderColumnStoreTTL writes `SET (TTL = ...)`, which installs a policy and
// replaces the one a table holds, or `RESET (TTL)`.
func renderColumnStoreTTL(ctx renderer.ExtensionContext, op *ydbast.AlterColumnStoreTTL) ([]string, error) {
	prefix := "ALTER TABLE " + sqlident.Quote(platform.YDB, ydbscheme.ObjectPath(parentName(ctx))) + " "
	if op.Policy == nil {
		return []string{prefix + "RESET (TTL);"}, nil
	}
	return []string{prefix + "SET (TTL = " + TieredTTLSetting(op.Policy) + ");"}, nil
}

// TieredTTLSetting writes a valid tiered TTL as the value of YQL's TTL
// setting, without the leading `TTL =`, as in `Interval("PT86400S") TO
// EXTERNAL DATA SOURCE `/local/ext/bucket`, Interval("PT604800S") DELETE ON
// `ts“. Intervals are written in seconds.
func TieredTTLSetting(policy *ydbschema.TieredTTL) string {
	return ydbcolumn.TTLClause(policy, func(name string) string { return sqlident.Quote(platform.YDB, name) })
}
