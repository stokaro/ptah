package ydbrender

import (
	"fmt"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/dialect/ydb/ydbscheme"
	"ptah.run/internal/sqlident"
	"ptah.run/internal/ydbttl"
)

// TTLHandler renders an in-place TTL change inside its ALTER TABLE.
func TTLHandler() renderer.ExtensionHandler {
	return renderer.TypedHandler(&ydbast.AlterTTL{}, ast.AlterExtension, validateTTL, renderTTL)
}

func validateTTL(ctx renderer.ExtensionContext, op *ydbast.AlterTTL) error {
	subject := ttlSubject(ctx)
	if err := requireTTL(ctx.Target, ctx.Capabilities, subject, op.Change.After); err != nil {
		return err
	}
	if err := ydbast.ValidateTTLChange(&op.Change); err != nil {
		return fmt.Errorf("%s: %w", subject, err)
	}
	return nil
}

// renderTTL writes `SET (TTL = ...)`, which puts a TTL on a table that has
// none and replaces the one a table has, or `RESET (TTL)`. The column's type is
// held to the TTL by the plan, which sees the table; this sees one payload.
func renderTTL(ctx renderer.ExtensionContext, op *ydbast.AlterTTL) ([]string, error) {
	prefix := "ALTER TABLE " + sqlident.Quote(platform.YDB, ydbscheme.ObjectPath(parentName(ctx))) + " "
	if op.Change.After == nil {
		return []string{prefix + "RESET (TTL);"}, nil
	}
	return []string{prefix + "SET (TTL = " + TTLSetting(op.Change.After.Policy) + ");"}, nil
}

// TTLSetting writes a valid TTL as the value of YQL's TTL setting, without the
// leading `TTL =`, as in `Interval("PT1H") ON `expires` AS SECONDS`.
func TTLSetting(policy ydbschema.TTL) string {
	return ydbttl.Setting(policy.Column, policy.Interval, policy.Unit, func(column string) string {
		return sqlident.Quote(platform.YDB, column)
	})
}

// CreateTableTTL returns the `TTL = ...` setting a CREATE TABLE carries for
// the table's TTL, and the empty string for a table without one. columnTypes
// maps each declared column to its YDB type: the TTL's column must be one of
// them, of a type YDB reads a TTL from (see ydbschema.TTLColumnRefusal). A target
// without the capability is refused rather than given a table without the
// setting, since the setting deletes rows.
func CreateTableTTL(target string, caps capability.Capabilities, table string, facets schemaext.Facets, columnTypes map[string]string) (string, error) {
	value, found, err := schemaext.FacetAs[*ydbschema.DesiredTTL](facets, ydbschema.TTLKind)
	if err != nil || !found {
		return "", err
	}
	subject := fmt.Sprintf("the TTL of table %q", table)
	if err := ydbschema.ValidateDesiredTTL(value); err != nil {
		return "", fmt.Errorf("%s: %w", subject, err)
	}
	if err := requireTTL(target, caps, subject, value); err != nil {
		return "", err
	}
	if reason := ydbschema.TTLColumnRefusal(value.Policy, columnTypes); reason != "" {
		return "", refuseTTLFact(subject, reason)
	}
	return "TTL = " + TTLSetting(value.Policy), nil
}

// ValidateTableFacets checks the table values a YDB CREATE TABLE consumes. An
// empty collection, including retained source exclusions, is valid. Unknown
// active kinds wrap ptaherr.ErrUnsupportedFeature; observations and malformed
// declarations wrap schemaext.ErrInvalidValue. Target selection happens before
// this call.
func ValidateTableFacets(facets schemaext.Facets) error {
	for _, kind := range facets.Kinds() {
		if kind != ydbschema.TTLKind {
			return fmt.Errorf("%w: YDB table facet %q is not supported", ptaherr.ErrUnsupportedFeature, kind)
		}
	}
	value, found, err := schemaext.FacetAs[*ydbschema.DesiredTTL](facets, ydbschema.TTLKind)
	if err != nil || !found {
		return err
	}
	return ydbschema.ValidateDesiredTTL(value)
}

// requireTTL refuses a TTL on a target other than YDB, a TTL on a target
// without capability.RowDeletionPolicy, and an integer column's unit on one
// without capability.RowDeletionPolicyEpochColumn. after is nil for a
// removal.
func requireTTL(target string, caps capability.Capabilities, subject string, after *ydbschema.DesiredTTL) error {
	if platform.NormalizeDialect(target) != platform.YDB {
		return &ptaherr.CapabilityError{Dialect: target, Feature: string(ydbschema.TTLKind), Err: ptaherr.ErrUnsupportedDialect,
			Message: fmt.Sprintf("a YDB TTL cannot be rendered for %q", target)}
	}
	if !caps.Has(capability.RowDeletionPolicy) {
		return refuseTTLKey(capability.RowDeletionPolicy, subject)
	}
	if after != nil && strings.TrimSpace(after.Policy.Unit) != "" && !caps.Has(capability.RowDeletionPolicyEpochColumn) {
		return refuseTTLKey(capability.RowDeletionPolicyEpochColumn, subject+" reads an integer column counting "+after.Policy.Unit)
	}
	return nil
}

func refuseTTLKey(key capability.Capability, subject string) error {
	return &ptaherr.CapabilityError{Dialect: platform.YDB, Feature: string(key), Err: ptaherr.ErrUnsupportedFeature,
		Message: fmt.Sprintf("%s, which requires target capability %s, unavailable on this %s target", subject, key, platform.YDB)}
}

func refuseTTLFact(subject, reason string) error {
	return &ptaherr.CapabilityError{Dialect: platform.YDB, Feature: subject, Err: ptaherr.ErrUnsupportedFeature,
		Message: fmt.Sprintf("%s: %s", subject, reason)}
}

func ttlSubject(ctx renderer.ExtensionContext) string {
	return fmt.Sprintf("the TTL of table %q", parentName(ctx))
}

func parentName(ctx renderer.ExtensionContext) string {
	if ctx.Parent == nil {
		return ""
	}
	return ctx.Parent.Name
}
