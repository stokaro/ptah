package chsql

import (
	"fmt"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chdiff"
	"ptah.run/dialect/clickhouse/chresolve"
)

// ValidateTTLChange checks the same transition before planning and rendering.
// No unrelated storage change may be acknowledged by a TTL-only operation.
func ValidateTTLChange(change *chdiff.Table) error {
	if err := chdiff.Validate(change); err != nil {
		return err
	}
	after, err := change.After.Observed()
	if err != nil {
		return err
	}
	beforeWithoutTTL, afterWithoutTTL := *change.Before, *after
	beforeWithoutTTL.TTL, afterWithoutTTL.TTL = "", ""
	if !SameTable(&beforeWithoutTTL, &afterWithoutTTL) {
		return fmt.Errorf("%w: changing ClickHouse engine, keys, partitioning, sampling, or settings requires a separate storage plan", ptaherr.ErrUnsupportedFeature)
	}
	if !chresolve.IsMergeTree(after.Engine) {
		return fmt.Errorf("%w: table TTL changes require a MergeTree engine", ptaherr.ErrUnsupportedFeature)
	}
	if SameExpression(change.Before.TTL, after.TTL) {
		return fmt.Errorf("%w: ClickHouse TTL operands contain no change", schemaext.ErrInvalidValue)
	}
	return nil
}
