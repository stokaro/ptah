package ydbast

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
)

// AlterTTLKind identifies an in-place change to a table's TTL.
const AlterTTLKind schemaext.Kind = "ptah.run/ydb/alter-ttl"

// AlterTTL retains the complete transition, so the statement it lowers to is a
// function of the two states alone: `RESET (TTL)` for a removal, and `SET (TTL
// = ...)` otherwise, which puts a TTL on a table that has none and replaces the
// one it has.
type AlterTTL struct {
	Change ydbdiff.TTL `json:"change"`
}

// Kind returns the stable operation identity.
func (*AlterTTL) Kind() schemaext.Kind { return AlterTTLKind }

// CloneExtension returns independent operands; a nil receiver remains typed nil.
func (v *AlterTTL) CloneExtension() ast.ExtensionPayload {
	if v == nil {
		return (*AlterTTL)(nil)
	}
	return &AlterTTL{Change: *v.Change.CloneChange().(*ydbdiff.TTL)}
}

// Effect reports that the TTL decides which rows YDB deletes. Restoring a
// prior TTL cannot recover rows already deleted.
func (*AlterTTL) Effect() schemaext.Effect {
	return schemaext.Effect{Impact: schemaext.Behavioral, Reason: "a TTL decides which rows YDB deletes; restoring a prior TTL cannot recover deleted rows"}
}

// ValidateTTLChange requires a valid change whose operands differ: a TTL
// replaced by one with the same column, unit and seconds is not a change. The
// column is compared exactly, which is the conservative reading.
func ValidateTTLChange(change *ydbdiff.TTL) error {
	if err := ydbdiff.ValidateTTL(change); err != nil {
		return err
	}
	if change.Before != nil && change.After != nil &&
		ydbschema.EquivalentTTL(change.After.Policy, change.Before.Policy, func(s string) string { return s }) {
		return fmt.Errorf("%w: YDB TTL operands contain no change", schemaext.ErrInvalidValue)
	}
	return nil
}
