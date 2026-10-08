// Package chast defines owned ClickHouse operations for common AST extension
// envelopes. Its versioned payloads carry data without rendering or I/O.
package chast

import (
	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chdiff"
)

// AlterTTLKind identifies an in-place change to a table's TTL rules.
const AlterTTLKind schemaext.Kind = "ptah.run/clickhouse/alter-ttl"

// AlterTTL retains the complete transition so validation cannot silently omit
// another storage change. An explicit empty after TTL removes the rules.
// A nonempty rule containing only whitespace is invalid on either operand.
type AlterTTL struct {
	Change chdiff.Table `json:"change"`
}

// Kind returns the stable operation identity.
func (*AlterTTL) Kind() schemaext.Kind { return AlterTTLKind }

// CloneExtension returns independent operands; a nil receiver remains typed nil.
func (v *AlterTTL) CloneExtension() ast.ExtensionPayload {
	if v == nil {
		return (*AlterTTL)(nil)
	}
	return &AlterTTL{Change: *v.Change.CloneChange().(*chdiff.Table)}
}

// Effect reports possible asynchronous changes to retained rows and values.
// Removing a rule cannot restore data already expired by the previous rule.
func (*AlterTTL) Effect() schemaext.Effect {
	return schemaext.Effect{Impact: schemaext.Behavioral, Reason: "TTL changes can expire, move, or aggregate existing data; restoring the rule cannot recover that data"}
}
