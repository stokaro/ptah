package ydbdiff

import (
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
)

// TTLKind identifies a change to a surviving table's TTL.
const TTLKind schemaext.Kind = "ptah.run/ydb/ttl-change"

// TTL retains the observed TTL and the requested one. A nil Before is a table
// the read covered and found without a TTL; a nil After requests that the TTL
// be removed. Both nil is not a change. Table creation and removal carry the
// TTL with the table and never produce this change.
type TTL struct {
	Before *ydbschema.ObservedTTL `json:"before,omitzero"`
	After  *ydbschema.DesiredTTL  `json:"after,omitzero"`
}

// Kind returns the owned change identity.
func (*TTL) Kind() schemaext.Kind { return TTLKind }

// CloneChange returns independent operands. A nil receiver remains typed nil.
func (v *TTL) CloneChange() schemaext.ChangeValue {
	if v == nil {
		return (*TTL)(nil)
	}
	result := &TTL{}
	if v.Before != nil {
		before := *v.Before
		result.Before = &before
	}
	if v.After != nil {
		after := *v.After
		result.After = &after
	}
	return result
}

// ValidateTTL requires at least one valid operand. Invalid operands wrap
// schemaext.ErrInvalidValue. It does not decide whether the operands differ on
// the server; the comparison owner produced the change because they do.
func ValidateTTL(v *TTL) error {
	if v == nil || (v.Before == nil && v.After == nil) {
		return fmt.Errorf("%w: a YDB TTL change requires an operand", schemaext.ErrInvalidValue)
	}
	if v.Before != nil {
		if err := ydbschema.ValidateObservedTTL(v.Before); err != nil {
			return err
		}
	}
	if v.After != nil {
		return ydbschema.ValidateDesiredTTL(v.After)
	}
	return nil
}
