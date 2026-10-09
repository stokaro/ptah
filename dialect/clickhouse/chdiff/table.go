// Package chdiff owns captured directional changes to ClickHouse storage settings.
package chdiff

import (
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chschema"
)

// TableKind identifies a change to a surviving table's storage settings.
const TableKind schemaext.Kind = "ptah.run/clickhouse/table-change"

// Table retains the complete prior observation and resolved desired state.
// Neither operand may be nil: table creation and removal belong to the common
// table lifecycle. After contains explicit settings, including empty clauses.
type Table struct {
	Before *chschema.ObservedTable `json:"before"`
	After  *chschema.DesiredTable  `json:"after"`
}

// Kind returns the owned change identity.
func (*Table) Kind() schemaext.Kind { return TableKind }

// CloneChange returns independent operands. A nil receiver remains typed nil.
func (v *Table) CloneChange() schemaext.ChangeValue {
	if v == nil {
		return (*Table)(nil)
	}
	result := &Table{}
	if v.Before != nil {
		result.Before = new(*v.Before)
	}
	if v.After != nil {
		result.After = new(*v.After)
	}
	return result
}

// Validate requires complete before and resolved after state. Invalid operands
// wrap schemaext.ErrInvalidValue; it performs no SQL equivalence or capability
// check and does not assert that an ALTER statement exists for this change.
func Validate(v *Table) error {
	if v == nil || v.Before == nil || v.After == nil {
		return fmt.Errorf("%w: ClickHouse table change requires both operands", schemaext.ErrInvalidValue)
	}
	if err := chschema.ValidateObserved(v.Before); err != nil {
		return err
	}
	_, err := v.After.Observed()
	return err
}
