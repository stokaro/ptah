package chdiff

import (
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chschema"
)

// IndexKind identifies a change to a surviving data-skipping index's settings.
const IndexKind schemaext.Kind = "ptah.run/clickhouse/index-change"

// Index captures complete prior settings and resolved desired settings. Index
// identity, key expressions, and creation/removal belong to the common index
// lifecycle. Both operands are required; absence is not an unknown setting.
type Index struct {
	Before *chschema.ObservedIndex `json:"before"`
	After  *chschema.DesiredIndex  `json:"after"`
}

// Kind returns the owned change identity.
func (*Index) Kind() schemaext.Kind { return IndexKind }

// Effect reports the replacement the change requires. ClickHouse cannot change
// a skipping index's type or granularity in place, and a replacement discards
// the index data built for existing parts until MATERIALIZE INDEX rebuilds it.
func (*Index) Effect() schemaext.Effect {
	return schemaext.Effect{Impact: schemaext.Behavioral, Reason: "changing a skipping index's type or granularity replaces the index and discards the index data built for existing parts"}
}

// CloneChange returns independent operands. A nil receiver remains typed nil.
func (v *Index) CloneChange() schemaext.ChangeValue {
	if v == nil {
		return (*Index)(nil)
	}
	result := &Index{}
	if v.Before != nil {
		result.Before = new(*v.Before)
	}
	if v.After != nil {
		result.After = new(*v.After)
	}
	return result
}

// ValidateIndex requires complete before and resolved after settings. Invalid
// operands wrap schemaext.ErrInvalidValue. This validates the model, not server
// support or whether the operands have different SQL semantics.
func ValidateIndex(v *Index) error {
	if v == nil || v.Before == nil || v.After == nil {
		return fmt.Errorf("%w: ClickHouse index change requires both operands", schemaext.ErrInvalidValue)
	}
	if err := chschema.ValidateObservedIndex(v.Before); err != nil {
		return err
	}
	_, err := v.After.Observed()
	return err
}
