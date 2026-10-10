package ydbast

import (
	"encoding/json"
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/ydbpartition"
)

// AlterTablePartitioningKind identifies an in-place change of a row table's
// settings.
const AlterTablePartitioningKind schemaext.Kind = "ptah.run/ydb/alter-table-partitioning"

// AlterTablePartitioning retains what the table holds and what the
// declaration states, so the statement it lowers to is a function of the two
// alone: one `ALTER TABLE ... SET (...)` naming every setting of each group
// the change touches, since setting one resets others (see
// [ydbpartition.TableClause]).
type AlterTablePartitioning struct {
	Change ydbdiff.TablePartitioning `json:"change"`
}

// Kind returns the stable operation identity.
func (*AlterTablePartitioning) Kind() schemaext.Kind { return AlterTablePartitioningKind }

// CloneExtension returns independent operands; a nil receiver remains typed
// nil.
func (v *AlterTablePartitioning) CloneExtension() ast.ExtensionPayload { return v.Copy() }

// Copy is [AlterTablePartitioning.CloneExtension] without the interface: it
// shares no operand with v, and a nil receiver returns nil.
func (v *AlterTablePartitioning) Copy() *AlterTablePartitioning {
	if v == nil {
		return nil
	}
	return &AlterTablePartitioning{Change: *v.Change.Copy()}
}

// Effect reports that the settings rewrite no value: they change how YDB
// splits, replicates and filters the table's rows.
func (*AlterTablePartitioning) Effect() schemaext.Effect {
	return schemaext.Effect{Impact: schemaext.Additive,
		Reason: "a table's partitioning, read replicas and key bloom filter decide how YDB stores and serves its rows; changing them rewrites no value"}
}

// Settings are the settings of the SET the operation lowers to; see
// [ydbpartition.TableClause]. A side YDB could not hold is an error saying
// why.
func (v *AlterTablePartitioning) Settings() ([]string, error) {
	held, desired, err := v.Resolve()
	if err != nil {
		return nil, err
	}
	return ydbpartition.TableClause(desired, held), nil
}

// Resolve reads both sides: what the table holds, and the declaration over
// it. A side YDB could not hold is an error saying why.
func (v *AlterTablePartitioning) Resolve() (held, desired ydbpartition.TableSettings, _ error) {
	var before *ydbschema.TablePartitioning
	if v.Change.Before != nil {
		before = &v.Change.Before.TablePartitioning
	}
	held, err := ydbpartition.HeldTable(before)
	if err != nil {
		return held, desired, fmt.Errorf("the settings it holds: %w", err)
	}
	desired, err = ydbpartition.ResolveTable(&v.Change.After.TablePartitioning, held)
	return held, desired, err
}

// Validate requires a valid change that writes at least one setting.
func (v *AlterTablePartitioning) Validate() error {
	if v == nil {
		return fmt.Errorf("%w: YDB table partitioning operation is nil", schemaext.ErrInvalidValue)
	}
	if err := ydbdiff.ValidateTablePartitioning(&v.Change); err != nil {
		return err
	}
	settings, err := v.Settings()
	if err != nil {
		return fmt.Errorf("%w: %w", schemaext.ErrInvalidValue, err)
	}
	if len(settings) == 0 {
		return fmt.Errorf("%w: YDB table partitioning operands contain no change", schemaext.ErrInvalidValue)
	}
	return nil
}

// tablePartitioningShape is the operation object's one key.
var tablePartitioningShape = schemaext.ObjectShape{Name: "YDB table partitioning operation", Allowed: []string{"change"}, Required: []string{"change"}}

// TablePartitioningCodec returns the version-one operation codec. The wire
// shape is the owner's change, checked by the change codec, never a
// serialized common Go AST. A transition that writes nothing, and one YDB
// could not hold, is refused. Every refusal is a
// [schemaext.InvalidModelError].
func TablePartitioningCodec() schemaext.Codec {
	change := ydbdiff.TablePartitioningCodec()
	return schemaext.ModelCodec[*AlterTablePartitioning]{
		Prototype: &AlterTablePartitioning{}, Representation: schemaext.Operation, Version: 1,
		Definition: json.RawMessage(fmt.Sprintf(`{"change":%s}`, change.Definition)),
		Shape:      changeEnvelope(tablePartitioningShape, change),
		Validate:   (*AlterTablePartitioning).Validate,
		Clone:      (*AlterTablePartitioning).Copy,
	}.Codec()
}

// changeEnvelope checks an operation object holding one change under
// "change", decoded through the change codec.
func changeEnvelope(shape schemaext.ObjectShape, change schemaext.Codec) func(json.RawMessage) error {
	return func(data json.RawMessage) error {
		fields, err := schemaext.DecodeObject(data, shape)
		if err != nil {
			return err
		}
		_, err = change.Decode(fields["change"])
		return err
	}
}
