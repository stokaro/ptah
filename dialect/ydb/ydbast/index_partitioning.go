package ydbast

import (
	"encoding/json"
	"fmt"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/ydbindex"
	"ptah.run/internal/ydbpartition"
)

// AlterIndexPartitioningKind identifies an in-place change of a global
// index's partitioning.
const AlterIndexPartitioningKind schemaext.Kind = "ptah.run/ydb/alter-index-partitioning"

// AlterIndexPartitioning changes the settings of the index named Index on
// the table the operation alters: one `ALTER TABLE t ALTER INDEX i SET (...)`
// naming every splitting setting whenever it changes one, since setting one
// resets others (see [ydbpartition.Clause]).
type AlterIndexPartitioning struct {
	Index  string                    `json:"index"`
	Change ydbdiff.IndexPartitioning `json:"change"`
}

// Kind returns the stable operation identity.
func (*AlterIndexPartitioning) Kind() schemaext.Kind { return AlterIndexPartitioningKind }

// CloneExtension returns independent operands; a nil receiver remains typed
// nil.
func (v *AlterIndexPartitioning) CloneExtension() ast.ExtensionPayload { return v.Copy() }

// Copy is [AlterIndexPartitioning.CloneExtension] without the interface. A
// nil receiver returns nil.
func (v *AlterIndexPartitioning) Copy() *AlterIndexPartitioning {
	if v == nil {
		return nil
	}
	return &AlterIndexPartitioning{Index: v.Index, Change: *v.Change.Copy()}
}

// Effect reports that the settings rewrite no value.
func (*AlterIndexPartitioning) Effect() schemaext.Effect {
	return schemaext.Effect{Impact: schemaext.Additive,
		Reason: "an index's partitioning and read replicas decide how YDB stores and serves it; changing them rewrites no value"}
}

// Resolve reads both sides: what the index holds, and the declaration over
// it. A side YDB could not hold is an error saying why.
func (v *AlterIndexPartitioning) Resolve() (held, desired ydbpartition.Settings, _ error) {
	var before *ydbschema.IndexPartitioning
	if v.Change.Before != nil {
		before = &v.Change.Before.IndexPartitioning
	}
	held, err := ydbindex.Held(before)
	if err != nil {
		return held, desired, fmt.Errorf("the settings it holds: %w", err)
	}
	desired, err = ydbindex.Resolve(&v.Change.After.IndexPartitioning, held)
	return held, desired, err
}

// Settings are the settings of the SET the operation lowers to.
func (v *AlterIndexPartitioning) Settings() ([]string, error) {
	held, desired, err := v.Resolve()
	if err != nil {
		return nil, err
	}
	return ydbpartition.Clause(desired, held), nil
}

// Validate requires an index name and a valid change that writes at least
// one setting.
func (v *AlterIndexPartitioning) Validate() error {
	if v == nil {
		return fmt.Errorf("%w: YDB index partitioning operation is nil", schemaext.ErrInvalidValue)
	}
	if strings.TrimSpace(v.Index) == "" {
		return fmt.Errorf("%w: a YDB index partitioning operation names no index", schemaext.ErrInvalidValue)
	}
	if err := ydbdiff.ValidateIndexPartitioning(&v.Change); err != nil {
		return err
	}
	settings, err := v.Settings()
	if err != nil {
		return fmt.Errorf("%w: %w", schemaext.ErrInvalidValue, err)
	}
	if len(settings) == 0 {
		return fmt.Errorf("%w: YDB index partitioning operands contain no change", schemaext.ErrInvalidValue)
	}
	return nil
}

var indexPartitioningShape = schemaext.ObjectShape{Name: "YDB index partitioning operation",
	Allowed: []string{"index", "change"}, Required: []string{"index", "change"}}

// IndexPartitioningCodec returns the version-one operation codec: the index
// name and the owner's change.
func IndexPartitioningCodec() schemaext.Codec {
	change := ydbdiff.IndexPartitioningCodec()
	return schemaext.ModelCodec[*AlterIndexPartitioning]{
		Prototype: &AlterIndexPartitioning{}, Representation: schemaext.Operation, Version: 1,
		Definition: json.RawMessage(fmt.Sprintf(`{"index":"the index name","change":%s}`, change.Definition)),
		Shape: func(data json.RawMessage) error {
			fields, err := schemaext.DecodeObject(data, indexPartitioningShape)
			if err != nil {
				return err
			}
			_, err = change.Decode(fields["change"])
			return err
		},
		Validate: (*AlterIndexPartitioning).Validate,
		Clone:    (*AlterIndexPartitioning).Copy,
	}.Codec()
}
