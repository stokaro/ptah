package ydbdiff

import (
	"fmt"
	"unicode/utf8"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbworkload"
)

// ResourcePoolKind identifies a captured transition of one database-wide resource pool.
const ResourcePoolKind schemaext.Kind = "ptah.run/ydb/resource-pool-change"

// ResourcePool preserves complete settings on both sides. A nil operand means
// established absence. Removal is an explicit operation or rollback; comparison
// never removes database-wide objects merely omitted by an application's source.
type ResourcePool struct {
	Before *ydbworkload.ObservedPool `json:"before"`
	After  *ydbworkload.DesiredPool  `json:"after"`
}

// Kind returns the captured change identity.
func (*ResourcePool) Kind() schemaext.Kind { return ResourcePoolKind }

// CloneChange returns independent operands, including optional limits.
func (v *ResourcePool) CloneChange() schemaext.ChangeValue {
	if v == nil {
		return (*ResourcePool)(nil)
	}
	cloned := &ResourcePool{}
	if v.Before != nil {
		cloned.Before = &ydbworkload.ObservedPool{Spec: v.Before.Spec.Clone()}
	}
	if v.After != nil {
		cloned.After = new(*v.After)
		cloned.After.Spec = v.After.Spec.Clone()
	}
	return cloned
}

// Validate refuses missing operands and invalid captured settings. Object names
// and target capabilities are checked by contextual comparison and planning.
func (v *ResourcePool) Validate() error {
	if v == nil || (v.Before == nil && v.After == nil) {
		return fmt.Errorf("%w: resource pool change requires a before or after definition", schemaext.ErrInvalidValue)
	}
	if v.Before != nil {
		if err := ydbworkload.ValidatePool(v.Before.Spec); err != nil {
			return err
		}
	}
	if v.After != nil {
		if !utf8.ValidString(v.After.StructName) {
			return fmt.Errorf("%w: workload source holder must be valid UTF-8", schemaext.ErrInvalidValue)
		}
		if err := ydbworkload.ValidatePool(v.After.Spec); err != nil {
			return err
		}
	}
	return nil
}

// ResourcePoolCodec describes explicit before/after operands without inferring absence.
func ResourcePoolCodec() schemaext.Codec {
	return workloadChangeCodec(&ResourcePool{}, "pool", ydbworkload.PoolCodecs(),
		func(before *ydbworkload.ObservedPool, after *ydbworkload.DesiredPool) *ResourcePool {
			return &ResourcePool{Before: before, After: after}
		})
}

// Effect describes workload consequences without claiming data loss.
func (v *ResourcePool) Effect() schemaext.Effect {
	if v.Validate() != nil {
		return schemaext.Effect{}
	}
	if v.Before == nil {
		return schemaext.Effect{Impact: schemaext.Additive, Reason: "creates a resource pool"}
	}
	return schemaext.Effect{Impact: schemaext.Behavioral, Reason: "changes workload limits or fallback routing"}
}
