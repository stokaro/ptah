package ydbast

import (
	"fmt"
	"unicode/utf8"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/internal/ydbpool"
)

// ResourcePoolKind identifies an operation on a database-scoped YDB pool.
const ResourcePoolKind schemaext.Kind = "ptah.run/ydb/resource-pool-operation"

// ResourcePoolClassifierKind identifies an operation on a YDB routing classifier.
const ResourcePoolClassifierKind schemaext.Kind = "ptah.run/ydb/resource-pool-classifier-operation"

// PoolOperation selects creation, alteration, or removal of a workload object.
type PoolOperation string

const (
	// PoolCreate creates a workload object.
	PoolCreate PoolOperation = "create"
	// PoolAlter changes a workload object using both captured operands.
	PoolAlter PoolOperation = "alter"
	// PoolDrop removes a workload object.
	PoolDrop PoolOperation = "drop"
)

// ResourcePool carries a standalone pool operation. Names are database-scoped,
// never paths. Create requires Spec, alter requires Spec and Previous, and drop
// carries neither. An empty settings object is distinct from a missing operand.
type ResourcePool struct {
	Operation PoolOperation         `json:"operation"`
	Name      string                `json:"name"`
	Spec      *ast.ResourcePoolSpec `json:"spec,omitempty"`
	Previous  *ast.ResourcePoolSpec `json:"previous,omitempty"`
}

// Kind returns the pool operation identity.
func (*ResourcePool) Kind() schemaext.Kind { return ResourcePoolKind }

// CloneExtension copies both operands and every optional setting.
func (v *ResourcePool) CloneExtension() ast.ExtensionPayload {
	if v == nil {
		return (*ResourcePool)(nil)
	}
	cloned := *v
	if v.Spec != nil {
		cloned.Spec = new(v.Spec.Clone())
	}
	if v.Previous != nil {
		cloned.Previous = new(v.Previous.Clone())
	}
	return &cloned
}

// Subject returns the database-scoped pool identity for reports and dependencies.
func (v *ResourcePool) Subject() objectidentity.ID {
	return objectidentity.NewBuilder(identifier.ForDialect("ydb")).SchemaScopedParts("ptah.run/ydb/resource-pool", "", v.Name)
}

// Validate checks intrinsic semantics without granting a target capability.
func (v *ResourcePool) Validate() error {
	if v == nil {
		return fmt.Errorf("%w: resource pool operation is nil", schemaext.ErrInvalidValue)
	}
	if err := poolOperands(v.Operation, v.Spec, v.Previous); err != nil {
		return err
	}
	// The capability is checked against the actual target by the renderer. Here
	// it only lets the shared validator inspect names and operand values.
	caps := capability.Capabilities{capability.ResourcePools: true}
	if v.Operation == PoolDrop {
		return poolInvalid(ydbpool.CheckPoolDrop(v.Name, caps))
	}
	if err := poolInvalid(ydbpool.CheckPool(v.Name, *v.Spec, caps)); err != nil {
		return err
	}
	if v.Previous != nil {
		return poolInvalid(ydbpool.CheckPool(v.Name, *v.Previous, caps))
	}
	return nil
}

// Effect describes workload consequences; an invalid operation has unknown effects.
func (v *ResourcePool) Effect() schemaext.Effect {
	if v.Validate() != nil {
		return schemaext.Effect{}
	}
	switch v.Operation {
	case PoolDrop:
		return schemaext.Effect{Impact: schemaext.Behavioral, Reason: "DROP RESOURCE POOL runs the queries a classifier sends to the pool in the pool default"}
	case PoolAlter:
		return schemaext.Effect{Impact: schemaext.Behavioral, Reason: "ALTER RESOURCE POOL changes the limits of running and queued queries"}
	default:
		return schemaext.Effect{Impact: schemaext.Additive, Reason: "does not remove data or tighten constraints"}
	}
}

// ResourcePoolClassifier carries routing settings and an explicit transition.
// Create requires Spec, alter requires both operands, and drop carries neither.
type ResourcePoolClassifier struct {
	Operation PoolOperation                   `json:"operation"`
	Name      string                          `json:"name"`
	Spec      *ast.ResourcePoolClassifierSpec `json:"spec,omitempty"`
	Previous  *ast.ResourcePoolClassifierSpec `json:"previous,omitempty"`
}

// Kind returns the classifier operation identity.
func (*ResourcePoolClassifier) Kind() schemaext.Kind { return ResourcePoolClassifierKind }

// CloneExtension returns independent operand pointers.
func (v *ResourcePoolClassifier) CloneExtension() ast.ExtensionPayload {
	if v == nil {
		return (*ResourcePoolClassifier)(nil)
	}
	cloned := *v
	if v.Spec != nil {
		cloned.Spec = new(*v.Spec)
	}
	if v.Previous != nil {
		cloned.Previous = new(*v.Previous)
	}
	return &cloned
}

// Subject returns the database-scoped classifier identity.
func (v *ResourcePoolClassifier) Subject() objectidentity.ID {
	return objectidentity.NewBuilder(identifier.ForDialect("ydb")).SchemaScopedParts("ptah.run/ydb/resource-pool-classifier", "", v.Name)
}

// Validate refuses incomplete transitions and settings YDB cannot represent.
func (v *ResourcePoolClassifier) Validate() error {
	if v == nil {
		return fmt.Errorf("%w: resource pool classifier operation is nil", schemaext.ErrInvalidValue)
	}
	if err := poolOperands(v.Operation, v.Spec, v.Previous); err != nil {
		return err
	}
	caps := capability.Capabilities{capability.ResourcePools: true}
	if v.Operation == PoolDrop {
		return poolInvalid(ydbpool.CheckClassifier(v.Name, ast.ResourcePoolClassifierSpec{ResourcePool: ydbpool.DefaultPool}, caps))
	}
	for _, spec := range []*ast.ResourcePoolClassifierSpec{v.Spec, v.Previous} {
		if spec == nil {
			continue
		}
		if !utf8.ValidString(spec.ResourcePool) || !utf8.ValidString(spec.MemberName) {
			return fmt.Errorf("%w: classifier settings contain invalid UTF-8", schemaext.ErrInvalidValue)
		}
		if err := poolInvalid(ydbpool.CheckClassifier(v.Name, *spec, caps)); err != nil {
			return err
		}
	}
	return nil
}

// Effect records routing changes, including a new classifier matching existing users.
func (v *ResourcePoolClassifier) Effect() schemaext.Effect {
	if v.Validate() != nil {
		return schemaext.Effect{}
	}
	if v.Operation == PoolDrop {
		return schemaext.Effect{Impact: schemaext.Behavioral, Reason: "DROP RESOURCE POOL CLASSIFIER sends its member's queries to another classifier's pool or to the pool default"}
	}
	return schemaext.Effect{Impact: schemaext.Behavioral, Reason: "resource pool classifier settings change which pool receives matching queries"}
}

func poolOperands[T any](operation PoolOperation, spec, previous *T) error {
	switch operation {
	case PoolCreate:
		if spec != nil && previous == nil {
			return nil
		}
	case PoolAlter:
		if spec != nil && previous != nil {
			return nil
		}
	case PoolDrop:
		if spec == nil && previous == nil {
			return nil
		}
	default:
		return fmt.Errorf("%w: unknown workload operation %q", schemaext.ErrInvalidValue, operation)
	}
	return fmt.Errorf("%w: workload operation %q has incomplete or irrelevant operands", schemaext.ErrInvalidValue, operation)
}

func poolInvalid(refusal *ydbpool.Refusal) error {
	if refusal == nil {
		return nil
	}
	return fmt.Errorf("%w: %s: %s", schemaext.ErrInvalidValue, refusal.Subject, refusal.Reason)
}
