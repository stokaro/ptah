package ydbdiff

import (
	"fmt"
	"unicode/utf8"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbworkload"
)

// ResourcePoolClassifierKind identifies a captured transition of one database-wide resource pool classifier.
const ResourcePoolClassifierKind schemaext.Kind = "ptah.run/ydb/resource-pool-classifier-change"

// ResourcePoolClassifier preserves complete settings on both sides. A nil operand means
// established absence. Removal is an explicit operation or rollback; comparison
// never removes database-wide objects merely omitted by an application's source.
type ResourcePoolClassifier struct {
	Before *ydbworkload.ObservedClassifier `json:"before"`
	After  *ydbworkload.DesiredClassifier  `json:"after"`
}

// Kind returns the captured change identity.
func (*ResourcePoolClassifier) Kind() schemaext.Kind { return ResourcePoolClassifierKind }

// CloneChange returns independent operands, including optional limits.
func (v *ResourcePoolClassifier) CloneChange() schemaext.ChangeValue {
	if v == nil {
		return (*ResourcePoolClassifier)(nil)
	}
	cloned := &ResourcePoolClassifier{}
	if v.Before != nil {
		cloned.Before = &ydbworkload.ObservedClassifier{Spec: v.Before.Spec}
	}
	if v.After != nil {
		cloned.After = new(*v.After)
	}
	return cloned
}

// Validate refuses missing operands and invalid captured settings. Object names
// and target capabilities are checked by contextual comparison and planning.
func (v *ResourcePoolClassifier) Validate() error {
	if v == nil || (v.Before == nil && v.After == nil) {
		return fmt.Errorf("%w: resource pool classifier change requires a before or after definition", schemaext.ErrInvalidValue)
	}
	if v.Before != nil {
		if err := ydbworkload.ValidateClassifier(v.Before.Spec); err != nil {
			return err
		}
	}
	if v.After != nil {
		if !utf8.ValidString(v.After.StructName) {
			return fmt.Errorf("%w: workload source holder must be valid UTF-8", schemaext.ErrInvalidValue)
		}
		if err := ydbworkload.ValidateClassifier(v.After.Spec); err != nil {
			return err
		}
	}
	return nil
}

// ResourcePoolClassifierCodec describes explicit before/after operands without inferring absence.
func ResourcePoolClassifierCodec() schemaext.Codec {
	return workloadChangeCodec(&ResourcePoolClassifier{}, "classifier", ydbworkload.ClassifierCodecs(),
		func(before *ydbworkload.ObservedClassifier, after *ydbworkload.DesiredClassifier) *ResourcePoolClassifier {
			return &ResourcePoolClassifier{Before: before, After: after}
		})
}

// Effect describes workload consequences without claiming data loss.
func (v *ResourcePoolClassifier) Effect() schemaext.Effect {
	if v.Validate() != nil {
		return schemaext.Effect{}
	}
	return schemaext.Effect{Impact: schemaext.Behavioral, Reason: "changes which pool receives matching queries"}
}
