package ydbworkload

import (
	"fmt"
	"unicode/utf8"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

// ValidatePool checks settings independently of a name or target capability.
// An unset limit is nil. Zero remains a configured limit, including a queue
// of zero or a pool that cannot run any queries.
func ValidatePool(spec PoolSpec) error {
	if reason := poolValueRefusal(spec); reason != "" {
		return fmt.Errorf("%w: %s", schemaext.ErrInvalidValue, reason)
	}
	if reason := poolShapeRefusal(spec); reason != "" {
		return fmt.Errorf("%w: %s", schemaext.ErrInvalidValue, reason)
	}
	return nil
}

// ValidateClassifier checks stored routing settings. The destination pool and
// member need not exist in the inspected database. Declaration-wide routing
// validation must check references against captured state separately.
func ValidateClassifier(spec ClassifierSpec) error {
	if spec.ResourcePool == "" || spec.Rank < 0 || !utf8.ValidString(spec.ResourcePool) || !utf8.ValidString(spec.MemberName) {
		return fmt.Errorf("%w: a classifier requires a pool, a nonnegative rank, and valid UTF-8 settings", schemaext.ErrInvalidValue)
	}
	return nil
}

// ValidatePoolRef checks exact database scope and protects default pool limits.
// It grants no permission to create, drop, or use a workload object on a target.
func ValidatePoolRef(ref objectidentity.ID, spec PoolSpec) error {
	if err := ValidateIdentity(ref, PoolKind); err != nil {
		return err
	}
	if err := ValidatePool(spec); err != nil {
		return err
	}
	if ref.Name.Source == DefaultPool && (spec.ConcurrentQueryLimit != nil || spec.DatabaseLoadCPUThreshold != nil || spec.QueueSize != nil) {
		return fmt.Errorf("%w: the default pool cannot limit concurrent queries, queue queries, or set a database load threshold", schemaext.ErrInvalidValue)
	}
	return nil
}
