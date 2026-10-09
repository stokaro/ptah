package ydbworkload

import (
	"fmt"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
)

// PoolKind identifies one database-wide YDB resource pool.
const PoolKind schemaext.Kind = "ptah.run/ydb/resource-pool"

// ClassifierKind identifies one database-wide YDB resource pool classifier.
const ClassifierKind schemaext.Kind = "ptah.run/ydb/resource-pool-classifier"

// PoolRef preserves the exact pool name without assigning a scheme directory.
func PoolRef(name string) objectidentity.ID {
	return workloadRef(PoolKind, name)
}

// ClassifierRef preserves the exact classifier name without a table or directory.
func ClassifierRef(name string) objectidentity.ID {
	return workloadRef(ClassifierKind, name)
}

func workloadRef(kind schemaext.Kind, name string) objectidentity.ID {
	return objectidentity.NewBuilder(identifier.ForDialect("ydb")).SchemaScopedParts(objectidentity.Kind(kind), "", name)
}

// ValidateIdentity refuses foreign kinds, directory/table scope, and invalid
// workload names. Unlike scheme objects, workload objects have no path parts.
func ValidateIdentity(ref objectidentity.ID, kind schemaext.Kind) error {
	if (kind != PoolKind && kind != ClassifierKind) || ref.Name.Source == "" || ref != workloadRef(kind, ref.Name.Source) {
		return fmt.Errorf("%w: workload objects require exact database-scoped YDB identities", schemaext.ErrInvalidValue)
	}
	for _, r := range ref.Name.Source {
		if r == '/' || r <= ' ' || r > '~' {
			return fmt.Errorf("%w: workload names require printable ASCII other than a slash or space", schemaext.ErrInvalidValue)
		}
	}
	return nil
}
