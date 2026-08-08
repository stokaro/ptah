package compare

import (
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/indexscope"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

// generatedIndexIdentities is the identity of every index the desired state
// declares, in the same key space the database index pool uses. It exists so
// constraint comparison can ask the one question index comparison already
// answers -- "does the desired state name this object as an index?" -- without
// re-deriving index table resolution or identifier folding.
func generatedIndexIdentities(
	generated *goschema.Database,
	semantics identifier.Semantics,
) map[difftypes.IndexRef]struct{} {
	declared, _ := collectGeneratedIndexes(generated, semantics)
	identities := make(map[difftypes.IndexRef]struct{}, len(declared))
	for identity := range declared {
		identities[identity] = struct{}{}
	}
	return identities
}

// uniqueConstraintOwnedByDeclaredIndex reports whether a database UNIQUE
// constraint names the same object as an index the desired state declares, and
// so must be left to index comparison instead of compared again here.
//
// Every engine Ptah supports except SQL Server enforces a UNIQUE constraint
// with an index of the constraint's own name on the constraint's own table, and
// introspection reports that one object twice: once in the index catalog, once
// in the constraint catalog. On MySQL and MariaDB there is not even a separate
// notion to report --
//
//	ALTER TABLE users ADD CONSTRAINT uq_users_email UNIQUE (email)
//	CREATE UNIQUE INDEX uq_users_email ON users (email)
//
// leave the identical catalog row, which is why `schema inspect` writes MySQL
// uniqueness back out as `index { unique = true }` and never as a constraint,
// on both `ptah-compat` and the pinned community binary v1.3.0.
//
// **Which representation wins is decided by the desired state, not by the
// dialect.** When the desired state names the object as an index, the index
// pool owns it and this pool stays out; when the desired state names it as a
// constraint, or does not name it at all, nothing changes and this pool keeps
// it. That is what makes the two sides agree without either one guessing:
// the side that has an opinion is the side that wrote the schema.
//
// Compared here, the object was reported removed while index comparison
// reported the very same name added, because the desired state written as
// `index { unique = true }` produces no [goschema.Constraint] to match it. The
// resulting plan cannot be applied: measured on MySQL 9.7.1 and MariaDB
// 11.8.8, the CREATE comes back as
// `Error 1061 (42000): Duplicate key name 'uq_users_email'` and the apply exits
// 1; measured on PostgreSQL 17.10, where the statements carry IF NOT EXISTS and
// IF EXISTS, the create was skipped, the drop ran, the apply exited 0 and the
// table was left with no unique index at all. The pinned community binary
// v1.3.0 reported "Schema is synced" for every one of those fixtures
// (stokaro/ptah#1245).
//
// Uniqueness of the declared index is deliberately *not* required for the
// hand-off. A desired plain `index "uq_users_email"` against a database
// `UNIQUE KEY uq_users_email` is a real change, and it is one index comparison
// states correctly as a replacement of a single object; splitting it across the
// two pools would plan a DROP CONSTRAINT and a DROP INDEX for the same row.
//
// SQL Server is excluded because a UNIQUE constraint and a unique index are
// separate objects there, which is the same reason it is excluded from
// [constraintBackedIndexIdentities].
func uniqueConstraintOwnedByDeclaredIndex(
	constraint types.DBConstraint,
	dialect string,
	declaredIndexes map[difftypes.IndexRef]struct{},
	semantics identifier.Semantics,
) bool {
	if constraint.Type != "UNIQUE" ||
		platform.NormalizeDialect(dialect) == platform.SQLServer {
		return false
	}
	identity := indexscope.IdentityKeyWithSemantics(semantics, difftypes.IndexRef{
		Name:      constraint.Name,
		TableName: constraint.QualifiedTableName(),
	})
	_, declared := declaredIndexes[identity]
	return declared
}
