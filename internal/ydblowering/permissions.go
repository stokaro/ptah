package ydblowering

import (
	"slices"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/ydbacl"
)

// YDBPermissionNamesFor returns database with each privilege of its grants and
// revoked grants written as the YDB permission it names, on a YDB target, and
// database itself on every other.
//
// A declaration may spell a permission as GRANT does, `SELECT ROW`, or by its
// name, `ydb.granular.select_row`, and YDB reports only the name: the access
// list of an object holds names. The comparison reads the declaration through
// this function so a grant spelled either way is the entry the reader reports,
// and a schema applied once plans nothing the second time. A file-to-file
// comparison reads its current side through it too. A privilege YDB has no
// permission for is left as written, so the refusal that reaches the author is
// the declaration gate's, which names it. database is not changed.
func YDBPermissionNamesFor(database *schemamodel.Database, dialect string) *schemamodel.Database {
	if database == nil || platform.NormalizeDialect(dialect) != platform.YDB {
		return database
	}
	lowered := *database
	lowered.Grants = ydbPermissionNames(database.Grants)
	lowered.RevokedGrants = ydbPermissionNames(database.RevokedGrants)
	return &lowered
}

// ydbPermissionNames returns grants with each privilege written as the YDB
// permission it names, canonicalized so two spellings of one permission are
// one privilege.
func ydbPermissionNames(grants []schemamodel.Grant) []schemamodel.Grant {
	if grants == nil {
		return nil
	}
	named := make([]schemamodel.Grant, 0, len(grants))
	for _, grant := range grants {
		grant.Privileges = slices.Clone(grant.Privileges)
		for i, privilege := range grant.Privileges {
			if permission, ok := ydbacl.Permission(privilege); ok {
				grant.Privileges[i] = permission
			}
		}
		grant.Canonicalize()
		named = append(named, grant)
	}
	return named
}
