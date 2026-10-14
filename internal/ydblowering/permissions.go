package ydblowering

import (
	"slices"
	"strings"

	"ptah.run/catalog"
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

// AdoptObjectGrants returns desired with each permission entry current holds
// on a scheme object an owner manages, such as a topic, that desired does not
// declare, so a comparison leaves the entry as the database holds it. desired
// is not modified.
//
// Ptah manages the permissions of the scheme objects a declaration declares.
// A declaration that does not declare an object makes no claim about who may
// use it: a document whose format cannot name the object, such as an HCL one
// for a topic, an object a source leaves unmanaged, and one the plan drops,
// whose entries go with it. Without this, applying an inspected HCL document
// back would revoke every entry its roles hold on a topic, since the document
// declares the roles and cannot name the grants. An entry the declaration
// revokes, and one YDB gives an object it creates, are not adopted.
func AdoptObjectGrants(desired *schemamodel.Database, current *catalog.Database) *schemamodel.Database {
	if desired == nil || current == nil {
		return desired
	}
	var adopted []schemamodel.Grant
	for _, grant := range current.Grants {
		if grant.Implicit || grant.IsPartialRevoke {
			continue
		}
		ref, owned := ydbacl.ObjectRef(grant.ObjectType, grant.QualifiedTarget(), "")
		if !owned {
			continue
		}
		if _, declared, err := desired.FeatureObjects.Get(ref); err != nil || declared {
			continue
		}
		entry := schemamodel.Grant{Role: grant.Role, Privileges: []string{grant.Privilege},
			OnObject: grant.QualifiedTarget(), ObjectKind: grant.ObjectType}
		entry.Canonicalize()
		if revokes(desired.RevokedGrants, entry) {
			continue
		}
		adopted = append(adopted, entry)
	}
	if len(adopted) == 0 {
		return desired
	}
	lowered := *desired
	lowered.Grants = slices.Concat(desired.Grants, adopted)
	return &lowered
}

// revokes reports whether revoked takes entry's one privilege away from its
// role on its object.
func revokes(revoked []schemamodel.Grant, entry schemamodel.Grant) bool {
	return slices.ContainsFunc(revoked, func(grant schemamodel.Grant) bool {
		grant.Canonicalize()
		return grant.Role == entry.Role && grant.TargetKey() == entry.TargetKey() &&
			slices.ContainsFunc(grant.Privileges, func(privilege string) bool {
				name, ok := ydbacl.Permission(privilege)
				return ok && strings.EqualFold(name, entry.Privileges[0])
			})
	})
}
