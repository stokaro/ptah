package atlasschema

import (
	"slices"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/ydbacl"
	"ptah.run/internal/ydburl"
)

// omitRealmRootGrants leaves the permissions on the database root out of a
// baseline that rebuilds the target in a YDB dev realm, on both sides of the
// comparison the baseline is planned from.
//
// The realm's root would stand in for the target's root, but a reset in the
// middle of a run removes what is under the realm and keeps the realm's root
// with its permissions, and nothing the cleanup reads can see a permission. A
// grant written there would outlive the target it came from and reach the
// next one rehearsed in the realm, so the baseline writes none, and a
// permission an earlier run left on the root plans no REVOKE either. Every
// other target is left as it is.
func omitRealmRootGrants(info catalog.ServerInfo, target *schemamodel.Database, dev *catalog.Database) {
	if platform.NormalizeDialect(info.Dialect) != platform.YDB {
		return
	}
	if parsed, err := ydburl.Parse(info.URL); err != nil || parsed.Realm == "" {
		return
	}
	onDatabase := func(grant schemamodel.Grant) bool { return grant.OnDatabase }
	if target != nil {
		target.Grants = slices.DeleteFunc(slices.Clone(target.Grants), onDatabase)
		target.RevokedGrants = slices.DeleteFunc(slices.Clone(target.RevokedGrants), onDatabase)
	}
	if dev != nil {
		dev.Grants = slices.DeleteFunc(slices.Clone(dev.Grants), func(grant catalog.Grant) bool {
			return grant.ObjectType == ydbacl.ObjectDatabase
		})
	}
}
