package postgres

import (
	"fmt"
	"maps"
	"slices"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgdefaultprivilege"
)

// defaultPrivilegeRow is one catalog ACL delta. Schema keeps the connected
// schema's name: an empty schema means global defaults, not the connection's
// default namespace. Revoked describes a right missing from the built-in ACL.
// This query shape stays private to the reader; consumers receive owner values.
type defaultPrivilegeRow struct {
	Grantor    string
	Schema     string
	ObjectType string
	Grantee    string
	Privilege  string
	WithOption bool
	Revoked    bool
}

// defaultPrivilegeLimit names an ACL this read cannot fully describe. A partial
// CockroachDB owner is one named grantee; FOR ALL ROLES and unknown object classes
// describe a whole ACL without inventing a named role.
type defaultPrivilegeLimit struct {
	Grantor      string
	Schema       string
	ObjectType   string
	OwnerPartial bool
}

func (r *Reader) recordDefaultPrivilegeObjects(database *catalog.Database, rows []defaultPrivilegeRow, limits []defaultPrivilegeLimit) error {
	grouped := make(map[pgdefaultprivilege.Identity]*pgdefaultprivilege.Observed)
	for _, row := range rows {
		identity := pgdefaultprivilege.Identity{Grantor: row.Grantor, Schema: row.Schema, ObjectType: row.ObjectType, Grantee: row.Grantee}
		value := grouped[identity]
		if value == nil {
			value = &pgdefaultprivilege.Observed{}
			grouped[identity] = value
		}
		if row.Revoked {
			value.Revoked = append(value.Revoked, row.Privilege)
		} else {
			value.Privileges = append(value.Privileges, pgdefaultprivilege.Grant{Privilege: row.Privilege, WithOption: row.WithOption})
		}
	}
	semantics := identifier.ForDialect(r.dialect)
	// Sorting the source identities makes validation errors deterministic too.
	identities := slices.Collect(maps.Keys(grouped))
	slices.SortFunc(identities, func(a, b pgdefaultprivilege.Identity) int {
		for _, values := range [][2]string{{a.Schema, b.Schema}, {a.Grantor, b.Grantor}, {a.ObjectType, b.ObjectType}, {a.Grantee, b.Grantee}} {
			if values[0] < values[1] {
				return -1
			}
			if values[0] > values[1] {
				return 1
			}
		}
		return 0
	})
	objects := database.FeatureObjects
	for _, identity := range identities {
		ref, err := pgdefaultprivilege.Ref(identity, semantics)
		if err != nil {
			return err
		}
		object, err := pgdefaultprivilege.ObservedObject(ref, *grouped[identity])
		if err != nil {
			return err
		}
		objects, err = objects.With(object)
		if err != nil {
			return err
		}
	}
	subjects, err := defaultPrivilegeSubjects(limits, semantics)
	if err != nil {
		return err
	}
	if err := recordDefaultPrivilegeCoverage(database, schemaext.Knowledge{State: schemaext.Complete}, subjects); err != nil {
		return err
	}
	database.FeatureObjects = objects
	return nil
}

func defaultPrivilegeSubjects(limits []defaultPrivilegeLimit, semantics identifier.Semantics) ([]schemaext.SubjectCoverage, error) {
	var subjects []schemaext.SubjectCoverage
	for _, limit := range limits {
		class := pgdefaultprivilege.Class{Grantor: limit.Grantor, Schema: limit.Schema, ObjectType: limit.ObjectType}
		ref, err := pgdefaultprivilege.ClassRef(class, semantics)
		if err != nil {
			return nil, err
		}
		reason := fmt.Sprintf("the catalog object class %q has no default privilege declaration", limit.ObjectType)
		if limit.Grantor == "" {
			reason = "FOR ALL ROLES has no named grantor in a schema declaration"
		}
		if limit.OwnerPartial {
			ref, err = pgdefaultprivilege.Ref(pgdefaultprivilege.Identity{Grantor: limit.Grantor, Schema: limit.Schema, ObjectType: limit.ObjectType, Grantee: limit.Grantor}, semantics)
			if err != nil {
				return nil, err
			}
			reason = "CockroachDB does not show which of its own privileges an owner took away from a global default"
		}
		subjects = append(subjects, schemaext.SubjectCoverage{Kind: pgdefaultprivilege.DefaultPrivilegeKind, Subject: ref, Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: reason}})
	}
	return subjects, nil
}

func recordDefaultPrivilegeCoverage(database *catalog.Database, knowledge schemaext.Knowledge, subjects []schemaext.SubjectCoverage) error {
	known, err := pgdefaultprivilege.Coverage(schemaext.Observed, knowledge, subjects)
	if err != nil {
		return err
	}
	database.FeatureCoverage, err = database.FeatureCoverage.Combine(known)
	return err
}
