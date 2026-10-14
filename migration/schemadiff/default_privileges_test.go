package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgdefaultprivilege"
	"ptah.run/migration/schemadiff"
)

func defaultPrivilegeRef(grantee string) objectidentity.ID {
	return must.Must(pgdefaultprivilege.Ref(pgdefaultprivilege.Identity{
		Grantor: "app_owner", Schema: "app", ObjectType: "TABLES", Grantee: grantee,
	}, identifier.ForDialect("postgres")))
}

func declaredDefaultPrivilege(grantee string, targets ...string) schemaext.Object {
	object := must.Must(pgdefaultprivilege.DesiredObject(defaultPrivilegeRef(grantee), pgdefaultprivilege.Desired{
		Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}},
	}))
	object.Targets = targets
	return object
}

func observedDefaultPrivilege() schemaext.Object {
	return must.Must(pgdefaultprivilege.ObservedObject(defaultPrivilegeRef("app_reader"), pgdefaultprivilege.Observed{
		Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}},
	}))
}

func desiredDefaults(objects ...schemaext.Object) *schemamodel.Database {
	return &schemamodel.Database{
		FeatureObjects:  must.Must(schemaext.NewObjects(objects...)),
		FeatureCoverage: must.Must(pgdefaultprivilege.CompleteCoverage(schemaext.Desired)),
	}
}

func currentDefaults(objects ...schemaext.Object) *catalog.Database {
	return &catalog.Database{
		FeatureObjects:  must.Must(schemaext.NewObjects(objects...)),
		FeatureCoverage: must.Must(pgdefaultprivilege.CompleteCoverage(schemaext.Observed)),
	}
}

// The public comparison must call the owner and count its result in HasChanges.
func TestCompare_ADeclaredDefaultPrivilegeReachesTheDiff(t *testing.T) {
	c := qt.New(t)
	desired := desiredDefaults(declaredDefaultPrivilege("app_reader"))
	diff, err := schemadiff.CompareWithDialect(t.Context(), desired, currentDefaults(), "postgres", must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)
	c.Assert(diff.FeatureChanges, qt.HasLen, 1)
	c.Assert(diff.FeatureChanges[0].Subject, qt.Equals, defaultPrivilegeRef("app_reader"))
	c.Assert(diff.FeatureChanges[0].Value, qt.DeepEquals, &pgdefaultprivilege.Change{
		Before: &pgdefaultprivilege.Observed{}, Granted: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}},
	})
	c.Assert(diff.HasChanges(), qt.IsTrue)
}

func TestCompare_ADefaultPrivilegeMatchingItsRowIsNoChange(t *testing.T) {
	c := qt.New(t)
	desired := desiredDefaults(declaredDefaultPrivilege("app_reader"))
	desired.Roles = []schemamodel.Role{{Name: "app_owner"}}
	current := currentDefaults(observedDefaultPrivilege())
	current.Roles = []catalog.Role{{Name: "app_owner"}}
	diff, err := schemadiff.CompareWithDialect(t.Context(), desired, current, "postgres", must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)
	c.Assert(diff.FeatureChanges, qt.HasLen, 0)
	c.Assert(diff.HasChanges(), qt.IsFalse)
}

func TestCompare_AScopedAwayDefaultPrivilegeIsNotPlannedForRemoval(t *testing.T) {
	for _, dialect := range []string{"postgres", "postgresql", "sqlite", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			desired := desiredDefaults(declaredDefaultPrivilege("app_reader", "mysql"))
			desired.Roles = []schemamodel.Role{{Name: "app_owner"}}
			current := currentDefaults(observedDefaultPrivilege())
			current.Roles = []catalog.Role{{Name: "app_owner"}}
			diff, err := schemadiff.CompareWithDialect(t.Context(), desired, current, dialect, must.Must(builtin.New()))
			c.Assert(err, qt.IsNil)
			c.Assert(diff.FeatureChanges, qt.HasLen, 0)
		})
	}
}

// Suppressing one identity must leave another identity of its grantor removable.
func TestCompare_AnUndeclaredDefaultPrivilegeIsStillRemovedOnAScopedTarget(t *testing.T) {
	c := qt.New(t)
	desired := desiredDefaults(declaredDefaultPrivilege("somebody_else", "mysql"))
	desired.Roles = []schemamodel.Role{{Name: "app_owner"}}
	current := currentDefaults(observedDefaultPrivilege())
	current.Roles = []catalog.Role{{Name: "app_owner"}}
	diff, err := schemadiff.CompareWithDialect(t.Context(), desired, current, "postgres", must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)
	c.Assert(diff.FeatureChanges, qt.HasLen, 1)
	c.Assert(diff.FeatureChanges[0].Subject, qt.Equals, defaultPrivilegeRef("app_reader"))
	c.Assert(diff.FeatureChanges[0].Value, qt.DeepEquals, &pgdefaultprivilege.Change{
		Before:  &pgdefaultprivilege.Observed{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}}},
		Revoked: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}},
	})
}
