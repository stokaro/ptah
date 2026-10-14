package pgdefaultprivilegeprovider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgdefaultprivilege"
	"ptah.run/internal/builtintest"
	"ptah.run/migration/schemadiff"
)

func TestYDBRefusesDefaultPrivilegesOnRenderAndCompare(t *testing.T) {
	c := qt.New(t)
	ref := must.Must(pgdefaultprivilege.Ref(pgdefaultprivilege.Identity{Grantor: "owner", Schema: "app", ObjectType: "TABLES", Grantee: "reader"}, identifier.ForDialect("postgres")))
	object := must.Must(pgdefaultprivilege.DesiredObject(ref, pgdefaultprivilege.Desired{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}}}))
	desired := &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(object)), FeatureCoverage: must.Must(pgdefaultprivilege.CompleteCoverage(schemaext.Desired))}
	sql, err := builtin.GetOrderedCreateStatements(desired, "ydb")
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `(?s).*PostgreSQL default privileges.*owner.*cannot be planned on ydb.*`)
	c.Assert(sql, qt.IsNil)
	diff, err := schemadiff.CompareWithDialect(t.Context(), desired, &catalog.Database{}, "ydb", builtintest.Runtime())
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `(?s).*PostgreSQL default privileges.*owner.*cannot be planned on ydb.*`)
	c.Assert(diff, qt.IsNil)
}

func TestYDBAcceptsDefaultPrivilegesScopedToPostgres(t *testing.T) {
	c := qt.New(t)
	ref := must.Must(pgdefaultprivilege.Ref(pgdefaultprivilege.Identity{Grantor: "owner", Schema: "app", ObjectType: "TABLES", Grantee: "reader"}, identifier.ForDialect("postgres")))
	object := must.Must(pgdefaultprivilege.DesiredObject(ref, pgdefaultprivilege.Desired{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}}}))
	object.Targets = []string{"postgres"}
	desired := &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(object)), FeatureCoverage: must.Must(pgdefaultprivilege.CompleteCoverage(schemaext.Desired))}
	sql, err := builtin.GetOrderedCreateStatements(desired, "ydb")
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.HasLen, 0)
	diff, err := schemadiff.CompareWithDialect(t.Context(), desired, &catalog.Database{}, "ydb", builtintest.Runtime())
	c.Assert(err, qt.IsNil)
	c.Assert(diff.HasChanges(), qt.IsFalse)
}
