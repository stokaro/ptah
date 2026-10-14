package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/core/sqlext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbreplication"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/engine/builtin"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/sqlschema"
	"ptah.run/migration/schemadiff"
)

// Once a family has desired YQL syntax, an empty document manages its absence.
func TestCompare_YQLOmittedReplicationRequestsRemoval(t *testing.T) {
	c := qt.New(t)
	desired, _, err := sqlschema.ReadWith(c.Context(), nil, "ydb", nil, yqlOwners())
	c.Assert(err, qt.IsNil)
	held := &catalog.Database{
		FeatureCoverage: completeYDBFixtureCoverage(),
		FeatureObjects: must.Must(schemaext.NewObjects(ydbreplication.ObservedReplicationObject("", "copy",
			ydbreplication.ReplicationSpec{}, ydbreplication.StateRunning))),
	}
	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), &desired, held, "ydb", must.Must(builtin.New())))
	c.Assert(diff.FeatureChanges, qt.DeepEquals, []schemaext.ChangeRecord{{Subject: ydbreplication.ReplicationRef("", "copy"),
		Value: ydbdiff.NewAsyncReplication(&ydbreplication.ObservedReplication{State: ydbreplication.StateRunning}, nil)}})
}

func TestCompare_YQLOmittedTTLRequestsRemoval(t *testing.T) {
	c := qt.New(t)
	desired, _, err := sqlschema.ReadWith(c.Context(), []byte("CREATE TABLE events (id Int64 NOT NULL, ts Timestamp64, expires Uint64, PRIMARY KEY (id));"), "ydb", nil, yqlOwners())
	c.Assert(err, qt.IsNil)
	held := ydbTTLCatalog(&ydbschema.TTL{Column: "ts", Interval: "PT1H"})
	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), &desired, held, "ydb", must.Must(builtin.New())))
	c.Assert(diff.TablesModified, qt.HasLen, 1)
}

func TestCompare_YQLOmittedViewsAndTopicsRequestRemoval(t *testing.T) {
	c := qt.New(t)
	desired, _, err := sqlschema.ReadWith(c.Context(), nil, "ydb", nil, yqlOwners())
	c.Assert(err, qt.IsNil)
	held := &catalog.Database{FeatureCoverage: completeYDBFixtureCoverage(), Views: []catalog.View{{Name: "summary"}},
		FeatureObjects: must.Must(schemaext.NewObjects(ydbtopic.ObservedObject("app", "events", readTopicSpec())))}
	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), &desired, held, "ydb", must.Must(builtin.New())))
	c.Assert(diff.ViewsRemoved, qt.HasLen, 1)
	c.Assert(diff.FeatureChanges, qt.DeepEquals, []schemaext.ChangeRecord{
		{Subject: ydbtopic.Ref("app", "events"), Value: &ydbdiff.Topic{Before: &ydbtopic.Observed{Spec: readTopicSpec()}}},
	})
}

func TestCompare_YQLSecretsDeclaredAndOmitted(t *testing.T) {
	for _, test := range []struct {
		name   string
		source string
		want   []schemaext.ChangeRecord
	}{
		{name: "declared", source: "CREATE SECRET credential WITH (value = $PTAH_SECRET_TEST);"},
		{name: "omitted", want: []schemaext.ChangeRecord{{Subject: ydbsecret.Ref("", "credential"), Value: &ydbdiff.Secret{Before: &ydbsecret.Observed{}}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired, _, err := sqlschema.ReadWith(c.Context(), []byte(test.source), "ydb", nil, yqlOwners())
			c.Assert(err, qt.IsNil)
			held := &catalog.Database{FeatureCoverage: completeYDBFixtureCoverage(),
				FeatureObjects: must.Must(schemaext.NewObjects(ydbsecret.ObservedObject("", "credential")))}
			diff := must.Must(schemadiff.CompareWithDialect(t.Context(), &desired, held, "ydb", must.Must(builtin.New())))
			c.Assert(diff.FeatureChanges, qt.DeepEquals, test.want)
		})
	}
}

// The source must carry both principal kind and membership through comparison.
// Database-wide principals remain when omitted, including from an empty file.
func TestCompare_YQLPrincipals(t *testing.T) {
	c := qt.New(t)
	desired, _, err := sqlschema.ReadWith(c.Context(), []byte("CREATE USER app PASSWORD 'Secret1!'; CREATE GROUP readers WITH USER app; ALTER GROUP `DATA-READERS` ADD USER app;"), "ydb", nil, yqlOwners())
	c.Assert(err, qt.IsNil)
	held := ydbAccessCatalog()
	held.Tables, held.Constraints, held.Grants = nil, nil, nil
	c.Assert(must.Must(schemadiff.CompareWithDialect(t.Context(), &desired, held, "ydb", must.Must(builtin.New()))).HasChanges(), qt.IsFalse)
	changed, _, err := sqlschema.ReadWith(c.Context(), []byte("CREATE USER app NOLOGIN; CREATE GROUP readers;"), "ydb", nil, yqlOwners())
	c.Assert(err, qt.IsNil)
	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), &changed, held, "ydb", must.Must(builtin.New())))
	c.Assert(diff.RolesModified, qt.HasLen, 1)
	c.Assert(diff.RolesModified[0].RoleName, qt.Equals, "app")
	c.Assert(diff.RoleMembershipsRemoved, qt.HasLen, 1)
	c.Assert(diff.RoleMembershipsRemoved[0].Role, qt.Equals, "readers")
	empty, _, err := sqlschema.ReadWith(c.Context(), nil, "ydb", nil, yqlOwners())
	c.Assert(err, qt.IsNil)
	c.Assert(must.Must(schemadiff.CompareWithDialect(t.Context(), &empty, held, "ydb", must.Must(builtin.New()))).HasChanges(), qt.IsFalse)
}

func TestCompare_YQLPrivileges(t *testing.T) {
	c := qt.New(t)
	const objects = "CREATE TABLE `shop/orders` (id Int64 NOT NULL, PRIMARY KEY(id)); CREATE GROUP readers; CREATE USER app PASSWORD 'Secret1!'; ALTER GROUP readers ADD USER app; ALTER GROUP `DATA-READERS` ADD USER app;"
	const grants = "GRANT SELECT ROW, LIST ON `shop/orders` TO readers; GRANT LIST ON shop TO readers; GRANT CONNECT ON `/local` TO app;"
	load, err := sqlschema.ReadLoad(c.Context(), sqlext.GrammarRequest{Target: "ydb", DatabasePath: "/local",
		Files: []sqlext.SourceFile{{Path: "schema.sql", Text: objects + grants}}}, yqlOwners())
	c.Assert(err, qt.IsNil)
	desired := load.Database
	held := ydbAccessCatalog()
	c.Assert(must.Must(schemadiff.CompareWithDialect(t.Context(), &desired, held, "ydb", must.Must(builtin.New()))).HasChanges(), qt.IsFalse)
	omitted, _, err := sqlschema.ReadWith(c.Context(), []byte(objects), "ydb", nil, yqlOwners())
	c.Assert(err, qt.IsNil)
	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), &omitted, held, "ydb", must.Must(builtin.New())))
	c.Assert(diff.GrantsRemoved, qt.HasLen, 4)
	c.Assert(diff.RolesRemoved, qt.HasLen, 0)
}

// yqlOwners selects the SQL owners the built-in runtime registers, whose YDB
// grammar reads YQL and claims what a YQL document declares, as a production
// read does.
func yqlOwners() sqlschema.Options {
	return sqlschema.Options{Owners: builtintest.Runtime().SQL()}
}
