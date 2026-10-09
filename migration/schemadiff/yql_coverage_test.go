package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/engine/builtin"
	"ptah.run/internal/sqlschema"
	"ptah.run/migration/schemadiff"
)

// Once a family has desired YQL syntax, an empty document manages its absence.
func TestCompare_YQLOmittedReplicationRequestsRemoval(t *testing.T) {
	c := qt.New(t)
	desired, _, err := sqlschema.Read(nil, "ydb")
	c.Assert(err, qt.IsNil)
	held := &catalog.Database{
		FeatureCoverage:   completeYDBFixtureCoverage(),
		AsyncReplications: []catalog.AsyncReplication{{Name: "copy"}},
	}
	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), &desired, held, "ydb", must.Must(builtin.New())))
	c.Assert(diff.AsyncReplicationsRemoved, qt.HasLen, 1)
}

func TestCompare_YQLOmittedTTLRequestsRemoval(t *testing.T) {
	c := qt.New(t)
	desired, _, err := sqlschema.Read([]byte("CREATE TABLE events (id Int64 NOT NULL, ts Timestamp64, expires Uint64, PRIMARY KEY (id));"), "ydb")
	c.Assert(err, qt.IsNil)
	held := ydbTTLCatalog(&ast.RowDeletionPolicySpec{Column: "ts", Interval: "PT1H"})
	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), &desired, held, "ydb", must.Must(builtin.New())))
	c.Assert(diff.TablesModified, qt.HasLen, 1)
}

func TestCompare_YQLOmittedViewsAndTopicsRequestRemoval(t *testing.T) {
	c := qt.New(t)
	desired, _, err := sqlschema.Read(nil, "ydb")
	c.Assert(err, qt.IsNil)
	held := &catalog.Database{FeatureCoverage: completeYDBFixtureCoverage(), Topics: []catalog.Topic{readTopic("events")}, Views: []catalog.View{{Name: "summary"}}}
	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), &desired, held, "ydb", must.Must(builtin.New())))
	c.Assert(diff.ViewsRemoved, qt.HasLen, 1)
	c.Assert(diff.TopicsRemoved, qt.HasLen, 1)
}

func TestCompare_YQLSecretsDeclaredAndOmitted(t *testing.T) {
	for _, test := range []struct {
		name    string
		source  string
		removed []string
	}{
		{name: "declared", source: "CREATE SECRET credential WITH (value = $PTAH_SECRET_TEST);"},
		{name: "omitted", removed: []string{"credential"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired, _, err := sqlschema.Read([]byte(test.source), "ydb")
			c.Assert(err, qt.IsNil)
			held := &catalog.Database{FeatureCoverage: completeYDBFixtureCoverage(), Secrets: []catalog.Secret{{Name: "credential"}}}
			diff := must.Must(schemadiff.CompareWithDialect(t.Context(), &desired, held, "ydb", must.Must(builtin.New())))
			c.Assert(diff.SecretsRemoved.Names(), qt.DeepEquals, test.removed)
			c.Assert(diff.SecretsAdded, qt.HasLen, 0)
			c.Assert(diff.SecretsRotated, qt.HasLen, 0)
		})
	}
}

// The source must carry both principal kind and membership through comparison.
// Database-wide principals remain when omitted, including from an empty file.
func TestCompare_YQLPrincipals(t *testing.T) {
	c := qt.New(t)
	desired, _, err := sqlschema.Read([]byte("CREATE USER app PASSWORD 'Secret1!'; CREATE GROUP readers WITH USER app; ALTER GROUP `DATA-READERS` ADD USER app;"), "ydb")
	c.Assert(err, qt.IsNil)
	held := ydbAccessCatalog()
	held.Tables, held.Constraints, held.Grants = nil, nil, nil
	c.Assert(must.Must(schemadiff.CompareWithDialect(t.Context(), &desired, held, "ydb", must.Must(builtin.New()))).HasChanges(), qt.IsFalse)
	changed, _, err := sqlschema.Read([]byte("CREATE USER app NOLOGIN; CREATE GROUP readers;"), "ydb")
	c.Assert(err, qt.IsNil)
	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), &changed, held, "ydb", must.Must(builtin.New())))
	c.Assert(diff.RolesModified, qt.HasLen, 1)
	c.Assert(diff.RolesModified[0].RoleName, qt.Equals, "app")
	c.Assert(diff.RoleMembershipsRemoved, qt.HasLen, 1)
	c.Assert(diff.RoleMembershipsRemoved[0].Role, qt.Equals, "readers")
	empty, _, err := sqlschema.Read(nil, "ydb")
	c.Assert(err, qt.IsNil)
	c.Assert(must.Must(schemadiff.CompareWithDialect(t.Context(), &empty, held, "ydb", must.Must(builtin.New()))).HasChanges(), qt.IsFalse)
}

func TestCompare_YQLPrivileges(t *testing.T) {
	c := qt.New(t)
	const objects = "CREATE TABLE `shop/orders` (id Int64 NOT NULL, PRIMARY KEY(id)); CREATE GROUP readers; CREATE USER app PASSWORD 'Secret1!'; ALTER GROUP readers ADD USER app; ALTER GROUP `DATA-READERS` ADD USER app;"
	const grants = "GRANT SELECT ROW, LIST ON `shop/orders` TO readers; GRANT LIST ON shop TO readers; GRANT CONNECT ON `/local` TO app;"
	document := sqlschema.NewDocument(nil)
	document.YDBDatabasePath = "/local"
	desired, _, err := sqlschema.ReadOnto([]byte(objects+grants), "ydb", document)
	c.Assert(err, qt.IsNil)
	held := ydbAccessCatalog()
	c.Assert(must.Must(schemadiff.CompareWithDialect(t.Context(), &desired, held, "ydb", must.Must(builtin.New()))).HasChanges(), qt.IsFalse)
	omitted, _, err := sqlschema.Read([]byte(objects), "ydb")
	c.Assert(err, qt.IsNil)
	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), &omitted, held, "ydb", must.Must(builtin.New())))
	c.Assert(diff.GrantsRemoved, qt.HasLen, 4)
	c.Assert(diff.RolesRemoved, qt.HasLen, 0)
}
