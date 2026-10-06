package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/internal/sqlschema"
	"ptah.run/migration/schemadiff"
)

// A desired file cannot request removal of families it has no syntax for.
// Exercise the source-to-comparison boundary, including an empty document.
func TestCompare_YQLPreservesUnrepresentedFamilies(t *testing.T) {
	c := qt.New(t)
	desired, _, err := sqlschema.Read(nil, "ydb")
	c.Assert(err, qt.IsNil)
	held := &catalog.Database{
		Secrets: []catalog.Secret{{Name: "credential"}},
	}
	diff := schemadiff.CompareWithDialect(&desired, held, "ydb")
	c.Assert(diff.HasChanges(), qt.IsFalse)
}

func TestCompare_YQLOmittedTTLRequestsRemoval(t *testing.T) {
	c := qt.New(t)
	desired, _, err := sqlschema.Read([]byte("CREATE TABLE events (id Int64 NOT NULL, ts Timestamp64, expires Uint64, PRIMARY KEY (id));"), "ydb")
	c.Assert(err, qt.IsNil)
	held := ydbTTLCatalog(&ast.RowDeletionPolicySpec{Column: "ts", Interval: "PT1H"})
	diff := schemadiff.CompareWithDialect(&desired, held, "ydb")
	c.Assert(diff.TablesModified, qt.HasLen, 1)
}

func TestCompare_YQLOmittedViewsAndTopicsRequestRemoval(t *testing.T) {
	c := qt.New(t)
	desired, _, err := sqlschema.Read(nil, "ydb")
	c.Assert(err, qt.IsNil)
	held := &catalog.Database{Topics: []catalog.Topic{readTopic("events")}, Views: []catalog.View{{Name: "summary"}}}
	diff := schemadiff.CompareWithDialect(&desired, held, "ydb")
	c.Assert(diff.ViewsRemoved, qt.HasLen, 1)
	c.Assert(diff.TopicsRemoved, qt.HasLen, 1)
}

// The source must carry both principal kind and membership through comparison.
// Database-wide principals remain when omitted, including from an empty file.
func TestCompare_YQLPrincipals(t *testing.T) {
	c := qt.New(t)
	desired, _, err := sqlschema.Read([]byte("CREATE USER app PASSWORD 'Secret1!'; CREATE GROUP readers WITH USER app; ALTER GROUP `DATA-READERS` ADD USER app;"), "ydb")
	c.Assert(err, qt.IsNil)
	held := ydbAccessCatalog()
	held.Tables, held.Constraints, held.Grants = nil, nil, nil
	c.Assert(schemadiff.CompareWithDialect(&desired, held, "ydb").HasChanges(), qt.IsFalse)
	changed, _, err := sqlschema.Read([]byte("CREATE USER app NOLOGIN; CREATE GROUP readers;"), "ydb")
	c.Assert(err, qt.IsNil)
	diff := schemadiff.CompareWithDialect(&changed, held, "ydb")
	c.Assert(diff.RolesModified, qt.HasLen, 1)
	c.Assert(diff.RolesModified[0].RoleName, qt.Equals, "app")
	c.Assert(diff.RoleMembershipsRemoved, qt.HasLen, 1)
	c.Assert(diff.RoleMembershipsRemoved[0].Role, qt.Equals, "readers")
	empty, _, err := sqlschema.Read(nil, "ydb")
	c.Assert(err, qt.IsNil)
	c.Assert(schemadiff.CompareWithDialect(&empty, held, "ydb").HasChanges(), qt.IsFalse)
}
