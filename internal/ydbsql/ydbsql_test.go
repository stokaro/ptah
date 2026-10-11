package ydbsql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/core/sqlext"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/internal/sqlschema"
	"ptah.run/internal/ydbsql"
)

// TestSQL_ClaimsWhatAYQLDocumentDeclares reads a YQL document through the
// owner's grammar, and claims the topic namespace less the topic the header
// leaves unmanaged.
func TestSQL_ClaimsWhatAYQLDocumentDeclares(t *testing.T) {
	c := qt.New(t)
	owners := must.Must(sqlext.NewSet(ydbsql.SQL()))
	source := "-- ptah:not-described topic \"kept\"\nCREATE TOPIC `declared`;\n"

	database, statements, err := sqlschema.ReadWith(t.Context(), []byte(source), "ydb", nil, sqlschema.Options{Owners: owners})

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.IsNil)
	c.Assert(database.FeatureObjects.Len(), qt.Equals, 1)
	c.Assert(database.FeatureCoverage.Lookup(ydbtopic.Kind, ydbtopic.Ref("", "kept")).State, qt.Equals, schemaext.Uninspected)
	c.Assert(database.FeatureCoverage.Lookup(ydbtopic.Kind, ydbtopic.Ref("", "other")).State, qt.Equals, schemaext.Complete)
}

// TestSQL_ReadsTheYDBTargetOnly registers the grammar and the header kinds
// for YDB and for no other target.
func TestSQL_ReadsTheYDBTargetOnly(t *testing.T) {
	c := qt.New(t)
	owners := must.Must(sqlext.NewSet(ydbsql.SQL()))

	_, ydb := owners.Grammar("ydb")
	_, postgres := owners.Grammar("postgres")

	c.Assert(ydb, qt.IsTrue)
	c.Assert(postgres, qt.IsFalse)
	c.Assert(owners.ReadsHeader("ydb", "topic"), qt.IsTrue)
	c.Assert(owners.ReadsHeader("postgres", "topic"), qt.IsFalse)
}
