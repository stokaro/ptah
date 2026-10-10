package ydbsource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/annotation"
	"ptah.run/core/goschema"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/internal/ydbsource"
)

// ydbOwners selects the YDB owner alone, so each test reads YDB's directives
// through the frontend the way a runtime that registers the owner does.
var ydbOwners = must.Must(annotation.NewSet(ydbsource.Annotations()))

const ownedSource = `package entities

//ptah:schema:table name="events"
//ptah:schema:changefeed name="feed" mode="UPDATES" format="JSON"
type Event struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true"
	ID int64
}

//ptah:schema:secret name="pw" value_env="PTAH_SECRET_PW"
type Credentials struct{}
`

// TestParseSource_TheOwnerDeclaresAndClaims pins what selecting the owner
// adds: its objects, and the claim that the source describes each YDB
// namespace and every table's TTL, so a TTL no table declares is absent.
func TestParseSource_TheOwnerDeclaresAndClaims(t *testing.T) {
	c := qt.New(t)

	db, err := goschema.ParseSource(ydbOwners, "events.go", ownedSource)

	c.Assert(err, qt.IsNil)
	c.Assert(db.FeatureObjects.Len(), qt.Equals, 2)
	events := objectidentity.NewBuilder(identifier.ForDialect("ydb")).TableParts("", "events")
	c.Assert(db.FeatureCoverage.Lookup(ydbschema.TTLKind, events).State, qt.Equals, schemaext.Complete)
	c.Assert(db.FeatureCoverage.Lookup(ydbsecret.Kind, ydbsecret.Ref("", "other")).State, qt.Equals, schemaext.Complete)
	c.Assert(db.FeatureCoverage.Lookup(ydbschema.TablePartitioningKind, events).State, qt.Equals, schemaext.Complete)
}

// TestParseSource_WithoutTheOwnerTheDirectivesDeclareNothing is the control
// on the selection: a parse that selects no owner reads a YDB directive no
// more than one it does not know, and claims no knowledge of YDB's models, so
// a comparison leaves an existing secret or TTL alone rather than reading its
// absence as a removal.
func TestParseSource_WithoutTheOwnerTheDirectivesDeclareNothing(t *testing.T) {
	c := qt.New(t)

	db, err := goschema.ParseSource(annotation.None(), "events.go", ownedSource)

	c.Assert(err, qt.IsNil)
	c.Assert(db.FeatureObjects.Len(), qt.Equals, 0)
	events := objectidentity.NewBuilder(identifier.ForDialect("ydb")).TableParts("", "events")
	c.Assert(db.FeatureCoverage.Lookup(ydbschema.TTLKind, events).State, qt.Not(qt.Equals), schemaext.Complete)
	c.Assert(db.FeatureCoverage.Lookup(ydbsecret.Kind, ydbsecret.Ref("", "other")).State, qt.Not(qt.Equals), schemaext.Complete)
}

// TestParseSource_WithoutTheOwnerItsSettingsAreUnknown is the control on the
// attributes the owner adds to the frontend's directives: without the owner,
// a table's YDB setting is an attribute the directive does not know, and is
// refused rather than dropped.
func TestParseSource_WithoutTheOwnerItsSettingsAreUnknown(t *testing.T) {
	c := qt.New(t)

	db, err := goschema.ParseSource(annotation.None(), "events.go",
		"package entities\n\n//ptah:schema:table name=\"events\" key_bloom_filter=\"ENABLED\"\ntype Event struct{}\n")

	c.Assert(err, qt.ErrorMatches, `unknown annotation attribute "key_bloom_filter" on //ptah:schema:table at Event`)
	c.Assert(db.Tables, qt.HasLen, 0)
}
