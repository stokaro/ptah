package dbschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/convert/dbschematogo"
)

// rangeSchema is one range type as the reader reports it.
func rangeSchema(rangeType catalog.Range) *catalog.Database {
	return &catalog.Database{Ranges: []catalog.Range{rangeType}}
}

// onlyRange returns the single converted range.
func onlyRange(c *qt.C, database *schemamodel.Database) schemamodel.Range {
	c.Helper()
	c.Assert(database.Ranges, qt.HasLen, 1)
	return database.Ranges[0]
}

// TestConvert_CarriesEveryRangeAttribute pins the four attributes beside the
// subtype.
//
// The reader asks pg_range for all of them and the PostgreSQL renderer emits an
// option for each, but this conversion listed Name, Schema and Subtype and
// stopped. A range read off a live server was therefore described as having no
// canonicalization and no subtype difference function, and replaying that
// description built a different type: discrete values stop folding, so two
// ranges that compared equal in the source stop comparing equal in the replay,
// and a GiST index loses the penalty function it was built with
// (stokaro/ptah#2200).
func TestConvert_CarriesEveryRangeAttribute(t *testing.T) {
	c := qt.New(t)

	database := dbschematogo.ConvertDBSchemaToGoSchema(rangeSchema(catalog.Range{
		Name:           "fancy",
		Schema:         "public",
		Subtype:        "double precision",
		SubtypeOpClass: "float8_ops",
		Collation:      "en_US",
		Canonical:      "fancy_canonical",
		SubtypeDiff:    "f8diff",
	}), "")

	converted := onlyRange(c, database)
	c.Assert(converted.Name, qt.Equals, "fancy")
	c.Assert(converted.Schema, qt.Equals, "public")
	c.Assert(converted.Subtype, qt.Equals, "double precision")
	c.Assert(converted.SubtypeOpClass, qt.Equals, "float8_ops")
	c.Assert(converted.Collation, qt.Equals, "en_US")
	c.Assert(converted.Canonical, qt.Equals, "fancy_canonical")
	c.Assert(converted.SubtypeDiff, qt.Equals, "f8diff")
}

// TestConvert_LeavesARangeWithNoAttributesBare is the control.
//
// A range whose catalog row carries nothing beside the subtype must not gain
// invented attributes: the renderer emits an option for each non-empty one, and
// a description declaring a canonicalization function the type does not have
// would fail to apply.
func TestConvert_LeavesARangeWithNoAttributesBare(t *testing.T) {
	c := qt.New(t)

	database := dbschematogo.ConvertDBSchemaToGoSchema(rangeSchema(catalog.Range{
		Name:    "plainrange",
		Schema:  "public",
		Subtype: "double precision",
	}), "")

	converted := onlyRange(c, database)
	c.Assert(converted.Subtype, qt.Equals, "double precision")
	c.Assert(converted.SubtypeOpClass, qt.Equals, "")
	c.Assert(converted.Collation, qt.Equals, "")
	c.Assert(converted.Canonical, qt.Equals, "")
	c.Assert(converted.SubtypeDiff, qt.Equals, "")
}
