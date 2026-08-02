package atlashcl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/atlashcl"
)

func TestParseMaterializedViewRefreshStrategy(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
materialized "user_stats" {
  schema           = schema.public
  as               = "SELECT count(*) FROM users"
  refresh_strategy = "concurrently"
  comment          = "user stats"
}
`), "schema.hcl")

	c.Assert(err, qt.IsNil)
	c.Assert(db.MaterializedViews, qt.HasLen, 1)
	c.Assert(db.MaterializedViews[0].Name, qt.Equals, "public.user_stats")
	c.Assert(db.MaterializedViews[0].Body, qt.Equals, "SELECT count(*) FROM users")
	c.Assert(db.MaterializedViews[0].RefreshStrategy, qt.Equals, "concurrently")
	c.Assert(db.MaterializedViews[0].Comment, qt.Equals, "user stats")
}

func TestParseMaterializedViewRefreshStrategyDefaultsToManual(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
materialized "user_stats" {
  as = "SELECT count(*) FROM users"
}
`), "schema.hcl")

	c.Assert(err, qt.IsNil)
	c.Assert(db.MaterializedViews, qt.HasLen, 1)
	c.Assert(db.MaterializedViews[0].RefreshStrategy, qt.Equals, "manual")
}

// TestParseMaterializedViewRefreshStrategyCanonicalized verifies the HCL path
// lowercases and trims the value through MaterializedView.Canonicalize, matching
// the Go-annotation path (which lowercases before canonicalizing). Neither path
// validates the value against an allow-list, so an arbitrary strategy string is
// accepted rather than rejected.
func TestParseMaterializedViewRefreshStrategyCanonicalized(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
materialized "user_stats" {
  as               = "SELECT count(*) FROM users"
  refresh_strategy = "  CONCURRENTLY  "
}
`), "schema.hcl")

	c.Assert(err, qt.IsNil)
	c.Assert(db.MaterializedViews, qt.HasLen, 1)
	c.Assert(db.MaterializedViews[0].RefreshStrategy, qt.Equals, "concurrently")
}

func TestParseMaterializedViewRejectsUnknownAttribute(t *testing.T) {
	c := qt.New(t)

	_, err := atlashcl.Parse([]byte(`
materialized "user_stats" {
  as       = "SELECT count(*) FROM users"
  populate = true
}
`), "schema.hcl")

	c.Assert(err, qt.ErrorMatches, `.*unsupported materialized attribute "populate".*`)
}

// TestMaterializedViewRefreshStrategyGoAnnotationParity asserts that the Go
// annotation frontend and the Atlas HCL frontend produce an equivalent
// MaterializedView for the same schema, closing the #684 parity gap for
// refresh_strategy.
func TestMaterializedViewRefreshStrategyGoAnnotationParity(t *testing.T) {
	c := qt.New(t)

	goDB, err := goschema.ParseSource("user_stats.go", `package models

//ptah:schema:matview name="user_stats" body="SELECT count(*) FROM users" refresh_strategy="concurrently" comment="user stats"
type UserStatsMatView struct{}
`)
	c.Assert(err, qt.IsNil)
	c.Assert(goDB.MaterializedViews, qt.HasLen, 1)

	hclDB, err := atlashcl.Parse([]byte(`
materialized "user_stats" {
  as               = "SELECT count(*) FROM users"
  refresh_strategy = "concurrently"
  comment          = "user stats"
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(hclDB.MaterializedViews, qt.HasLen, 1)

	goView := goDB.MaterializedViews[0]
	hclView := hclDB.MaterializedViews[0]
	c.Assert(hclView.Name, qt.Equals, goView.Name)
	c.Assert(hclView.Body, qt.Equals, goView.Body)
	c.Assert(hclView.RefreshStrategy, qt.Equals, goView.RefreshStrategy)
	c.Assert(hclView.Comment, qt.Equals, goView.Comment)
}
