package atlasschema

// White-box testing required: refuseOutsideDatabase decides from a desired
// model and the name of the database a URL is limited to. The exported paths
// that reach it, apply and diff, need a live one-database connection before
// they get there; the e2e tests drive those paths, and these rows pin the
// decision for each kind of object.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlassource"
)

// TestRefuseOutsideDatabase_FailurePath refuses each kind of object a
// document puts in a database other than the one the URL names.
func TestRefuseOutsideDatabase_FailurePath(t *testing.T) {
	rows := []struct {
		name    string
		desired schemamodel.Database
		wantErr string
	}{
		{
			name:    "a table",
			desired: schemamodel.Database{Tables: []schemamodel.Table{{Name: "u", Schema: "other"}}},
			wantErr: `table "other.u" is in database "other", but the target URL is limited to database "app"; ` +
				`name no database in the URL to reach the whole server, or leave the database out of the name`,
		},
		{
			name:    "a declared database",
			desired: schemamodel.Database{Schemas: []schemamodel.Schema{{Name: "other"}}},
			wantErr: `the document declares database "other", but the target URL is limited to database "app"; .*`,
		},
		{
			name:    "a view",
			desired: schemamodel.Database{Views: []schemamodel.View{{Name: "other.v"}}},
			wantErr: `view "other.v" is in database "other", .*`,
		},
		{
			name:    "a routine",
			desired: schemamodel.Database{Functions: []schemamodel.Function{{Name: "other.f"}}},
			wantErr: `routine "other.f" is in database "other", .*`,
		},
		{
			name:    "a trigger's table",
			desired: schemamodel.Database{Triggers: []schemamodel.Trigger{{Name: "tr", Table: "other.t"}}},
			wantErr: `trigger table "other.t" is in database "other", .*`,
		},
		{
			name:    "a sequence",
			desired: schemamodel.Database{Sequences: []schemamodel.Sequence{{Name: "s", Schema: "other"}}},
			wantErr: `sequence "other.s" is in database "other", .*`,
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			err := refuseOutsideDatabase(platform.MariaDB, "app", "the target URL", &row.desired)

			c.Assert(err, qt.ErrorMatches, row.wantErr)
			c.Assert(err, qt.ErrorAs, new(*OutsideDatabaseError))
		})
	}
}

// TestRefuseOutsideDatabase_HappyPath is the control: a name left unqualified
// or qualified with the URL's own database, a whole server, and another
// dialect's schema are not refused.
func TestRefuseOutsideDatabase_HappyPath(t *testing.T) {
	inOther := schemamodel.Database{Tables: []schemamodel.Table{{Name: "u", Schema: "other"}}}
	rows := []struct {
		name    string
		dialect string
		limit   string
		desired schemamodel.Database
	}{
		{name: "an unqualified table", dialect: platform.MySQL, limit: "app",
			desired: schemamodel.Database{Tables: []schemamodel.Table{{Name: "t"}}}},
		{name: "a table and a database named for the URL's database", dialect: platform.MySQL, limit: "app",
			desired: schemamodel.Database{
				Schemas: []schemamodel.Schema{{Name: "app"}},
				Tables:  []schemamodel.Table{{Name: "t", Schema: "app"}},
			}},
		{name: "a whole server", dialect: platform.MySQL, limit: "", desired: inOther},
		{name: "a PostgreSQL schema", dialect: platform.Postgres, limit: "public", desired: inOther},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(refuseOutsideDatabase(row.dialect, row.limit, "the target URL", &row.desired), qt.IsNil)
		})
	}
}

// TestRefuseOutsideComparedDatabase names the side of a diff whose URL is
// limited, and leaves a comparison of two databases alone.
func TestRefuseOutsideComparedDatabase(t *testing.T) {
	database := atlassource.State{Kind: atlassource.KindDatabase, DefaultSchema: "app"}
	document := atlassource.State{Kind: atlassource.KindLocalFile, Schema: &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "u", Schema: "other"}},
	}}
	rows := []struct {
		name     string
		from, to atlassource.State
		wantErr  string
	}{
		{name: "one database to a document", from: database, to: document,
			wantErr: `table "other.u" is in database "other", but --from is limited to database "app"; .*`},
		{name: "a document to one database", from: document, to: database,
			wantErr: `table "other.u" is in database "other", but --to is limited to database "app"; .*`},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(refuseOutsideComparedDatabase(platform.MySQL, row.from, row.to), qt.ErrorMatches, row.wantErr)
		})
	}
	t.Run("two databases", func(t *testing.T) {
		c := qt.New(t)
		other := atlassource.State{Kind: atlassource.KindDatabase, DefaultSchema: "app2", Schema: document.Schema}

		c.Assert(refuseOutsideComparedDatabase(platform.MySQL, database, other), qt.IsNil)
	})
}
