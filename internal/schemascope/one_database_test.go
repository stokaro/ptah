package schemascope_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemascope"
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

			err := schemascope.RefuseOutsideDatabase(platform.MariaDB, "app", "the target URL", &row.desired)

			c.Assert(err, qt.ErrorMatches, row.wantErr)
			c.Assert(err, qt.ErrorAs, new(*schemascope.OutsideDatabaseError))
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
		{name: "ClickHouse, a table and a database named for the URL's database", dialect: platform.ClickHouse, limit: "app",
			desired: schemamodel.Database{
				Schemas: []schemamodel.Schema{{Name: "app"}},
				Tables:  []schemamodel.Table{{Name: "t", Schema: "app"}},
			}},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(schemascope.RefuseOutsideDatabase(row.dialect, row.limit, "the target URL", &row.desired), qt.IsNil)
		})
	}
}

// TestRefuseOutsideDatabase_ClickHouse refuses what a read of the URL's database
// cannot find, and names the remedy an engine with no wider scope has.
func TestRefuseOutsideDatabase_ClickHouse(t *testing.T) {
	const remedy = "name that database in the URL, or leave the database out of the name"
	rows := []struct {
		name    string
		desired schemamodel.Database
		wantErr string
	}{
		{
			name:    "a declared database",
			desired: schemamodel.Database{Schemas: []schemamodel.Schema{{Name: "other"}}},
			wantErr: `the document declares database "other", but the target URL is limited to database "app"; ` + remedy,
		},
		{
			name:    "a table",
			desired: schemamodel.Database{Tables: []schemamodel.Table{{Name: "u", Schema: "other"}}},
			wantErr: `table "other.u" is in database "other", but the target URL is limited to database "app"; ` + remedy,
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			err := schemascope.RefuseOutsideDatabase(platform.ClickHouse, "app", "the target URL", &row.desired)

			c.Assert(err, qt.ErrorMatches, row.wantErr)
		})
	}
}

// TestRefuseOutsideTargetDatabase holds the connection's scope to the same rule.
// A whole server reaches every database, and a connection names the rest.
func TestRefuseOutsideTargetDatabase(t *testing.T) {
	desired := &schemamodel.Database{Schemas: []schemamodel.Schema{{Name: "other"}}}

	t.Run("one database", func(t *testing.T) {
		c := qt.New(t)

		err := schemascope.RefuseOutsideTargetDatabase(catalog.ServerInfo{Dialect: platform.MySQL, Schema: "app"}, desired)

		c.Assert(err, qt.ErrorMatches, `the document declares database "other", but the target URL is limited to database "app"; .*`)
	})
	t.Run("a whole server", func(t *testing.T) {
		c := qt.New(t)

		err := schemascope.RefuseOutsideTargetDatabase(catalog.ServerInfo{Dialect: platform.MySQL, Schema: "app", WholeServer: true}, desired)

		c.Assert(err, qt.IsNil)
	})
}
