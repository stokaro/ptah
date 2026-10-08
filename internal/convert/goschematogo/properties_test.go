package goschematogo_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/goschema"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemaproperties"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/goschematogo"
)

func TestRender_PreservesTableProperties(t *testing.T) {
	for _, single := range []bool{true, false} {
		t.Run(map[bool]string{true: "single", false: "per table"}[single], func(t *testing.T) {
			c := qt.New(t)
			source := "package models\n" + `//ptah:schema:table name="events" platform.clickhouse.engine="ReplacingMergeTree(version)" platform.clickhouse.primary_key="" platform.clickhouse.order_by="tuple(id, version)" platform.postgres.comment="line\n\"quote\"\\path"` + "\ntype Events struct {\n//ptah:schema:field type=\"UInt64\"\nID uint64\n}\n"
			db, err := goschema.ParseSource("input.go", source)
			c.Assert(err, qt.IsNil)
			files, err := goschematogo.Render(c.Context(), &db, goschematogo.Options{SingleFile: single})
			c.Assert(err, qt.IsNil)
			c.Assert(files, qt.HasLen, 1)
			got, err := goschema.ParseSource(files[0].Name, files[0].Data)
			c.Assert(err, qt.IsNil)
			c.Assert(got.Tables[0].Overrides, qt.DeepEquals, db.Tables[0].Overrides)
			c.Assert(string(files[0].Data), qt.Contains, `platform.clickhouse.primary_key=""`)
			again, err := goschematogo.Render(c.Context(), &db, goschematogo.Options{SingleFile: single})
			c.Assert(err, qt.IsNil)
			c.Assert(again, qt.DeepEquals, files)
		})
	}
}

func TestRender_EncodesSelectedOwnedTableFacets(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	facets := must.Must(schemaext.NewFacets(&chschema.DesiredTable{
		Engine:     chschema.Setting{State: chschema.Explicit, Value: "MergeTree"},
		OrderBy:    chschema.Setting{State: chschema.Default},
		PrimaryKey: chschema.Setting{State: chschema.Explicit},
	}))
	facets = must.Must(facets.WithTargetScope(chschema.TableKind, "clickhouse"))
	db := &schemamodel.Database{Tables: []schemamodel.Table{{Name: "events", StructName: "Events", Facets: facets}}, Fields: []schemamodel.Field{{StructName: "Events", Name: "id", FieldName: "ID", Type: "UInt64"}}}
	files, err := goschematogo.Render(c.Context(), db, goschematogo.Options{SingleFile: true, Dialect: "clickhouse", Runtime: runtime})
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	parsed, err := goschema.ParseSource("schema.go", files[0].Data)
	c.Assert(err, qt.IsNil)
	decoded, err := schemaproperties.DecodeTables(c.Context(), &parsed, "clickhouse", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(decoded.Tables[0].Facets.Equal(facets), qt.IsTrue)
	c.Assert(decoded.Tables[0].Facets.TargetScope(chschema.TableKind), qt.DeepEquals, []string{"clickhouse"})
	c.Assert(db.Tables[0].Facets.Equal(facets), qt.IsTrue)
	c.Assert(db.Tables[0].Overrides, qt.IsNil)
}

func TestRender_RefusesUnrepresentableTablePropertyNames(t *testing.T) {
	for _, key := range []string{"", "engine bad", "engine\nname", "engine..state"} {
		t.Run(key, func(t *testing.T) {
			c := qt.New(t)
			db := &schemamodel.Database{Tables: []schemamodel.Table{{Name: "events", StructName: "Events", Overrides: map[string]map[string]string{"clickhouse": {key: "value"}}}}}
			files, err := goschematogo.Render(c.Context(), db, goschematogo.Options{SingleFile: true})
			c.Assert(err, qt.ErrorMatches, `table "events" has an unrepresentable Go annotation property .*`)
			c.Assert(files, qt.IsNil)
		})
	}
}

func TestRender_RefusesCanceledContext(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(c.Context())
	cancel()
	files, err := goschematogo.Render(ctx, &schemamodel.Database{}, goschematogo.Options{})
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(files, qt.IsNil)
}

func TestRender_RefusesConflictingOwnedProperties(t *testing.T) {
	c := qt.New(t)
	db := &schemamodel.Database{Tables: []schemamodel.Table{{Name: "events", StructName: "Events", Overrides: map[string]map[string]string{
		"clickhouse": {"engine": "Memory", "engine.state": "default"},
	}}}}
	files, err := goschematogo.Render(c.Context(), db, goschematogo.Options{SingleFile: true, Dialect: "clickhouse", Runtime: must.Must(builtin.New())})
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(files, qt.IsNil)
	c.Assert(db.Tables[0].Overrides["clickhouse"], qt.HasLen, 2)
}

func TestRender_RefusesAmbiguousPropertyTargets(t *testing.T) {
	c := qt.New(t)
	db := &schemamodel.Database{Tables: []schemamodel.Table{{Name: "events", StructName: "Events", Overrides: map[string]map[string]string{
		"click.house": {"engine": "Memory"},
	}}}}
	files, err := goschematogo.Render(c.Context(), db, goschematogo.Options{SingleFile: true})
	c.Assert(err, qt.ErrorMatches, `table "events" has an unrepresentable Go annotation target "click.house"`)
	c.Assert(files, qt.IsNil)
}
