package goschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/goschema"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemaproperties"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/dialect/clickhouse/chsource"
	"ptah.run/engine"
	"ptah.run/internal/convert/goschematogo"
)

func indexSourceRuntime() *engine.Runtime {
	return must.Must(engine.New(engine.Provider{
		ID: "example.org/index-source", Targets: []engine.Target{{Name: "clickhouse", Aliases: []string{"ch"}}},
		Codecs: chschema.IndexCodecs(), Properties: []engine.PropertySource{{
			Target: "clickhouse", Format: schemaext.IndexPlatformProperties, Definitions: chsource.IndexDefinitions(), Service: chsource.IndexService{},
		}},
	}))
}

func TestRenderPreservesIndexSourceProperties(t *testing.T) {
	for _, single := range []bool{true, false} {
		t.Run(map[bool]string{true: "single", false: "per table"}[single], func(t *testing.T) {
			c := qt.New(t)
			db, err := goschema.ParseSource("input.go", `package models
//ptah:schema:table name="events"
type Event struct {
  //ptah:schema:field name="id" type="UInt64"
  ID uint64
  //ptah:schema:index name="by_id" fields="id" platform.clickhouse.type.state="default" platform.clickhouse.granularity="18446744073709551615" platform.other.future="" platform.other.escaped="line\n\"quote\"\\path"
  _ int
}`)
			c.Assert(err, qt.IsNil)
			c.Assert(db.Indexes, qt.HasLen, 1)
			c.Assert(db.Indexes[0].Overrides, qt.DeepEquals, map[string]map[string]string{
				"clickhouse": {"type.state": "default", "granularity": "18446744073709551615"},
				"other":      {"future": "", "escaped": "line\n\"quote\"\\path"},
			})
			files, err := goschematogo.Render(t.Context(), &db, goschematogo.Options{SingleFile: single})
			c.Assert(err, qt.IsNil)
			c.Assert(files, qt.HasLen, 1)
			parsed, err := goschema.ParseSource(files[0].Name, files[0].Data)
			c.Assert(err, qt.IsNil)
			c.Assert(parsed.Indexes[0].Overrides, qt.DeepEquals, db.Indexes[0].Overrides)
			again, err := goschematogo.Render(t.Context(), &parsed, goschematogo.Options{SingleFile: single})
			c.Assert(err, qt.IsNil)
			c.Assert(again, qt.DeepEquals, files)
		})
	}
}

func TestRenderEncodesSelectedIndexFacets(t *testing.T) {
	c := qt.New(t)
	runtime := indexSourceRuntime()
	facets := must.Must(schemaext.NewFacets(&chschema.DesiredIndex{
		IndexType:   chschema.Setting{State: chschema.Default},
		Granularity: chschema.GranularitySetting{State: chschema.Explicit, Value: 18446744073709551615},
	}))
	facets = must.Must(facets.WithTargetScope(chschema.IndexKind, "clickhouse"))
	db := &schemamodel.Database{
		Tables:  []schemamodel.Table{{Name: "events", StructName: "Event"}},
		Fields:  []schemamodel.Field{{StructName: "Event", Name: "id", FieldName: "ID", Type: "UInt64"}},
		Indexes: []schemamodel.Index{{Name: "by_id", StructName: "Event", TableName: "events", Fields: []string{"id"}, Facets: facets}},
	}
	files, err := goschematogo.Render(t.Context(), db, goschematogo.Options{SingleFile: true, Dialect: "ch", Runtime: runtime})
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	parsed, err := goschema.ParseSource(files[0].Name, files[0].Data)
	c.Assert(err, qt.IsNil)
	c.Assert(parsed.Indexes[0].Overrides["clickhouse"], qt.DeepEquals, map[string]string{"type.state": "default", "granularity": "18446744073709551615"})
	decoded, err := schemaproperties.DecodeIndexes(t.Context(), &parsed, "ch", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(decoded.Indexes[0].Facets.Equal(facets), qt.IsTrue)
	c.Assert(decoded.Indexes[0].Facets.TargetScope(chschema.IndexKind), qt.DeepEquals, []string{"clickhouse"})
	c.Assert(db.Indexes[0].Facets.Equal(facets), qt.IsTrue)
	c.Assert(db.Indexes[0].Overrides, qt.IsNil)
}

func TestRenderRefusesUnrepresentableIndexProperties(t *testing.T) {
	for _, test := range []struct {
		name       string
		properties map[string]map[string]string
	}{
		{"empty key", map[string]map[string]string{"clickhouse": {"": "x"}}},
		{"empty segment", map[string]map[string]string{"clickhouse": {"type..state": "default"}}},
		{"target separator", map[string]map[string]string{"click.house": {"type": "minmax"}}},
		{"nul", map[string]map[string]string{"clickhouse": {"type": "minmax\x00"}}},
		{"invalid utf8", map[string]map[string]string{"clickhouse": {"type": string([]byte{0xff})}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := &schemamodel.Database{Indexes: []schemamodel.Index{{Name: "by_id", Overrides: test.properties}}}
			files, err := goschematogo.Render(t.Context(), db, goschematogo.Options{SingleFile: true})
			c.Assert(err, qt.ErrorMatches, `index "by_id" has an unrepresentable Go annotation .*`)
			c.Assert(files, qt.IsNil)
		})
	}
}

func TestRenderRefusesConflictingIndexSourceDeclarations(t *testing.T) {
	c := qt.New(t)
	db := &schemamodel.Database{Indexes: []schemamodel.Index{{Name: "by_id", Type: "minmax", Overrides: map[string]map[string]string{"ch": {"type": "set(10)"}}}}}
	files, err := goschematogo.Render(t.Context(), db, goschematogo.Options{SingleFile: true, Dialect: "ch", Runtime: indexSourceRuntime()})
	c.Assert(err, qt.ErrorIs, schemaext.ErrDuplicate)
	c.Assert(files, qt.IsNil)
	c.Assert(db.Indexes[0].Type, qt.Equals, "minmax")
	c.Assert(db.Indexes[0].Overrides["ch"]["type"], qt.Equals, "set(10)")
}
