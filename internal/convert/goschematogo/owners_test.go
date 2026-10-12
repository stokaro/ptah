package goschematogo_test

import (
	"cmp"
	"context"
	"encoding/json"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/annotation"
	"ptah.run/core/goschema"
	"ptah.run/core/objectidentity"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/dialect/mssql/mssqlschema"
	"ptah.run/dialect/timescaledb/tsschema"
	"ptah.run/engine"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/convert/goschematogo"
)

// The paint owner is a feature owner the bundled runtime does not know: a
// mural is a named object of its, and a shade a setting of a table.
const (
	muralKind schemaext.Kind = "example.org/paint/mural"
	shadeKind schemaext.Kind = "example.org/paint/shade"
)

type mural struct {
	Wall string `json:"wall,omitempty"`
}

func (*mural) Kind() schemaext.Kind { return muralKind }
func (m *mural) Clone() schemaext.Value {
	cloned := *m
	return &cloned
}
func (m *mural) Equal(other schemaext.Value) bool {
	o, ok := other.(*mural)
	return ok && *o == *m
}

type shade struct {
	Name string `json:"name"`
}

func (*shade) Kind() schemaext.Kind { return shadeKind }
func (s *shade) Clone() schemaext.Value {
	cloned := *s
	return &cloned
}
func (s *shade) Equal(other schemaext.Value) bool {
	o, ok := other.(*shade)
	return ok && *o == *s
}

func muralObject(name, wall string) schemaext.Object {
	return schemaext.Object{Ref: objectidentity.ID{Kind: objectidentity.Kind(muralKind), Name: objectidentity.Part{Source: name, Normalized: name}},
		Value: &mural{Wall: wall}}
}

// paintWriter writes the paint owner's models back as the directives its
// decoder reads: a mural among the standalone objects, after the schemas,
// and a shade beside its table.
type paintWriter struct{}

func (paintWriter) EncodeAnnotations(_ context.Context, request annotation.EncodeRequest) (annotation.EncodeResult, error) {
	var result annotation.EncodeResult
	for _, object := range request.Objects {
		attributes := []annotation.EncodedAttribute{{Name: "name", Value: object.Ref.Name.Source}}
		if wall := object.Value.(*mural).Wall; wall != "" {
			attributes = append(attributes, annotation.EncodedAttribute{Name: "wall", Value: wall})
		}
		result.Directives = append(result.Directives, annotation.EncodedDirective{Directive: "ptah:schema:mural",
			Placement: annotation.AfterSchemas, Attributes: attributes})
	}
	for _, slot := range request.Slots {
		painted, _, err := schemaext.FacetAs[*shade](slot.Facets, shadeKind)
		if err != nil {
			return annotation.EncodeResult{}, err
		}
		table := slot.Slot.Table
		result.Directives = append(result.Directives, annotation.EncodedDirective{Directive: "ptah:schema:shade",
			Placement: annotation.AfterTable, Table: &table,
			Attributes: []annotation.EncodedAttribute{{Name: "table", Value: table.QualifiedName()}, {Name: "name", Value: painted.Name}}})
	}
	return result, nil
}

// fixedWriter writes the same directives whatever the schema holds.
type fixedWriter []annotation.EncodedDirective

func (w fixedWriter) EncodeAnnotations(context.Context, annotation.EncodeRequest) (annotation.EncodeResult, error) {
	return annotation.EncodeResult{Directives: w}, nil
}

func decodePaint(declaration annotation.Declaration) ([]annotation.Contribution, error) {
	kv := declaration.Attributes
	if declaration.Directive == "ptah:schema:shade" {
		return []annotation.Contribution{{Facet: &shade{Name: kv["name"]}, Table: kv["table"], Label: "a shade"}}, nil
	}
	object := muralObject(kv["name"], kv["wall"])
	return []annotation.Contribution{{Object: &object, Label: "a mural"}}, nil
}

// paintProvider is the paint owner as a provider, with encoder writing its
// directives.
func paintProvider(id string, encoder annotation.Encoder) engine.Provider {
	codec := func(prototype schemaext.Payload) schemaext.Codec {
		encode := func(payload schemaext.Payload) (json.RawMessage, error) { return json.Marshal(payload) }
		return schemaext.Codec{Prototype: prototype, Representation: schemaext.Desired, Version: 1, Definition: json.RawMessage(`{"type":"object"}`),
			Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
				return payload.(schemaext.Value).Clone(), nil
			},
			Encode: encode, Canonical: encode,
			Decode: func(data json.RawMessage) (schemaext.Payload, error) {
				value := prototype.(schemaext.Value).Clone()
				return value, json.Unmarshal(data, value)
			}}
	}
	text := func(name string, required bool) annotation.Attribute {
		return annotation.Attribute{Name: name, Description: name + ".", Value: "string", Required: required}
	}
	return engine.Provider{ID: id, Codecs: []schemaext.Codec{codec(&mural{}), codec(&shade{})},
		Annotations: []annotation.Extension{{
			Owner: id,
			Directives: []annotation.Directive{
				{Name: "ptah:schema:mural", Scopes: []annotation.Scope{annotation.ScopeStruct}, Attributes: []annotation.Attribute{text("name", true), text("wall", false)}},
				{Name: "ptah:schema:shade", Scopes: []annotation.Scope{annotation.ScopeStruct}, Attributes: []annotation.Attribute{text("table", true), text("name", true)}},
				{Name: "ptah:schema:stroke", Scopes: []annotation.Scope{annotation.ScopeField}, Attributes: []annotation.Attribute{text("name", true)}},
			},
			Kinds:    []schemaext.Kind{muralKind, shadeKind},
			Decode:   decodePaint,
			Encoder:  encoder,
			Encodes:  []schemaext.Kind{muralKind, shadeKind},
			Coverage: annotation.Unlimited(func() (schemaext.Coverage, error) { return schemaext.Coverage{}, nil }),
		}},
	}
}

func paintedSchema(c *qt.C) *schemamodel.Database {
	c.Helper()
	return &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Room", Name: "rooms", Schema: "app", Facets: must.Must(schemaext.NewFacets(&shade{Name: "teal"}))},
			{StructName: "Hall", Name: "halls"},
		},
		Fields: []schemamodel.Field{
			{StructName: "Room", FieldName: "ID", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Hall", FieldName: "ID", Name: "id", Type: "INTEGER", Primary: true},
		},
		Schemas:        []schemamodel.Schema{{Name: "app"}},
		FeatureObjects: must.Must(schemaext.NewObjects(muralObject("north", "brick"), muralObject("south", ""))),
	}
}

// TestRender_AnOwnerOutsideTheBundleWritesItsDirectives pins the owner seam
// end to end: an owner the bundled runtime does not know, reached through
// engine.New, writes its object after the schemas and its table setting
// beside the table, and the source reads back to the same schema through the
// same owner.
func TestRender_AnOwnerOutsideTheBundleWritesItsDirectives(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(engine.New(paintProvider("example.org/paint", paintWriter{})))
	db := paintedSchema(c)

	files, err := goschematogo.Render(c.Context(), db, goschematogo.Options{Annotations: runtime.Annotations(), SingleFile: true})

	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	c.Assert(string(files[0].Data), qt.Equals, `// Code generated by ptah introspect; DO NOT EDIT.

package models

//ptah:schema:schema name="app"
//ptah:schema:mural name="north" wall="brick"
//ptah:schema:mural name="south"
type PtahSchemaObjects struct{}

//ptah:schema:table name="rooms" schema="app"
//ptah:schema:shade table="app.rooms" name="teal"
type Room struct {
	//ptah:schema:field name="id" type="INTEGER" not_null="true" primary="true"
	ID int
}

//ptah:schema:table name="halls"
type Hall struct {
	//ptah:schema:field name="id" type="INTEGER" not_null="true" primary="true"
	ID int
}
`)
	parsed, err := goschema.ParseSource(runtime.Annotations(), files[0].Name, files[0].Data)
	c.Assert(err, qt.IsNil)
	c.Assert(parsed.FeatureObjects, qt.DeepEquals, db.FeatureObjects)
	c.Assert(tableFacetsByName(&parsed), qt.DeepEquals, map[string]schemaext.Facets{"rooms": db.Tables[0].Facets, "halls": {}})
}

// TestRender_AnOwnersTableSettingStaysInItsTablesFile pins that a directive
// placed at a table is written in that table's file, where the frontend looks
// for the table it names.
func TestRender_AnOwnersTableSettingStaysInItsTablesFile(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(engine.New(paintProvider("example.org/paint", paintWriter{})))
	db := paintedSchema(c)

	files, err := goschematogo.Render(c.Context(), db, goschematogo.Options{Annotations: runtime.Annotations(), PerTable: true})

	c.Assert(err, qt.IsNil)
	source := fstest.MapFS{}
	names := make([]string, 0, len(files))
	for _, file := range files {
		source["models/"+file.Name] = &fstest.MapFile{Data: file.Data}
		names = append(names, file.Name)
	}
	c.Assert(names, qt.DeepEquals, []string{"schema_objects.go", "app_rooms.go", "halls.go"})
	parsed, err := goschema.ParseFS(runtime.Annotations(), source, "models")
	c.Assert(err, qt.IsNil)
	c.Assert(parsed.FeatureObjects, qt.DeepEquals, db.FeatureObjects)
	c.Assert(tableFacetsByName(parsed), qt.DeepEquals, map[string]schemaext.Facets{"rooms": db.Tables[0].Facets, "halls": {}})
}

func tableFacetsByName(db *schemamodel.Database) map[string]schemaext.Facets {
	facets := make(map[string]schemaext.Facets, len(db.Tables))
	for _, table := range db.Tables {
		facets[table.Name] = table.Facets
	}
	return facets
}

// ink is a second owner's object, for an owner that shares a placement with
// the paint owner.
type ink struct{}

const inkKind schemaext.Kind = "example.org/ink/ink"

func (*ink) Kind() schemaext.Kind   { return inkKind }
func (*ink) Clone() schemaext.Value { return &ink{} }
func (*ink) Equal(other schemaext.Value) bool {
	_, ok := other.(*ink)
	return ok
}

func inkProvider(encoder annotation.Encoder) engine.Provider {
	encode := func(payload schemaext.Payload) (json.RawMessage, error) { return json.Marshal(payload) }
	return engine.Provider{ID: "example.org/ink",
		Codecs: []schemaext.Codec{{Prototype: &ink{}, Representation: schemaext.Desired, Version: 1, Definition: json.RawMessage(`{"type":"object"}`),
			Clone: func(schemaext.Payload) (schemaext.Payload, error) { return &ink{}, nil }, Encode: encode, Canonical: encode,
			Decode: func(json.RawMessage) (schemaext.Payload, error) { return &ink{}, nil }}},
		Annotations: []annotation.Extension{{
			Owner: "example.org/ink",
			Directives: []annotation.Directive{{Name: "ptah:schema:ink", Scopes: []annotation.Scope{annotation.ScopeStruct},
				Attributes: []annotation.Attribute{{Name: "name", Description: "name.", Value: "string", Required: true}}}},
			Kinds:    []schemaext.Kind{inkKind},
			Decode:   func(annotation.Declaration) ([]annotation.Contribution, error) { return nil, nil },
			Encoder:  encoder,
			Encodes:  []schemaext.Kind{inkKind},
			Coverage: annotation.Unlimited(func() (schemaext.Coverage, error) { return schemaext.Coverage{}, nil }),
		}},
	}
}

// TestRender_OwnersShareAPlaceInTheOrderOfTheirNames pins that the directives
// two owners write at one placement follow the order of the owners' names,
// whichever provider was registered first, so a source does not change when
// a runtime lists its providers differently.
func TestRender_OwnersShareAPlaceInTheOrderOfTheirNames(t *testing.T) {
	c := qt.New(t)
	paint := paintProvider("example.org/paint", fixedWriter{{Directive: "ptah:schema:mural", Placement: annotation.AfterPrivileges,
		Attributes: []annotation.EncodedAttribute{{Name: "name", Value: "north"}}}})
	ink := inkProvider(fixedWriter{{Directive: "ptah:schema:ink", Placement: annotation.AfterPrivileges,
		Attributes: []annotation.EncodedAttribute{{Name: "name", Value: "black"}}}})
	runtime := must.Must(engine.New(paint, ink))

	files, err := goschematogo.Render(c.Context(), &schemamodel.Database{}, goschematogo.Options{Annotations: runtime.Annotations(), SingleFile: true})

	c.Assert(err, qt.IsNil)
	c.Assert(string(files[0].Data), qt.Equals, `// Code generated by ptah introspect; DO NOT EDIT.

package models

//ptah:schema:ink name="black"
//ptah:schema:mural name="north"
type PtahSchemaObjects struct{}
`)
}

// TestRender_RefusesADirectiveTheFrontendWouldNotReadBack pins the writer's
// own check of what an owner writes: the source is read back by the frontend's
// grammar, so a directive the grammar would refuse, or read differently, is
// refused before any file is written.
func TestRender_RefusesADirectiveTheFrontendWouldNotReadBack(t *testing.T) {
	tests := []struct {
		name      string
		directive annotation.EncodedDirective
		wantErr   string
	}{
		{name: "an attribute the directive does not declare", directive: annotation.EncodedDirective{Directive: "ptah:schema:mural",
			Placement: annotation.AfterSchemas, Attributes: []annotation.EncodedAttribute{{Name: "name", Value: "north"}, {Name: "colour", Value: "red"}}},
			wantErr: `.*an owner wrote attribute "colour" of directive "ptah:schema:mural", which the frontend does not read`},
		{name: "targets on a directive without a dialect scope", directive: annotation.EncodedDirective{Directive: "ptah:schema:mural",
			Placement: annotation.AfterSchemas, Attributes: []annotation.EncodedAttribute{{Name: "name", Value: "north"}}, Targets: []string{"postgres"}},
			wantErr: `.*an owner wrote attribute "dialects" of directive "ptah:schema:mural", which the frontend does not read`},
		{name: "a required attribute left out", directive: annotation.EncodedDirective{Directive: "ptah:schema:mural",
			Placement: annotation.AfterSchemas, Attributes: []annotation.EncodedAttribute{{Name: "wall", Value: "brick"}}},
			wantErr: `.*an owner wrote directive "ptah:schema:mural" without its required attribute "name"`},
		{name: "a directive the frontend reads on a field only", directive: annotation.EncodedDirective{Directive: "ptah:schema:stroke",
			Placement: annotation.AfterSchemas, Attributes: []annotation.EncodedAttribute{{Name: "name", Value: "wide"}}},
			wantErr: `.*an owner wrote directive "ptah:schema:stroke", which the frontend does not read on a struct`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(engine.New(paintProvider("example.org/paint", fixedWriter{test.directive})))

			files, err := goschematogo.Render(c.Context(), &schemamodel.Database{}, goschematogo.Options{Annotations: runtime.Annotations(), SingleFile: true})

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(files, qt.IsNil)
		})
	}
}

// TestRender_RequiresAnOwnerSelection pins that the export takes an explicit
// selection of owners: the zero set is refused rather than read as a decision
// to write no owner's directives.
func TestRender_RequiresAnOwnerSelection(t *testing.T) {
	c := qt.New(t)

	files, err := goschematogo.Render(c.Context(), &schemamodel.Database{}, goschematogo.Options{})

	c.Assert(err, qt.ErrorIs, annotation.ErrUnselected)
	c.Assert(files, qt.IsNil)
}

// TestRender_WithoutOwnersRefusesAnOwnersObject pins that a selection without
// owners writes no owner's directive and refuses the owner's object rather
// than leaving it out.
func TestRender_WithoutOwnersRefusesAnOwnersObject(t *testing.T) {
	c := qt.New(t)
	db := &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(tsschema.DesiredContinuousAggregateObject("", "hourly",
		tsschema.DesiredContinuousAggregate{Body: "SELECT 1"})))}

	files, err := goschematogo.Render(c.Context(), db, goschematogo.Options{Annotations: annotation.None(), SingleFile: true})

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `.*Go annotations cannot represent feature object .*hourly.*`)
	c.Assert(files, qt.IsNil)
}

// TestRender_TheBundledOwnersAgreeWithTheirDecoders pins that the bundled
// runtime's annotation set writes what its own decoders read: an export with
// it parses back with it.
func TestRender_TheBundledOwnersAgreeWithTheirDecoders(t *testing.T) {
	c := qt.New(t)
	db := &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(tsschema.DesiredContinuousAggregateObject("", "hourly",
		tsschema.DesiredContinuousAggregate{Body: "SELECT 1"})))}

	files, err := goschematogo.Render(c.Context(), db, goschematogo.Options{Annotations: builtintest.Annotations(), SingleFile: true})
	c.Assert(err, qt.IsNil)
	parsed, err := goschema.ParseSource(builtintest.Annotations(), files[0].Name, files[0].Data)

	c.Assert(err, qt.IsNil)
	c.Assert(parsed.FeatureObjects.Refs(), qt.DeepEquals, db.FeatureObjects.Refs())
}

// TestRender_AnOwnersTargetsAreItsDialects pins that the targets an owner
// scopes a directive to are written as the directive's dialects attribute,
// after the others: a frontend directive the owner reads by its target scope
// reaches the owner on the way back only through that attribute.
func TestRender_AnOwnersTargetsAreItsDialects(t *testing.T) {
	c := qt.New(t)
	rooms := annotation.Table{Schema: "app", Name: "rooms", Struct: "Room"}
	provider := paintProvider("example.org/paint", fixedWriter{{Directive: "ptah:schema:rls:policy", Placement: annotation.AfterTable,
		Table: &rooms, Attributes: []annotation.EncodedAttribute{{Name: "name", Value: "tenant"}, {Name: "table", Value: "app.rooms"}},
		Targets: []string{"postgres"}}})
	provider.Annotations[0].TargetScopes = []annotation.TargetScope{{Directive: "ptah:schema:rls:policy", Targets: []string{"postgres"}, Label: "PostgreSQL"}}
	runtime := must.Must(engine.New(provider))
	db := &schemamodel.Database{Tables: []schemamodel.Table{{StructName: "Room", Name: "rooms", Schema: "app"}}}

	files, err := goschematogo.Render(c.Context(), db, goschematogo.Options{Annotations: runtime.Annotations(), SingleFile: true})

	c.Assert(err, qt.IsNil)
	c.Assert(string(files[0].Data), qt.Equals, `// Code generated by ptah introspect; DO NOT EDIT.

package models

//ptah:schema:table name="rooms" schema="app"
//ptah:schema:rls:policy name="tenant" table="app.rooms" dialects="postgres"
type Room struct {
}
`)
}

// TestRender_HandsAnOwnerEverySettingOfItsModel pins that a facet on any
// declaration the owners are handed -- a primary key, a field, an index, a
// constraint, and a field or an index of a struct that declares no table --
// reaches the owner of its model, which refuses one it has no spelling for,
// rather than disappearing from the export.
func TestRender_HandsAnOwnerEverySettingOfItsModel(t *testing.T) {
	hypertable := must.Must(schemaext.NewFacets(&tsschema.DesiredHypertable{Column: "time"}))
	readings := []schemamodel.Table{{StructName: "Reading", Name: "readings"}}
	tests := []struct {
		name string
		db   *schemamodel.Database
	}{
		{name: "a primary key", db: &schemamodel.Database{Tables: []schemamodel.Table{{StructName: "Reading", Name: "readings", PrimaryKeyFacets: hypertable}}}},
		{name: "a field", db: &schemamodel.Database{Tables: readings,
			Fields: []schemamodel.Field{{StructName: "Reading", Name: "time", Type: "TIMESTAMPTZ", Facets: hypertable}}}},
		{name: "an index", db: &schemamodel.Database{Tables: readings,
			Indexes: []schemamodel.Index{{StructName: "Reading", Name: "i", Fields: []string{"time"}, Facets: hypertable}}}},
		{name: "a constraint", db: &schemamodel.Database{Tables: readings,
			Constraints: []schemamodel.Constraint{{StructName: "Reading", Name: "u", Type: "UNIQUE", Columns: []string{"time"}, Facets: hypertable}}}},
		{name: "a field of a struct without a table", db: &schemamodel.Database{
			Fields: []schemamodel.Field{{StructName: "Loose", Name: "time", Type: "TIMESTAMPTZ", Facets: hypertable}}}},
		{name: "an index of a struct without a table", db: &schemamodel.Database{
			Indexes: []schemamodel.Index{{StructName: "Loose", Name: "i", Fields: []string{"time"}, Facets: hypertable}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			files, err := goschematogo.Render(c.Context(), test.db, goschematogo.Options{Annotations: builtintest.Annotations(), SingleFile: true})

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `.*Go annotations cannot represent feature facet "ptah.run/timescaledb/hypertable"`)
			c.Assert(files, qt.IsNil)
		})
	}
}

// TestRender_RefusesAnOwnersModelWhereNoOwnerIsHandedIt pins that a facet of
// a model an owner encodes, on a declaration the owners are not handed, such
// as a function, is refused rather than left to an owner that never sees it.
func TestRender_RefusesAnOwnersModelWhereNoOwnerIsHandedIt(t *testing.T) {
	c := qt.New(t)
	db := &schemamodel.Database{Functions: []schemamodel.Function{{Name: "f", Body: "SELECT 1",
		Facets: must.Must(schemaext.NewFacets(&tsschema.DesiredHypertable{Column: "time"}))}}}

	files, err := goschematogo.Render(c.Context(), db, goschematogo.Options{Annotations: builtintest.Annotations(), SingleFile: true})

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `.*Go annotations cannot represent feature facet "ptah.run/timescaledb/hypertable"`)
	c.Assert(files, qt.IsNil)
}

// TestRender_ChecksTheSelectionBeforeTheSchema pins that a missing selection
// is refused before anything in the schema is: the refusal names what the
// caller forgot, not the first thing the schema could not say.
func TestRender_ChecksTheSelectionBeforeTheSchema(t *testing.T) {
	c := qt.New(t)
	db := &schemamodel.Database{Constraints: []schemamodel.Constraint{{Name: "fk", Type: "FOREIGN KEY", Table: "t",
		Columns: []string{"a", "b"}, ForeignTable: "u", ForeignColumns: []string{"a", "b"}, OnDelete: "SET NULL", OnDeleteColumns: []string{"b"}}}}

	files, err := goschematogo.Render(c.Context(), db, goschematogo.Options{SingleFile: true})

	c.Assert(err, qt.ErrorIs, annotation.ErrUnselected)
	c.Assert(files, qt.IsNil)
}

// recordingWriter records the declarations it is handed and writes nothing.
type recordingWriter struct{ handed *[]annotation.Slot }

func (w recordingWriter) EncodeAnnotations(_ context.Context, request annotation.EncodeRequest) (annotation.EncodeResult, error) {
	for _, slot := range request.Slots {
		*w.handed = append(*w.handed, slot.Slot)
	}
	return annotation.EncodeResult{}, nil
}

// TestRender_HandsAnOwnerEveryDeclarationOfItsModel pins which declarations
// an owner is handed, and in which order: each table and its primary key,
// then the fields, indexes and constraints of each table's struct, then those
// of a struct that declares no table.
func TestRender_HandsAnOwnerEveryDeclarationOfItsModel(t *testing.T) {
	c := qt.New(t)
	var handed []annotation.Slot
	runtime := must.Must(engine.New(paintProvider("example.org/paint", recordingWriter{handed: &handed})))
	teal := must.Must(schemaext.NewFacets(&shade{Name: "teal"}))
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Hall", Name: "halls", Facets: teal}, {StructName: "Room", Name: "rooms", Schema: "app", Facets: teal, PrimaryKeyFacets: teal}},
		Fields: []schemamodel.Field{{StructName: "Loose", Name: "x", Type: "TEXT", Facets: teal}, {StructName: "Room", Name: "id", Type: "INTEGER", Facets: teal}},
		Indexes: []schemamodel.Index{{StructName: "Room", Name: "i", Fields: []string{"id"}, Facets: teal},
			{StructName: "Loose", Name: "j", Fields: []string{"x"}, Facets: teal}},
		Constraints: []schemamodel.Constraint{{StructName: "Room", Name: "u", Type: "UNIQUE", Columns: []string{"id"}, Facets: teal}},
	}

	_, err := goschematogo.Render(c.Context(), db, goschematogo.Options{Annotations: runtime.Annotations(), SingleFile: true})

	c.Assert(err, qt.IsNil)
	rooms := annotation.Table{Schema: "app", Name: "rooms", Struct: "Room"}
	loose := annotation.Table{Struct: "Loose"}
	c.Assert(handed, qt.DeepEquals, []annotation.Slot{
		{Directive: "ptah:schema:table", Table: rooms},
		{Directive: "ptah:schema:table", Table: rooms, PrimaryKey: true},
		{Directive: "ptah:schema:table", Table: annotation.Table{Name: "halls", Struct: "Hall"}},
		{Directive: "ptah:schema:field", Table: rooms, Name: "id"},
		{Directive: "ptah:schema:index", Table: rooms, Name: "i"},
		{Directive: "ptah:schema:constraint", Table: rooms, Name: "u"},
		{Directive: "ptah:schema:field", Table: loose, Name: "x"},
		{Directive: "ptah:schema:index", Table: loose, Name: "j"},
	})
}

// slotWriter adds the paint owner's attribute to every declaration it is
// handed, at position, under name, or shade when name is empty.
type slotWriter struct {
	position annotation.Position
	name     string
}

func (w slotWriter) EncodeAnnotations(_ context.Context, request annotation.EncodeRequest) (annotation.EncodeResult, error) {
	var result annotation.EncodeResult
	for _, slot := range request.Slots {
		painted, _, err := schemaext.FacetAs[*shade](slot.Facets, shadeKind)
		if err != nil {
			return annotation.EncodeResult{}, err
		}
		result.Attributes = append(result.Attributes, annotation.SlotAttributes{Slot: slot.Slot, Position: w.position,
			Attributes: []annotation.EncodedAttribute{{Name: cmp.Or(w.name, "shade"), Value: painted.Name}}})
	}
	return result, nil
}

// shadingProvider is the paint owner adding a shade attribute to the table
// and the materialized view directives, and a key_shade attribute of a
// table's primary key, as writer writes them.
func shadingProvider(writer slotWriter) engine.Provider {
	provider := paintProvider("example.org/paint", writer)
	decode := func(name string) func(map[string]string) (schemaext.Facets, error) {
		return func(attributes map[string]string) (schemaext.Facets, error) {
			return schemaext.NewFacets(&shade{Name: attributes[name]})
		}
	}
	shadeAttribute := []annotation.Attribute{{Name: "shade", Description: "The shade.", Value: "string"}}
	provider.Annotations[0].Attributes = []annotation.DirectiveAttributes{
		{Directive: "ptah:schema:table", Attributes: shadeAttribute, Decode: decode("shade")},
		{Directive: "ptah:schema:table", Attributes: []annotation.Attribute{{Name: "key_shade", Description: "The key's shade.", Value: "string"}},
			Decode: decode("key_shade"), PrimaryKey: true},
		{Directive: "ptah:schema:matview", Attributes: shadeAttribute, Decode: decode("shade")},
	}
	return provider
}

// TestRender_AnOwnersAttributeOfAViewPrecedesItsDialects pins where the
// writer places an attribute an owner adds to a materialized view: after the
// directive's own attributes and before its dialects, and the source reads
// back to the same setting through the same owner.
func TestRender_AnOwnersAttributeOfAViewPrecedesItsDialects(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(engine.New(shadingProvider(slotWriter{})))
	db := &schemamodel.Database{MaterializedViews: []schemamodel.MaterializedView{{Name: "hourly", Body: "SELECT 1", Comment: "by hour",
		Dialects: []string{"postgres"}, Facets: must.Must(schemaext.NewFacets(&shade{Name: "teal"}))}}}

	files, err := goschematogo.Render(c.Context(), db, goschematogo.Options{Annotations: runtime.Annotations(), SingleFile: true})
	c.Assert(err, qt.IsNil)
	parsed, err := goschema.ParseSource(runtime.Annotations(), files[0].Name, files[0].Data)

	c.Assert(err, qt.IsNil)
	c.Assert(string(files[0].Data), qt.Contains,
		`//ptah:schema:matview name="hourly" body="SELECT 1" comment="by hour" shade="teal" dialects="postgres"`+"\n")
	c.Assert(parsed.MaterializedViews[0].Facets, qt.DeepEquals, db.MaterializedViews[0].Facets)
}

// TestRender_AnOwnersAttributeOfATable pins where the writer places an
// attribute an owner adds to a table: after the directive's own attributes,
// or after its platform properties where the owner asks for that, and the
// source reads back to the same setting through the same owner.
func TestRender_AnOwnersAttributeOfATable(t *testing.T) {
	for _, test := range []struct {
		name     string
		position annotation.Position
		want     string
	}{
		{"with the table's own", annotation.WithOwnAttributes,
			`//ptah:schema:table name="rooms" comment="by room" shade="teal" platform.mysql.engine="InnoDB"` + "\n"},
		{"after the platform properties", annotation.AfterProperties,
			`//ptah:schema:table name="rooms" comment="by room" platform.mysql.engine="InnoDB" shade="teal"` + "\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(engine.New(shadingProvider(slotWriter{position: test.position})))
			db := &schemamodel.Database{Tables: []schemamodel.Table{{StructName: "Room", Name: "rooms", Comment: "by room",
				Overrides: map[string]map[string]string{"mysql": {"engine": "InnoDB"}}, Facets: must.Must(schemaext.NewFacets(&shade{Name: "teal"}))}}}

			files, err := goschematogo.Render(c.Context(), db, goschematogo.Options{Annotations: runtime.Annotations(), SingleFile: true})
			c.Assert(err, qt.IsNil)
			parsed, err := goschema.ParseSource(runtime.Annotations(), files[0].Name, files[0].Data)

			c.Assert(err, qt.IsNil)
			c.Assert(string(files[0].Data), qt.Contains, test.want)
			c.Assert(parsed.Tables[0].Facets, qt.DeepEquals, db.Tables[0].Facets)
		})
	}
}

// TestRender_AnOwnersAttributeOfAPrimaryKey pins where the writer places an
// attribute an owner adds to a table's primary key: with the key's own
// attributes, before the column list, and the source reads back to the same
// setting of the key.
func TestRender_AnOwnersAttributeOfAPrimaryKey(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(engine.New(shadingProvider(slotWriter{name: "key_shade"})))
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Room", Name: "rooms", Comment: "by room", PrimaryKey: []string{"id"},
			PrimaryKeyComment: "lookup", PrimaryKeyFacets: must.Must(schemaext.NewFacets(&shade{Name: "teal"}))}},
		Fields: []schemamodel.Field{{StructName: "Room", FieldName: "ID", Name: "id", Type: "BIGINT"}},
	}

	files, err := goschematogo.Render(c.Context(), db, goschematogo.Options{Annotations: runtime.Annotations(), SingleFile: true})
	c.Assert(err, qt.IsNil)
	parsed, err := goschema.ParseSource(runtime.Annotations(), files[0].Name, files[0].Data)

	c.Assert(err, qt.IsNil)
	c.Assert(string(files[0].Data), qt.Contains,
		`//ptah:schema:table name="rooms" primary_key_comment="lookup" key_shade="teal" primary_key="id" comment="by room"`+"\n")
	c.Assert(parsed.Tables[0].PrimaryKeyFacets, qt.DeepEquals, db.Tables[0].PrimaryKeyFacets)
	c.Assert(parsed.Tables[0].Facets.IsZero(), qt.IsTrue)
}

// TestRender_RefusesAnOwnersAttributeWhereTheWriterPlacesNone pins that an
// attribute an owner adds where the writer has no place for it is refused
// rather than left out: after the platform properties of a directive that
// carries none, a primary key's among them, which shares the table's
// directive, and a platform property of a directive that reads none.
func TestRender_RefusesAnOwnersAttributeWhereTheWriterPlacesNone(t *testing.T) {
	painted := must.Must(schemaext.NewFacets(&shade{Name: "teal"}))
	for _, test := range []struct {
		name    string
		writer  slotWriter
		db      *schemamodel.Database
		wantErr string
	}{
		{"after the properties of a view", slotWriter{position: annotation.AfterProperties},
			&schemamodel.Database{MaterializedViews: []schemamodel.MaterializedView{{Name: "hourly", Body: "SELECT 1", Facets: painted}}},
			`.*an owner wrote attributes after the platform properties of matview "hourly", which has none`},
		{"after the properties of a primary key", slotWriter{position: annotation.AfterProperties, name: "key_shade"},
			&schemamodel.Database{Tables: []schemamodel.Table{{StructName: "Room", Name: "rooms", PrimaryKey: []string{"id"},
				PrimaryKeyFacets: painted}}},
			`.*an owner wrote attributes after the platform properties of the primary key of table "rooms", which has none`},
		{"a platform property of a view", slotWriter{name: "platform.mysql.shade"},
			&schemamodel.Database{MaterializedViews: []schemamodel.MaterializedView{{Name: "hourly", Body: "SELECT 1", Facets: painted}}},
			`.*an owner wrote attribute "platform.mysql.shade" of matview "hourly", which the frontend does not read`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(engine.New(shadingProvider(test.writer)))
			db := test.db

			files, err := goschematogo.Render(c.Context(), db, goschematogo.Options{Annotations: runtime.Annotations(), SingleFile: true})

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(files, qt.IsNil)
		})
	}
}

// TestRender_OwnersPoliciesFollowTheTablesOwn pins the order beside a table:
// the table's own row-level security policies, then the owners' policies in
// the order of the owners' names, the ClickHouse owner's row policy before
// the SQL Server owner's security policy.
func TestRender_OwnersPoliciesFollowTheTablesOwn(t *testing.T) {
	c := qt.New(t)
	security := must.Must(mssqlschema.DesiredSecurityPolicyObject(mssqlschema.SecurityPolicyRef("rls", "tenancy"), mssqlschema.DesiredSecurityPolicy{
		Predicates: []mssqlschema.Predicate{{Type: mssqlschema.Filter, Function: mssqlschema.ObjectName{Schema: "rls", Name: "fn"},
			Arguments: []string{"tenant_id"}, Table: mssqlschema.ObjectName{Schema: "dbo", Name: "orders"}}}}))
	security.Targets = []string{"sqlserver"}
	db := &schemamodel.Database{
		Tables:      []schemamodel.Table{{StructName: "Order", Name: "orders"}},
		RLSPolicies: []schemamodel.RLSPolicy{{StructName: "Order", Name: "common", Table: "orders", UsingExpression: "true"}},
		FeatureObjects: must.Must(schemaext.NewObjects(security,
			must.Must(chschema.DesiredRowPolicyObject(chschema.RowPolicyRef("", "orders", "tenant"), chschema.DesiredRowPolicy{})))),
	}

	files, err := goschematogo.Render(c.Context(), db, goschematogo.Options{Annotations: builtintest.Annotations(), SingleFile: true})

	c.Assert(err, qt.IsNil)
	c.Assert(string(files[0].Data), qt.Contains, `//ptah:schema:rls:policy name="common" table="orders" using="true"
//ptah:schema:rowpolicy name="tenant" table="orders"
//ptah:schema:rls:policy name="rls.tenancy" table="orders" using="[rls].[fn](tenant_id)" dialects="sqlserver"
//ptah:schema:table name="orders"
`)
}
