package ydbexternal_test

import (
	"encoding/json"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbexternal"
	"ptah.run/engine"
	"ptah.run/internal/ydbpath"
)

func fullSource() ydbexternal.DataSource {
	return ydbexternal.DataSource{SourceType: "PostgreSQL", Location: "pg:5432", AuthMethod: "BASIC",
		Options: map[string]string{"DATABASE_NAME": "app", "LOGIN": "reader", "PASSWORD_SECRET_PATH": "ext/pw"}}
}

func fullTable() ydbexternal.Table {
	return ydbexternal.Table{DataSource: "ext/bucket", Location: "e/",
		Columns: []ydbexternal.Column{{Name: "id", Type: "Int64", NotNull: true}, {Name: "the kind", Type: "Utf8"}},
		Options: map[string]string{"FORMAT": "csv_with_names", "CSV_DELIMITER": " "}}
}

// TestModelTransport_KeepsEverySetting round-trips each model through the
// registered codecs: every setting, every column and the holder survive,
// and a value with surrounding space keeps it.
func TestModelTransport_KeepsEverySetting(t *testing.T) {
	tests := []struct {
		name           string
		representation schemaext.Representation
		object         schemaext.Object
	}{
		{name: "a declared source", representation: schemaext.Desired, object: ydbexternal.DesiredSourceObject("ext", "pg.v1", "Warehouse", fullSource())},
		{name: "an observed source", representation: schemaext.Observed, object: ydbexternal.ObservedSourceObject("ext", "pg.v1", fullSource())},
		{name: "a declared table", representation: schemaext.Desired, object: ydbexternal.DesiredTableObject("", "events", "Event", fullTable())},
		{name: "an observed table", representation: schemaext.Observed, object: ydbexternal.ObservedTableObject("", "events", fullTable())},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(engine.New(engine.Provider{ID: "ptah.run/ydb", Codecs: ydbexternal.Codecs()}))
			objects := must.Must(schemaext.NewObjects(test.object))

			data, err := runtime.Codecs().EncodeObjects(t.Context(), test.representation, objects)
			c.Assert(err, qt.IsNil)
			decoded, err := runtime.Codecs().DecodeObjects(t.Context(), test.representation, data)

			c.Assert(err, qt.IsNil)
			values, err := decoded.All()
			c.Assert(err, qt.IsNil)
			c.Assert(values, qt.DeepEquals, []schemaext.Object{test.object})
		})
	}
}

// TestModelClone_IsIndependent changes a clone's options and columns and
// leaves the original as it was.
func TestModelClone_IsIndependent(t *testing.T) {
	c := qt.New(t)
	source := &ydbexternal.DesiredSource{Spec: fullSource()}
	table := &ydbexternal.ObservedTable{Spec: fullTable()}

	sourceClone := source.Clone().(*ydbexternal.DesiredSource)
	sourceClone.Spec.Options["LOGIN"] = "writer"
	tableClone := table.Clone().(*ydbexternal.ObservedTable)
	tableClone.Spec.Columns[0].Name = "other"
	tableClone.Spec.Options["FORMAT"] = "json_each_row"

	c.Assert(source, qt.DeepEquals, &ydbexternal.DesiredSource{Spec: fullSource()})
	c.Assert(table, qt.DeepEquals, &ydbexternal.ObservedTable{Spec: fullTable()})
}

// TestModelCodecs_RefuseALossyWire refuses a document that would decode to
// another object than the one written: an unknown field, a null, a missing
// spec, a holder on an observation, an option name a declaration does not
// write, and a table with no column.
func TestModelCodecs_RefuseALossyWire(t *testing.T) {
	tests := []struct {
		name  string
		codec int
		input string
	}{
		{name: "an unknown field", codec: 0, input: `{"spec":{"source_type":"ObjectStorage","auth_method":"NONE"},"references":[]}`},
		{name: "a null document", codec: 1, input: `null`},
		{name: "no spec", codec: 0, input: `{"struct_name":"S"}`},
		{name: "a holder on an observation", codec: 1, input: `{"spec":{"source_type":"ObjectStorage","auth_method":"NONE"},"struct_name":"S"}`},
		{name: "no auth method", codec: 0, input: `{"spec":{"source_type":"ObjectStorage"}}`},
		{name: "a lower-case option", codec: 1, input: `{"spec":{"source_type":"ObjectStorage","auth_method":"NONE","options":{"login":"u"}}}`},
		{name: "a reserved option", codec: 0, input: `{"spec":{"source_type":"ObjectStorage","auth_method":"NONE","options":{"LOCATION":"x"}}}`},
		{name: "a table with no column", codec: 2, input: `{"spec":{"data_source":"s","location":"e/","columns":[]}}`},
		{name: "a table option naming its source", codec: 3, input: `{"spec":{"data_source":"s","location":"e/","columns":[{"name":"id","type":"Int64"}],"options":{"DATA_SOURCE":"t"}}}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			value, err := ydbexternal.Codecs()[test.codec].Decode(json.RawMessage(test.input))
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(value, qt.IsNil)
		})
	}
}

// TestValidateIdentity_FailurePath refuses an identity that is not an
// external object's directory and leaf relative to the database root.
func TestValidateIdentity_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		ref  objectidentity.ID
	}{
		{name: "a slash in the leaf", ref: ydbexternal.SourceRef("", "ext/s3")},
		{name: "an absolute directory", ref: ydbexternal.TableRef("/ext", "events")},
		{name: "a parent directory", ref: ydbexternal.TableRef("../ext", "events")},
		{name: "an unclean directory", ref: ydbexternal.SourceRef("ext//b", "s3")},
		{name: "an empty leaf", ref: ydbexternal.SourceRef("ext", "")},
		{name: "another kind", ref: objectidentity.NewBuilder(identifier.ForDialect("ydb")).Table("events")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbexternal.ValidateIdentity(test.ref), qt.ErrorIs, schemaext.ErrInvalidValue)
		})
	}
}

// TestParsePath reads an external object's path relative to the database
// root, a dot kept in its segment, and refuses one written from the server
// root.
func TestParsePath(t *testing.T) {
	c := qt.New(t)
	ref, err := ydbexternal.ParsePath(ydbexternal.TableKind, "ext/events.v1")
	c.Assert(err, qt.IsNil)
	c.Assert(ref, qt.DeepEquals, ydbexternal.TableRef("ext", "events.v1"))
	_, err = ydbexternal.ParsePath(ydbexternal.SourceKind, "/local/ext/s3")
	c.Assert(err, qt.ErrorIs, ydbpath.ErrAbsolute)
	_, err = ydbexternal.ParsePath(ydbexternal.SourceKind, "ext/")
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
}

// TestResolveSource reads the data source an external table names against the
// database root: a relative path as it is, an absolute one under the root,
// and nothing for a path outside it or an absolute one with no root.
func TestResolveSource(t *testing.T) {
	tests := []struct {
		name, root, written string
		want                objectidentity.ID
		found               bool
	}{
		{name: "relative", written: "ext/s3", want: ydbexternal.SourceRef("ext", "s3"), found: true},
		{name: "absolute under the root", root: "/local", written: "/local/ext/s3", want: ydbexternal.SourceRef("ext", "s3"), found: true},
		{name: "outside the root", root: "/local", written: "/other/s3"},
		{name: "absolute with no root", written: "/local/ext/s3"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ref, found := ydbexternal.ResolveSource(test.root, test.written)
			c.Assert(found, qt.Equals, test.found)
			c.Assert(ref, qt.DeepEquals, test.want)
		})
	}
}

// TestDeclare_HappyPath adds each object under its path, with slashes around
// the directory dropped and the holder kept.
func TestDeclare_HappyPath(t *testing.T) {
	c := qt.New(t)
	objects, err := ydbexternal.DeclareSource(schemaext.Objects{}, " /ext/ ", " s3 ", "Storage", fullSource())
	c.Assert(err, qt.IsNil)
	objects, err = ydbexternal.DeclareTable(objects, "ext", "events", "Event", fullTable())
	c.Assert(err, qt.IsNil)
	all, err := objects.All()
	c.Assert(err, qt.IsNil)
	c.Assert(all, qt.DeepEquals, []schemaext.Object{
		ydbexternal.DesiredSourceObject("ext", "s3", "Storage", fullSource()),
		ydbexternal.DesiredTableObject("ext", "events", "Event", fullTable()),
	})
}

// TestDeclareSource_FailurePath refuses a data source a declaration cannot
// carry, naming the attribute, and leaves the objects as they were.
func TestDeclareSource_FailurePath(t *testing.T) {
	tests := []struct {
		name, schema, leaf string
		spec               ydbexternal.DataSource
		attribute          string
		wantErr            string
	}{
		{name: "no name", schema: "ext", leaf: " ", spec: fullSource(), attribute: ydbexternal.AttributeName,
			wantErr: "an external object needs a name"},
		{name: "a slash in the name", leaf: "ext/s3", spec: fullSource(), attribute: ydbexternal.AttributeName,
			wantErr: `"ext/s3" holds a slash; name the directory with schema`},
		{name: "a parent directory", schema: "../ext", leaf: "s3", spec: fullSource(), attribute: ydbexternal.AttributeSchema,
			wantErr: `"../ext" is not a directory path relative to the database root`},
		{name: "no source type", leaf: "s3", spec: ydbexternal.DataSource{AuthMethod: "NONE"}, attribute: ydbexternal.AttributeSourceType,
			wantErr: "a data source needs a source_type, such as ObjectStorage or PostgreSQL"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			objects, err := ydbexternal.DeclareSource(schemaext.Objects{}, test.schema, test.leaf, "", test.spec)
			c.Assert(err, qt.ErrorMatches, "invalid "+test.attribute+": "+test.wantErr)
			declaration, ok := errors.AsType[*ydbexternal.DeclarationError](err)
			c.Assert(ok, qt.IsTrue)
			c.Assert(declaration.Attribute, qt.Equals, test.attribute)
			c.Assert(objects.Len(), qt.Equals, 0)
		})
	}
}

// TestDeclareTable_FailurePath refuses an external table a declaration cannot
// carry, naming the attribute.
func TestDeclareTable_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		spec      ydbexternal.Table
		attribute string
		wantErr   string
	}{
		{name: "no data source", spec: ydbexternal.Table{Location: "e/", Columns: fullTable().Columns}, attribute: ydbexternal.AttributeDataSource,
			wantErr: "an external table needs a data_source, the path of the data source it reads"},
		{name: "no location", spec: ydbexternal.Table{DataSource: "s3", Columns: fullTable().Columns}, attribute: ydbexternal.AttributeLocation,
			wantErr: "an external table needs a location"},
		{name: "no column", spec: ydbexternal.Table{DataSource: "s3", Location: "e/"}, attribute: ydbexternal.AttributeColumns,
			wantErr: "an external table needs a column"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			objects, err := ydbexternal.DeclareTable(schemaext.Objects{}, "ext", "events", "", test.spec)
			c.Assert(err, qt.ErrorMatches, "invalid "+test.attribute+": "+test.wantErr)
			declaration, ok := errors.AsType[*ydbexternal.DeclarationError](err)
			c.Assert(ok, qt.IsTrue)
			c.Assert(declaration.Attribute, qt.Equals, test.attribute)
			c.Assert(objects.Len(), qt.Equals, 0)
		})
	}
}

// TestDeclare_RefusesASecondDeclaration refuses a second declaration of one
// path with a DuplicateError that wraps ErrDuplicate.
func TestDeclare_RefusesASecondDeclaration(t *testing.T) {
	c := qt.New(t)
	declared := must.Must(ydbexternal.DeclareSource(schemaext.Objects{}, "ext", "s3", "", fullSource()))

	objects, err := ydbexternal.DeclareSource(declared, "/ext", "s3", "Other", fullSource())

	c.Assert(err, qt.ErrorMatches, "external data source ext/s3 is declared twice")
	c.Assert(err, qt.ErrorIs, schemaext.ErrDuplicate)
	c.Assert(objects.Equal(declared), qt.IsTrue)
}
