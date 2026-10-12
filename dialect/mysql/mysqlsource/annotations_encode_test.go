package mysqlsource_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/annotation"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mysql/mysqlschema"
	"ptah.run/dialect/mysql/mysqlsource"
)

var docs = annotation.Table{Name: "docs", Struct: "Doc"}

// Declarations of the docs table the owner is handed.
var (
	bioField   = annotation.Slot{Directive: "ptah:schema:field", Table: docs, Name: "bio"}
	bioIndex   = annotation.Slot{Directive: "ptah:schema:index", Table: docs, Name: "ft_bio"}
	codeUnique = annotation.Slot{Directive: "ptah:schema:constraint", Table: docs, Name: "uq_code"}
	docsPK     = annotation.Slot{Directive: "ptah:schema:constraint", Table: docs, Name: "pk_docs", PrimaryKey: true}
	docsKey    = annotation.Slot{Directive: "ptah:schema:table", Table: docs, PrimaryKey: true}
	docsTable  = annotation.Slot{Directive: "ptah:schema:table", Table: docs}
)

func encodeSettings(ctx context.Context, slots ...annotation.SlotFacets) (annotation.EncodeResult, error) {
	return mysqlsource.Annotations().Encoder.EncodeAnnotations(ctx, annotation.EncodeRequest{Tables: annotation.Tables{docs}, Slots: slots})
}

// scoped is facets with the scope of kind narrowed to targets.
func scoped(facets schemaext.Facets, kind schemaext.Kind, targets ...string) schemaext.Facets {
	return must.Must(facets.WithTargetScope(kind, targets...))
}

func TestEncodeAnnotations_HappyPath(t *testing.T) {
	settings := mysqlschema.ColumnSettings{Charset: "latin1", OnUpdate: "CURRENT_TIMESTAMP"}
	parser := must.Must(mysqlschema.WithIndexOptions(schemaext.Facets{}, mysqlschema.DesiredIndex{Parser: "ngram"}))
	tests := []struct {
		name string
		slot annotation.SlotFacets
		want []annotation.SlotAttributes
	}{
		{name: "column settings, once for each MySQL-family target",
			slot: annotation.SlotFacets{Slot: bioField, Facets: must.Must(mysqlschema.WithColumnSettings(schemaext.Facets{}, settings))},
			want: []annotation.SlotAttributes{{Slot: bioField, Attributes: []annotation.EncodedAttribute{
				{Name: "platform.mariadb.charset", Value: "latin1"}, {Name: "platform.mariadb.on_update", Value: "CURRENT_TIMESTAMP"},
				{Name: "platform.mysql.charset", Value: "latin1"}, {Name: "platform.mysql.on_update", Value: "CURRENT_TIMESTAMP"}}}}},
		{name: "column settings bound to one target",
			slot: annotation.SlotFacets{Slot: bioField, Facets: scoped(must.Must(schemaext.NewFacets(&mysqlschema.DesiredColumnSettings{Charset: "latin1"})),
				mysqlschema.ColumnSettingsKind, "mysql")},
			want: []annotation.SlotAttributes{{Slot: bioField, Attributes: []annotation.EncodedAttribute{{Name: "platform.mysql.charset", Value: "latin1"}}}}},
		{name: "column settings bound to none",
			slot: annotation.SlotFacets{Slot: bioField, Facets: must.Must(schemaext.NewFacets(&mysqlschema.ObservedColumnSettings{OnUpdate: "NOW()"}))},
			want: []annotation.SlotAttributes{{Slot: bioField, Attributes: []annotation.EncodedAttribute{
				{Name: "platform.mariadb.on_update", Value: "NOW()"}, {Name: "platform.mysql.on_update", Value: "NOW()"}}}}},
		{name: "an index's hint with its own attributes, its parser after its properties",
			slot: annotation.SlotFacets{Slot: bioIndex, Facets: must.Must(mysqlschema.WithIndexBlockSize(parser, 8))},
			want: []annotation.SlotAttributes{
				{Slot: bioIndex, Attributes: []annotation.EncodedAttribute{{Name: "key_block_size", Value: "8"}}},
				{Slot: bioIndex, Position: annotation.AfterProperties, Attributes: []annotation.EncodedAttribute{
					{Name: "platform.mariadb.parser", Value: "ngram"}, {Name: "platform.mysql.parser", Value: "ngram"}}},
			}},
		{name: "a parser bound to one target",
			slot: annotation.SlotFacets{Slot: bioIndex, Facets: scoped(parser, mysqlschema.IndexKind, "mariadb")},
			want: []annotation.SlotAttributes{{Slot: bioIndex, Position: annotation.AfterProperties,
				Attributes: []annotation.EncodedAttribute{{Name: "platform.mariadb.parser", Value: "ngram"}}}}},
		{name: "a primary key's hint",
			slot: annotation.SlotFacets{Slot: docsKey, Facets: must.Must(mysqlschema.WithIndexBlockSize(schemaext.Facets{}, 4))},
			want: []annotation.SlotAttributes{{Slot: docsKey, Attributes: []annotation.EncodedAttribute{{Name: "primary_key_block_size", Value: "4"}}}}},
		{name: "a PRIMARY KEY constraint's hint",
			slot: annotation.SlotFacets{Slot: docsPK, Facets: must.Must(mysqlschema.WithIndexBlockSize(schemaext.Facets{}, 2))},
			want: []annotation.SlotAttributes{{Slot: docsPK, Attributes: []annotation.EncodedAttribute{{Name: "key_block_size", Value: "2"}}}}},
		{name: "a PRIMARY KEY constraint's observed hint",
			slot: annotation.SlotFacets{Slot: docsPK,
				Facets: must.Must(mysqlschema.WithObservedIndexBlockSize(schemaext.Facets{}, mysqlschema.ObservedIndexBlockSize{KeyBlockSize: 16, Retained: true}))},
			want: []annotation.SlotAttributes{{Slot: docsPK, Attributes: []annotation.EncodedAttribute{{Name: "key_block_size", Value: "16"}}}}},
		{name: "a hint of zero",
			slot: annotation.SlotFacets{Slot: docsKey, Facets: must.Must(schemaext.NewFacets(&mysqlschema.DesiredIndexBlockSize{}))}},
		{name: "an empty parser",
			slot: annotation.SlotFacets{Slot: bioIndex, Facets: must.Must(schemaext.NewFacets(&mysqlschema.DesiredIndex{}))}},
		{name: "a declaration holding nothing", slot: annotation.SlotFacets{Slot: docsTable}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := encodeSettings(c.Context(), test.slot)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Attributes, qt.DeepEquals, test.want)
			c.Assert(result.Directives, qt.IsNil)
		})
	}
}

// TestEncodeAnnotations_EverySlotInOrder pins that the owner answers each
// declaration it is handed, in the order it is handed them.
func TestEncodeAnnotations_EverySlotInOrder(t *testing.T) {
	c := qt.New(t)
	hint := must.Must(mysqlschema.WithIndexBlockSize(schemaext.Facets{}, 2))

	result, err := encodeSettings(c.Context(), annotation.SlotFacets{Slot: docsPK, Facets: hint}, annotation.SlotFacets{Slot: docsKey, Facets: hint})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Attributes, qt.DeepEquals, []annotation.SlotAttributes{
		{Slot: docsPK, Attributes: []annotation.EncodedAttribute{{Name: "key_block_size", Value: "2"}}},
		{Slot: docsKey, Attributes: []annotation.EncodedAttribute{{Name: "primary_key_block_size", Value: "2"}}},
	})
}

func TestEncodeAnnotations_FailurePath(t *testing.T) {
	hint := must.Must(mysqlschema.WithIndexBlockSize(schemaext.Facets{}, 8))
	parser := must.Must(mysqlschema.WithIndexOptions(schemaext.Facets{}, mysqlschema.DesiredIndex{Parser: "ngram"}))
	tests := []struct {
		name    string
		slot    annotation.SlotFacets
		wantIs  error
		wantErr string
	}{
		{name: "a hint of a field", slot: annotation.SlotFacets{Slot: bioField, Facets: hint},
			wantIs: ptaherr.ErrUnsupportedFeature, wantErr: `.*Go annotations cannot represent feature facet "ptah.run/mysql/index-block-size"`},
		{name: "a hint of a table", slot: annotation.SlotFacets{Slot: docsTable, Facets: hint},
			wantIs: ptaherr.ErrUnsupportedFeature, wantErr: `.*Go annotations cannot represent feature facet "ptah.run/mysql/index-block-size"`},
		{name: "a hint of a UNIQUE constraint", slot: annotation.SlotFacets{Slot: codeUnique, Facets: hint},
			wantIs: ptaherr.ErrUnsupportedFeature, wantErr: `.*Go annotations cannot represent feature facet "ptah.run/mysql/index-block-size"`},
		{name: "a parser of a PRIMARY KEY constraint", slot: annotation.SlotFacets{Slot: docsPK, Facets: parser},
			wantIs: ptaherr.ErrUnsupportedFeature, wantErr: `.*Go annotations cannot represent feature facet "ptah.run/mysql/index"`},
		{name: "a parser of a constraint", slot: annotation.SlotFacets{Slot: codeUnique, Facets: parser},
			wantIs: ptaherr.ErrUnsupportedFeature, wantErr: `.*Go annotations cannot represent feature facet "ptah.run/mysql/index"`},
		{name: "a parser of a primary key", slot: annotation.SlotFacets{Slot: docsKey, Facets: parser},
			wantIs: ptaherr.ErrUnsupportedFeature, wantErr: `.*Go annotations cannot represent feature facet "ptah.run/mysql/index"`},
		{name: "column settings of an index",
			slot:   annotation.SlotFacets{Slot: bioIndex, Facets: must.Must(schemaext.NewFacets(&mysqlschema.DesiredColumnSettings{Charset: "latin1"}))},
			wantIs: ptaherr.ErrUnsupportedFeature, wantErr: `.*Go annotations cannot represent feature facet "ptah.run/mysql/column-settings"`},
		{name: "an index's observed hint",
			slot: annotation.SlotFacets{Slot: bioIndex,
				Facets: must.Must(mysqlschema.WithObservedIndexBlockSize(schemaext.Facets{}, mysqlschema.ObservedIndexBlockSize{KeyBlockSize: 8}))},
			wantIs: schemaext.ErrInvalidValue, wantErr: `.*index-block-size.*`},
		{name: "an index's hint over the limit",
			slot:   annotation.SlotFacets{Slot: bioIndex, Facets: must.Must(schemaext.NewFacets(&mysqlschema.DesiredIndexBlockSize{KeyBlockSize: 1 << 32}))},
			wantIs: schemaext.ErrInvalidValue, wantErr: `.*KEY_BLOCK_SIZE.*`},
		{name: "a constraint's hint over the limit",
			slot:   annotation.SlotFacets{Slot: docsPK, Facets: must.Must(schemaext.NewFacets(&mysqlschema.DesiredIndexBlockSize{KeyBlockSize: 1 << 32}))},
			wantIs: schemaext.ErrInvalidValue, wantErr: `.*KEY_BLOCK_SIZE.*`},
		{name: "a primary key's hint over the limit",
			slot:   annotation.SlotFacets{Slot: docsKey, Facets: must.Must(schemaext.NewFacets(&mysqlschema.DesiredIndexBlockSize{KeyBlockSize: 1 << 32}))},
			wantIs: schemaext.ErrInvalidValue, wantErr: `.*KEY_BLOCK_SIZE.*`},
		{name: "an observed parser",
			slot:   annotation.SlotFacets{Slot: bioIndex, Facets: must.Must(schemaext.NewFacets(&mysqlschema.ObservedIndex{Parser: "ngram"}))},
			wantIs: schemaext.ErrInvalidValue, wantErr: `.*ptah.run/mysql/index.*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := encodeSettings(c.Context(), test.slot)

			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(result, qt.DeepEquals, annotation.EncodeResult{})
		})
	}
}

// TestEncodeAnnotations_RefusesACanceledRequest pins that a canceled context
// stops the owner before it writes anything.
func TestEncodeAnnotations_RefusesACanceledRequest(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(c.Context())
	cancel()

	result, err := encodeSettings(ctx, annotation.SlotFacets{Slot: docsKey, Facets: must.Must(mysqlschema.WithIndexBlockSize(schemaext.Facets{}, 8))})

	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result, qt.DeepEquals, annotation.EncodeResult{})
}

// TestAnnotations_EncodesWhatItDecodes pins that the owner writes each model
// whose Go annotations it reads.
func TestAnnotations_EncodesWhatItDecodes(t *testing.T) {
	c := qt.New(t)
	extension := mysqlsource.Annotations()

	c.Assert(extension.Encodes, qt.DeepEquals, extension.Kinds)
	c.Assert(extension.Encoder, qt.IsNotNil)
}
