package mysqlsource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/hclext"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mysql/mysqlschema"
	"ptah.run/dialect/mysql/mysqlsource"
)

// facets holds values.
func facets(values ...schemaext.Value) schemaext.Facets {
	return must.Must(schemaext.NewFacets(values...))
}

// misread is a value under a kind of the owner that is not the owner's
// model of it.
type misread struct {
	kind schemaext.Kind
}

func (m *misread) Kind() schemaext.Kind { return m.kind }

func (m *misread) Clone() schemaext.Value { return new(*m) }

func (m *misread) Equal(other schemaext.Value) bool {
	o, ok := other.(*misread)
	return ok && *o == *m
}

func text(name, value string) hclext.EncodedAttribute {
	return hclext.EncodedAttribute{Name: name, Value: hclext.Value{Kind: hclext.String, Text: value}}
}

// TestHCL_EncodeHCL_HappyPath writes a table's options, a column's settings
// and an index's parser, declared or read, as the attributes of the writer's
// blocks: the auto-increment value as a number and ON UPDATE as SQL text the
// writer quotes. The block size of an index or a key is a loss, as are column
// settings the owner cannot read. A block whose options hold nothing to
// write, or that cannot be read, takes nothing.
func TestHCL_EncodeHCL_HappyPath(t *testing.T) {
	c := qt.New(t)
	table := hclext.Table{Schema: "app", Name: "t"}
	slot := func(kind hclext.SlotKind, name string) hclext.Slot {
		return hclext.Slot{Kind: kind, Table: table, Name: name}
	}
	blockSize := must.Must(mysqlschema.WithIndexBlockSize(schemaext.Facets{}, 8))
	request := hclext.EncodeRequest{
		Tables: []hclext.TableFacets{
			{Table: table, Facets: facets(&mysqlschema.DesiredTable{Engine: "InnoDB", AutoIncrement: "100", Charset: "utf8mb4"})},
			{Table: hclext.Table{Name: "read"}, Facets: facets(&mysqlschema.ObservedTable{Charset: "latin1"})},
			{Table: hclext.Table{Name: "empty"}, Facets: facets(&mysqlschema.DesiredTable{})},
			{Table: hclext.Table{Name: "misread"}, Facets: facets(&misread{kind: mysqlschema.TableKind})},
		},
		Columns: []hclext.SlotFacets{
			{Slot: slot(hclext.SlotColumn, "ts"), Facets: must.Must(mysqlschema.WithColumnSettings(schemaext.Facets{},
				mysqlschema.ColumnSettings{Charset: "utf8mb4", OnUpdate: `CURRENT_TIMESTAMP`}))},
			{Slot: slot(hclext.SlotColumn, "read"), Facets: must.Must(mysqlschema.WithObservedColumnSettings(schemaext.Facets{},
				mysqlschema.ColumnSettings{Charset: "latin1"}))},
			{Slot: slot(hclext.SlotColumn, "misread"), Facets: facets(&misread{kind: mysqlschema.ColumnSettingsKind})},
		},
		Indexes: []hclext.SlotFacets{
			{Slot: slot(hclext.SlotIndex, "ft"), Facets: must.Must(mysqlschema.WithIndexOptions(blockSize, mysqlschema.DesiredIndex{Parser: "ngram"}))},
			{Slot: slot(hclext.SlotIndex, "read"), Facets: facets(&mysqlschema.ObservedIndex{Parser: "mecab"})},
			{Slot: slot(hclext.SlotIndex, "misread"), Facets: facets(&misread{kind: mysqlschema.IndexKind})},
		},
		Keys: []hclext.SlotFacets{
			{Slot: hclext.Slot{Kind: hclext.SlotKey, Table: table, Primary: true}, Facets: blockSize},
			{Slot: slot(hclext.SlotKey, "pk"), Facets: blockSize},
			{Slot: slot(hclext.SlotKey, "none"), Facets: must.Must(mysqlschema.WithIndexBlockSize(schemaext.Facets{}, 0))},
		},
	}

	result, err := mysqlsource.HCL().Encoder.EncodeHCL(c.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Blocks, qt.IsNil)
	c.Assert(result.Header, qt.IsNil)
	keyLoss := []string{"the primary key KEY_BLOCK_SIZE option cannot be represented in HCL schema output"}
	c.Assert(result.Attributes, qt.DeepEquals, []hclext.SlotAttributes{
		{Slot: hclext.Slot{Kind: hclext.SlotTable, Table: table}, Attributes: []hclext.EncodedAttribute{text("engine", "InnoDB"), text("charset", "utf8mb4"),
			{Name: "auto_increment", Value: hclext.Value{Kind: hclext.Number, Text: "100"}}}},
		{Slot: hclext.Slot{Kind: hclext.SlotTable, Table: hclext.Table{Name: "read"}}, Attributes: []hclext.EncodedAttribute{text("charset", "latin1")}},
		{Slot: slot(hclext.SlotColumn, "ts"), Attributes: []hclext.EncodedAttribute{
			{Name: "on_update", Value: hclext.Value{Kind: hclext.SQL, Text: "CURRENT_TIMESTAMP"}}, text("charset", "utf8mb4")}},
		{Slot: slot(hclext.SlotColumn, "read"), Attributes: []hclext.EncodedAttribute{text("charset", "latin1")}},
		{Slot: slot(hclext.SlotColumn, "misread"), Losses: []string{`column settings are not represented in HCL: invalid feature value: ` +
			`facet "ptah.run/mysql/column-settings" has unexpected type *mysqlsource_test.misread`}},
		{Slot: slot(hclext.SlotIndex, "ft"), Attributes: []hclext.EncodedAttribute{text("parser", "ngram")},
			Losses: []string{"the index KEY_BLOCK_SIZE hint cannot be represented in HCL schema output; applying this HCL removes the hint"}},
		{Slot: slot(hclext.SlotIndex, "read"), Attributes: []hclext.EncodedAttribute{text("parser", "mecab")}},
		{Slot: hclext.Slot{Kind: hclext.SlotKey, Table: table, Primary: true}, Losses: keyLoss},
		{Slot: slot(hclext.SlotKey, "pk"), Losses: keyLoss},
	})
}

// TestHCL_EncodesTheOwnersKinds writes every kind the owner writes into the
// writer's blocks, and reads no block.
func TestHCL_EncodesTheOwnersKinds(t *testing.T) {
	c := qt.New(t)

	extension := mysqlsource.HCL()

	c.Assert(extension.Kinds, qt.DeepEquals, []schemaext.Kind{mysqlschema.TableKind, mysqlschema.ColumnSettingsKind, mysqlschema.IndexKind,
		mysqlschema.IndexBlockSizeKind})
	c.Assert(extension.Blocks, qt.IsNil)
}
