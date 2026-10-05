package ydb_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
	"google.golang.org/protobuf/encoding/protowire"

	"ptah.run/core/platform/capability"
	ydbschema "ptah.run/internal/dbschema/ydb"
)

// fullTextMessage is one length-delimited protocol field.
func fullTextMessage(number protowire.Number, body []byte) []byte {
	return protowire.AppendBytes(protowire.AppendTag(nil, number, protowire.BytesType), body)
}

// fullTextSource encodes the analyzer values observed on local-ydb 26.2:
// STANDARD tokenizer (2) and lowercase=true, on the body column. The existing
// generated protocol keeps the full-text family in its unknown fields.
func fullTextSource(analyzers []byte) fakeSource {
	column := append(fullTextMessage(1, []byte("body")), fullTextMessage(2, analyzers)...)
	settings := fullTextMessage(2, column)
	index := &Ydb_Table.TableIndexDescription{Name: "ft", IndexColumns: []string{"body"}}
	index.ProtoReflect().SetUnknown(fullTextMessage(11, fullTextMessage(2, settings)))
	table := plainTable(&Ydb_Table.ColumnMeta{Name: "body", Type: optional(primitive(Ydb.Type_UTF8))})
	table.Indexes = []*Ydb_Table.TableIndexDescription{index}
	return fakeSource{directories: map[string][]*Ydb_Scheme.Entry{"/local": {entry("docs", Ydb_Scheme.Entry_TABLE)}}, tables: map[string]*Ydb_Table.DescribeTableResult{"/local/docs": table}}
}

func TestReader_FullTextOptions(t *testing.T) {
	c := qt.New(t)
	// tokenizer=STANDARD, use_filter_lowercase=true.
	analyzers := []byte{0x08, 0x02, 0xa0, 0x06, 0x01}
	db, err := ydbschema.NewReaderFromSource(fullTextSource(analyzers), "/local", capability.YDB262()).ReadSchemaContext(context.Background())
	c.Assert(err, qt.IsNil)
	c.Assert(db.Indexes, qt.HasLen, 1)
	c.Assert(db.Indexes[0].Method, qt.Equals, "GLOBAL USING fulltext_relevance")
	c.Assert(db.Indexes[0].StorageParams, qt.DeepEquals, map[string]string{"tokenizer": "standard", "use_filter_lowercase": "true"})
	c.Assert(db.Indexes[0].Definition, qt.Equals, "INDEX `ft` GLOBAL USING fulltext_relevance ON (`body`) WITH (tokenizer=standard, use_filter_lowercase=true)")
}

func TestReader_FullTextUnknownSettingsFailClosed(t *testing.T) {
	for _, test := range []struct {
		name      string
		analyzers []byte
		want      string
	}{
		{name: "new tokenizer", analyzers: []byte{0x08, 0x7f}, want: `.*unknown full-text tokenizer 127`},
		{name: "new analyzer", analyzers: []byte{0x08, 0x02, 0xe8, 0x07, 0x01}, want: `.*unknown full-text analyzer field 125`},
		{name: "invalid boolean", analyzers: []byte{0x08, 0x02, 0xa0, 0x06, 0x02}, want: `.*invalid full-text boolean 2`},
		{name: "invalid language wire type", analyzers: []byte{0x08, 0x02, 0x10, 0x01}, want: `.*invalid full-text analyzer wire type 0 for field 2`},
		{name: "duplicate tokenizer", analyzers: []byte{0x08, 0x02, 0x08, 0x01}, want: `.*duplicate full-text analyzer option tokenizer`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := ydbschema.NewReaderFromSource(fullTextSource(test.analyzers), "/local", capability.YDB262()).ReadSchemaContext(context.Background())
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(db, qt.IsNil)
		})
	}
}
