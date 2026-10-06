package ydb_test

import (
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
	"google.golang.org/protobuf/encoding/protowire"

	"ptah.run/core/platform/capability"
	ydbschema "ptah.run/internal/dbschema/ydb"
)

func TestReader_UnsupportedIndexDiagnosticNamesTheKind(t *testing.T) {
	tests := []struct {
		name  string
		field protowire.Number
		kind  string
	}{
		{name: "bloom", field: 12, kind: "bloom_filter index"},
		{name: "ngram", field: 13, kind: "bloom_ngram_filter index"},
		{name: "JSON", field: 14, kind: "global JSON index"},
		{name: "min-max", field: 15, kind: "min_max index"},
		{name: "unknown", field: 999, kind: "index of a kind this build of Ptah does not know"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			index := &Ydb_Table.TableIndexDescription{Name: "body_idx", IndexColumns: []string{"body"}}
			unknown := protowire.AppendTag(nil, test.field, protowire.BytesType)
			index.ProtoReflect().SetUnknown(protowire.AppendBytes(unknown, nil))
			table := plainTable()
			table.Indexes = []*Ydb_Table.TableIndexDescription{index}
			source := fakeSource{
				directories: map[string][]*Ydb_Scheme.Entry{"/local": {entry("t", Ydb_Scheme.Entry_TABLE)}},
				tables:      map[string]*Ydb_Table.DescribeTableResult{"/local/t": table},
			}

			db, err := ydbschema.NewReaderFromSource(source, "/local", capability.YDB262()).ReadSchema()

			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(
				`YDB table /local/t: index "body_idx" is a `+test.kind+`, which this build of Ptah does not read`))
			c.Assert(db, qt.IsNil)
		})
	}
}
