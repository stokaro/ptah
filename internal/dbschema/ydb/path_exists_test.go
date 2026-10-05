package ydb_test

import (
	"context"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"

	ydbschema "ptah.run/internal/dbschema/ydb"
)

func TestPathExists_SchemeTree(t *testing.T) {
	for _, test := range []struct {
		name, lookup, realm string
		tree                map[string][]*Ydb_Scheme.Entry
		want                bool
		failure             string
	}{
		{name: "removed directory", failure: "<nil>", lookup: "probe", tree: map[string][]*Ydb_Scheme.Entry{"/local": {}}},
		{name: "remaining directory", failure: "<nil>", lookup: "probe", tree: map[string][]*Ydb_Scheme.Entry{"/local": {{Name: "probe", Type: Ydb_Scheme.Entry_DIRECTORY}}}, want: true},
		{name: "replacement object", failure: "<nil>", lookup: "probe", tree: map[string][]*Ydb_Scheme.Entry{"/local": {{Name: "probe", Type: Ydb_Scheme.Entry_COLUMN_TABLE}}}, want: true},
		{name: "missing parent", failure: "<nil>", lookup: "probe/nested", tree: map[string][]*Ydb_Scheme.Entry{"/local": {}}},
		{name: "nested path", failure: "<nil>", lookup: "probe/nested", tree: map[string][]*Ydb_Scheme.Entry{"/local": {{Name: "probe", Type: Ydb_Scheme.Entry_DIRECTORY}}, "/local/probe": {{Name: "nested", Type: Ydb_Scheme.Entry_TOPIC}}}, want: true},
		{name: "failed lookup", lookup: "probe", failure: "ydb: list /local: listed /local, which the fixture does not hold"},
		{name: "failed nested lookup", lookup: "probe/nested", tree: map[string][]*Ydb_Scheme.Entry{"/local": {{Name: "probe", Type: Ydb_Scheme.Entry_DIRECTORY}}}, failure: "ydb: list /local/probe: listed /local/probe, which the fixture does not hold"},
		{name: "non-directory parent", lookup: "probe/nested", tree: map[string][]*Ydb_Scheme.Entry{"/local": {{Name: "probe", Type: Ydb_Scheme.Entry_TABLE}}}, failure: "ydb: parent /local/probe is not a directory"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fake := &fakeDatabase{tree: test.tree}
			writer := ydbschema.NewWriterFromScheme(fake, fake, "/local", test.realm)
			found, err := writer.PathExists(context.Background(), test.lookup)
			c.Assert(fmt.Sprint(err), qt.Equals, test.failure)
			c.Assert(found, qt.Equals, test.want)
		})
	}
}
