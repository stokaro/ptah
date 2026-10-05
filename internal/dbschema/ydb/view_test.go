package ydb_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/ydb-platform/ydb-go-genproto/draft/protos/Ydb_View"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
	"google.golang.org/protobuf/encoding/protowire"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/platform/capability"
	ydbschema "ptah.run/internal/dbschema/ydb"
)

// viewSource holds a view at the root and one in a directory, each beside a
// table, with the query text DescribeView returns for them.
func viewSource() fakeSource {
	return fakeSource{
		directories: map[string][]*Ydb_Scheme.Entry{
			"/local": {
				entry("users", Ydb_Scheme.Entry_TABLE),
				entry("active_users", Ydb_Scheme.Entry_VIEW),
				entry("app", Ydb_Scheme.Entry_DIRECTORY),
			},
			"/local/app": {entry("orders", Ydb_Scheme.Entry_TABLE), entry("big_orders", Ydb_Scheme.Entry_VIEW)},
		},
		tables: map[string]*Ydb_Table.DescribeTableResult{
			"/local/users":          plainTable(),
			"/local/app/orders":     plainTable(),
			"/local/active_users":   {},
			"/local/app/big_orders": {},
		},
		views: map[string]*Ydb_View.DescribeViewResult{
			"/local/active_users":   {QueryText: "SELECT id FROM users"},
			"/local/app/big_orders": {QueryText: "SELECT id FROM `app/orders` WHERE id > 10"},
		},
	}
}

// A view is described by its directory, its name and the query text the
// server stores, in the order the walk meets it, and a read scoped to a
// directory describes only the views in it.
func TestReader_DescribesViews_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		schemas []string
		want    []catalog.View
	}{
		{
			name: "every directory",
			want: []catalog.View{
				{Name: "active_users", Body: "SELECT id FROM users"},
				{Name: "big_orders", Schema: "app", Body: "SELECT id FROM `app/orders` WHERE id > 10"},
			},
		},
		{
			name:    "one directory",
			schemas: []string{"app"},
			want:    []catalog.View{{Name: "big_orders", Schema: "app", Body: "SELECT id FROM `app/orders` WHERE id > 10"}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			reader := ydbschema.NewReaderFromSource(viewSource(), "/local", capability.YDB262())
			reader.SetSchemas(test.schemas)

			db, err := reader.ReadSchemaContext(context.Background())

			c.Assert(err, qt.IsNil)
			c.Assert(db.Views, qt.DeepEquals, test.want)
			c.Assert(db.NotDescribed.Describes(coverage.View, "active_users"), qt.IsTrue)
		})
	}
}

// A server without [capability.Views] has its views recorded rather than
// described, so a plan never meets a view the renderer would refuse. The view
// service is not asked: the fixture answers no view on this read.
func TestReader_RecordsViewsWithoutTheKey(t *testing.T) {
	c := qt.New(t)
	source := viewSource()
	source.views = nil
	caps := capability.YDB262().With(capability.Views, false)

	db, err := ydbschema.NewReaderFromSource(source, "/local", caps).ReadSchemaContext(context.Background())

	c.Assert(err, qt.IsNil)
	c.Assert(db.Views, qt.HasLen, 0)
	c.Assert(db.NotDescribed.Describes(coverage.View, "active_users"), qt.IsFalse)
	c.Assert(db.NotDescribed.Describes(coverage.View, "app.big_orders"), qt.IsFalse)
	c.Assert(db.Tables, qt.HasLen, 2)
}

// A view description carrying a field the pinned protocol buffers do not
// model refuses the read, naming the view and the field, rather than reading
// a view whose newer setting would be lost. A view the server cannot describe
// fails the read with the server's answer.
func TestReader_DescribesViews_FailurePath(t *testing.T) {
	unknown := &Ydb_View.DescribeViewResult{QueryText: "SELECT 1 AS a"}
	unknown.ProtoReflect().SetUnknown(protowire.AppendVarint(protowire.AppendTag(nil, 3, protowire.VarintType), 1))
	tests := []struct {
		name    string
		views   map[string]*Ydb_View.DescribeViewResult
		wantErr string
	}{
		{
			name:    "a field the reader does not know",
			views:   map[string]*Ydb_View.DescribeViewResult{"/local/v": unknown},
			wantErr: `YDB view /local/v carries field 3 of its description, which this build of Ptah does not read`,
		},
		{
			name:    "a view the server cannot describe",
			wantErr: `described view /local/v, which the fixture does not hold`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			source := fakeSource{
				directories: map[string][]*Ydb_Scheme.Entry{"/local": {entry("v", Ydb_Scheme.Entry_VIEW)}},
				tables:      map[string]*Ydb_Table.DescribeTableResult{"/local/v": {}},
				views:       test.views,
			}

			db, err := ydbschema.NewReaderFromSource(source, "/local", capability.YDB262()).
				ReadSchemaContext(context.Background())

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db, qt.IsNil)
		})
	}
}

// A database read retains user pragmas even when their path resembles a dev
// realm. Only a connection that explicitly reads a realm removes its prefix.
func TestReader_PreservesUserViewPragmas(t *testing.T) {
	c := qt.New(t)
	source := viewSource()
	const body = "PRAGMA TablePathPrefix('/local/ptah_dev/run');\nSELECT id FROM users"
	source.views["/local/active_users"].QueryText = body

	db, err := ydbschema.NewReaderFromSource(source, "/local", capability.YDB262()).ReadSchemaContext(c.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(db.Views[0].Body, qt.Equals, body)
}
