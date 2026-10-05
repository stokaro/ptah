package ydb_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/ydb-platform/ydb-go-genproto/draft/protos/Ydb_View"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	ydbschema "ptah.run/internal/dbschema/ydb"
)

// commentedSource holds a table whose attributes carry its own comment, a
// column's, an index's, the comments of a column and an index the table no
// longer has, and attributes Ptah does not own, beside a commented view.
func commentedSource() fakeSource {
	table := plainTable(&Ydb_Table.ColumnMeta{Name: "email", Type: optional(primitive(Ydb.Type_UTF8))})
	table.Indexes = []*Ydb_Table.TableIndexDescription{{Name: "by_email", IndexColumns: []string{"email"},
		Type: &Ydb_Table.TableIndexDescription_GlobalIndex{GlobalIndex: &Ydb_Table.GlobalIndex{}}}}
	table.Attributes = map[string]string{
		"ptah.comment":                "People who sign in",
		"ptah.comment.column.email":   "Login",
		"ptah.comment.column.phone":   "a dropped column's",
		"ptah.comment.index.by_email": "Lookup by login",
		"ptah.comment.index.by_phone": "a dropped index's",
		"__async_replica":             "true",
		"owner":                       "team",
	}
	return fakeSource{
		directories: map[string][]*Ydb_Scheme.Entry{
			"/local": {entry("users", Ydb_Scheme.Entry_TABLE), entry("active", Ydb_Scheme.Entry_VIEW)},
		},
		tables: map[string]*Ydb_Table.DescribeTableResult{
			"/local/users":                         table,
			"/local/users/by_email/indexImplTable": implementationTable(),
			"/local/active":                        {Attributes: map[string]string{"ptah.comment": "Recent", "owner": "team"}},
		},
		views: map[string]*Ydb_View.DescribeViewResult{"/local/active": {QueryText: "SELECT id FROM users"}},
	}
}

// The comments a table's and a view's attributes hold are read onto the
// objects they belong to. An attribute Ptah does not own is not a comment,
// and neither is the comment of a column or an index the table does not
// have.
func TestReader_ReadsComments(t *testing.T) {
	c := qt.New(t)
	db, err := ydbschema.NewReaderFromSource(commentedSource(), "/local", capability.YDB262()).
		ReadSchemaContext(context.Background())
	c.Assert(err, qt.IsNil)

	c.Assert(db.Tables, qt.HasLen, 1)
	c.Assert(db.Tables[0].Comment, qt.Equals, "People who sign in")
	comments := map[string]string{}
	for _, column := range db.Tables[0].Columns {
		comments[column.Name] = column.Comment
	}
	c.Assert(comments, qt.DeepEquals, map[string]string{"id": "", "email": "Login"})
	c.Assert(db.Indexes, qt.HasLen, 1)
	c.Assert(db.Indexes[0].Name, qt.Equals, "by_email")
	c.Assert(db.Indexes[0].Comment, qt.Equals, "Lookup by login")
	c.Assert(db.Views, qt.DeepEquals, []catalog.View{{Name: "active", Body: "SELECT id FROM users", Comment: "Recent"}})
}

// Each key decides its own read: a target without comment_attributes reads
// no table, column or index comment, and one without view_comments asks no
// view for its attributes -- the fixture holds none for the view, so asking
// would fail the read.
func TestReader_ReadsCommentsOnlyWithTheirKeys(t *testing.T) {
	c := qt.New(t)
	source := commentedSource()
	delete(source.tables, "/local/active")
	caps := capability.YDB262().With(capability.CommentAttributes, false).With(capability.ViewComments, false)

	db, err := ydbschema.NewReaderFromSource(source, "/local", caps).ReadSchemaContext(context.Background())
	c.Assert(err, qt.IsNil)

	c.Assert(db.Tables[0].Comment, qt.Equals, "")
	c.Assert(db.Tables[0].Columns[1].Comment, qt.Equals, "")
	c.Assert(db.Indexes[0].Comment, qt.Equals, "")
	c.Assert(db.Views, qt.DeepEquals, []catalog.View{{Name: "active", Body: "SELECT id FROM users"}})
}

// A view whose attributes the table service cannot describe fails the read
// with the server's answer rather than reading as uncommented.
func TestReader_ReadsComments_FailurePath(t *testing.T) {
	c := qt.New(t)
	source := commentedSource()
	delete(source.tables, "/local/active")

	db, err := ydbschema.NewReaderFromSource(source, "/local", capability.YDB262()).ReadSchemaContext(context.Background())

	c.Assert(err, qt.ErrorMatches, `described /local/active, which the fixture does not hold`)
	c.Assert(db, qt.IsNil)
}
