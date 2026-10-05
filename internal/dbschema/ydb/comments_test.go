package ydb_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"

	ydbschema "ptah.run/internal/dbschema/ydb"
	"ptah.run/internal/ydbcomment"
)

// fakeAttributes answers for a database whose objects it holds by absolute
// path, and records each change of attributes it is asked for.
type fakeAttributes struct {
	entries  map[string]Ydb_Scheme.Entry_Type
	tables   map[string]*Ydb_Table.DescribeTableResult
	alterErr error
	altered  []alteration
}

// alteration is one change of attributes the fake was asked for.
type alteration struct {
	Path       string
	Attributes map[string]string
}

func (f *fakeAttributes) DescribePath(_ context.Context, absolute string) (*Ydb_Scheme.Entry, error) {
	entryType, found := f.entries[absolute]
	if !found {
		return nil, errors.New("SCHEME_ERROR: Path not found")
	}
	return &Ydb_Scheme.Entry{Type: entryType}, nil
}

func (f *fakeAttributes) DescribeTable(_ context.Context, absolute string) (*Ydb_Table.DescribeTableResult, error) {
	return f.tables[absolute], nil
}

func (f *fakeAttributes) AlterAttributes(_ context.Context, absolute string, attributes map[string]string) error {
	if f.alterErr != nil {
		return f.alterErr
	}
	f.altered = append(f.altered, alteration{Path: absolute, Attributes: attributes})
	return nil
}

// usersDatabase holds a row table with a column and an index, a view, a
// column table and a topic at the root of /local, and a table inside a dev
// realm.
func usersDatabase() *fakeAttributes {
	return &fakeAttributes{
		entries: map[string]Ydb_Scheme.Entry_Type{
			"/local/shop/users":      Ydb_Scheme.Entry_TABLE,
			"/local/active":          Ydb_Scheme.Entry_VIEW,
			"/local/events":          Ydb_Scheme.Entry_COLUMN_TABLE,
			"/local/feed":            Ydb_Scheme.Entry_TOPIC,
			"/local/.ptah/r/t":       Ydb_Scheme.Entry_TABLE,
			"/local/.sys/partitions": Ydb_Scheme.Entry_SYS_VIEW,
		},
		tables: map[string]*Ydb_Table.DescribeTableResult{
			"/local/shop/users": {
				Columns: []*Ydb_Table.ColumnMeta{{Name: "id"}, {Name: "email"}},
				Indexes: []*Ydb_Table.TableIndexDescription{{Name: "by_email"}},
			},
		},
	}
}

// Each statement sets the one attribute its key names on the table or view,
// and an empty comment asks the service to remove it.
func TestRunCommentStatement_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		root  string
		query ydbcomment.Query
		want  alteration
	}{
		{name: "table", root: "/local",
			query: ydbcomment.Query{Statement: ydbcomment.Statement{Object: ydbcomment.Table, Path: "shop/users", Comment: "People"}},
			want:  alteration{Path: "/local/shop/users", Attributes: map[string]string{"ptah.comment": "People"}}},
		{name: "column", root: "/local",
			query: ydbcomment.Query{Statement: ydbcomment.Statement{Object: ydbcomment.Column, Path: "shop/users", Name: "email", Comment: "Login"}},
			want:  alteration{Path: "/local/shop/users", Attributes: map[string]string{"ptah.comment.column.email": "Login"}}},
		{name: "index", root: "/local",
			query: ydbcomment.Query{Statement: ydbcomment.Statement{Object: ydbcomment.Index, Path: "shop/users", Name: "by_email", Comment: "Lookup"}},
			want:  alteration{Path: "/local/shop/users", Attributes: map[string]string{"ptah.comment.index.by_email": "Lookup"}}},
		{name: "view", root: "/local",
			query: ydbcomment.Query{Statement: ydbcomment.Statement{Object: ydbcomment.View, Path: "active", Comment: "Recent"}},
			want:  alteration{Path: "/local/active", Attributes: map[string]string{"ptah.comment": "Recent"}}},
		{name: "removal of a dropped column's comment", root: "/local",
			query: ydbcomment.Query{Statement: ydbcomment.Statement{Object: ydbcomment.Column, Path: "shop/users", Name: "gone"}},
			want:  alteration{Path: "/local/shop/users", Attributes: map[string]string{"ptah.comment.column.gone": ""}}},
		{name: "removal of a dropped index's comment", root: "/local",
			query: ydbcomment.Query{Statement: ydbcomment.Statement{Object: ydbcomment.Index, Path: "shop/users", Name: "gone"}},
			want:  alteration{Path: "/local/shop/users", Attributes: map[string]string{"ptah.comment.index.gone": ""}}},
		{name: "absolute path", root: "/local",
			query: ydbcomment.Query{Statement: ydbcomment.Statement{Object: ydbcomment.Table, Path: "/local/shop/users", Comment: "x"}},
			want:  alteration{Path: "/local/shop/users", Attributes: map[string]string{"ptah.comment": "x"}}},
		{name: "inside a dev realm", root: "/local/.ptah/r",
			query: ydbcomment.Query{Statement: ydbcomment.Statement{Object: ydbcomment.Table, Path: "t", Comment: "x"}, PathPrefix: "/local/.ptah/r"},
			want:  alteration{Path: "/local/.ptah/r/t", Attributes: map[string]string{"ptah.comment": "x"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			service := usersDatabase()
			err := ydbschema.RunCommentStatement(context.Background(), service, "/local", test.root, test.query)
			c.Assert(err, qt.IsNil)
			c.Assert(service.altered, qt.DeepEquals, []alteration{test.want})
		})
	}
}

// A statement YDB would accept and not keep, or keep where nothing reads it,
// is refused before any attribute changes, as is one outside the
// connection's database or realm.
func TestRunCommentStatement_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		root    string
		query   ydbcomment.Query
		wantErr string
	}{
		{name: "a column table", root: "/local",
			query:   ydbcomment.Query{Statement: ydbcomment.Statement{Object: ydbcomment.Table, Path: "events", Comment: "x"}},
			wantErr: `invalid comment statement: /local/events is a column table, which takes an attribute and does not keep it, so YDB has nowhere to keep its comments`},
		{name: "a view named as a table", root: "/local",
			query:   ydbcomment.Query{Statement: ydbcomment.Statement{Object: ydbcomment.Table, Path: "active", Comment: "x"}},
			wantErr: `invalid comment statement: COMMENT ON TABLE names /local/active, which is a VIEW`},
		{name: "a table named as a view", root: "/local",
			query:   ydbcomment.Query{Statement: ydbcomment.Statement{Object: ydbcomment.View, Path: "shop/users", Comment: "x"}},
			wantErr: `invalid comment statement: COMMENT ON VIEW names /local/shop/users, which is a TABLE`},
		{name: "a topic", root: "/local",
			query:   ydbcomment.Query{Statement: ydbcomment.Statement{Object: ydbcomment.Table, Path: "feed", Comment: "x"}},
			wantErr: `invalid comment statement: COMMENT ON TABLE names /local/feed, which is a TOPIC`},
		{name: "a view's column", root: "/local",
			query:   ydbcomment.Query{Statement: ydbcomment.Statement{Object: ydbcomment.Column, Path: "active", Name: "id", Comment: "x"}},
			wantErr: `invalid comment statement: COMMENT ON COLUMN names /local/active, which is a VIEW`},
		{name: "a column the table does not have", root: "/local",
			query:   ydbcomment.Query{Statement: ydbcomment.Statement{Object: ydbcomment.Column, Path: "shop/users", Name: "phone", Comment: "x"}},
			wantErr: `invalid comment statement: table /local/shop/users has no column "phone"`},
		{name: "an index the table does not have", root: "/local",
			query:   ydbcomment.Query{Statement: ydbcomment.Statement{Object: ydbcomment.Index, Path: "shop/users", Name: "by_phone", Comment: "x"}},
			wantErr: `invalid comment statement: table /local/shop/users has no index "by_phone"`},
		{name: "outside the realm", root: "/local/.ptah/r",
			query:   ydbcomment.Query{Statement: ydbcomment.Statement{Object: ydbcomment.Table, Path: "/local/shop/users", Comment: "x"}},
			wantErr: `invalid comment statement: /local/shop/users is not in /local/.ptah/r`},
		{name: "climbing out of the realm", root: "/local/.ptah/r",
			query:   ydbcomment.Query{Statement: ydbcomment.Statement{Object: ydbcomment.Table, Path: "../../shop/users", Comment: "x"}, PathPrefix: "/local/.ptah/r"},
			wantErr: `invalid comment statement: /local/shop/users is not in /local/.ptah/r`},
		{name: "the database itself", root: "/local",
			query:   ydbcomment.Query{Statement: ydbcomment.Statement{Object: ydbcomment.Table, Path: "/local", Comment: "x"}},
			wantErr: `invalid comment statement: /local is not in /local`},
		{name: "a dev realm from the database", root: "/local",
			query:   ydbcomment.Query{Statement: ydbcomment.Statement{Object: ydbcomment.Table, Path: ".ptah/r/t", Comment: "x"}},
			wantErr: `invalid comment statement: /local/.ptah/r/t is under a directory whose name starts with a dot, which belongs to the server or to the dev realms`},
		{name: "a server path", root: "/local",
			query:   ydbcomment.Query{Statement: ydbcomment.Statement{Object: ydbcomment.Table, Path: ".sys/partitions", Comment: "x"}},
			wantErr: `invalid comment statement: /local/.sys/partitions is under a directory whose name starts with a dot, .*`},
		{name: "a relative prefix", root: "/local",
			query:   ydbcomment.Query{Statement: ydbcomment.Statement{Object: ydbcomment.Table, Path: "t", Comment: "x"}, PathPrefix: "relp"},
			wantErr: `invalid comment statement: path relp/t is not in database /local: .*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			service := usersDatabase()
			err := ydbschema.RunCommentStatement(context.Background(), service, "/local", test.root, test.query)
			c.Assert(err, qt.ErrorIs, ydbcomment.ErrStatement)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(service.altered, qt.HasLen, 0)
		})
	}
}

// What the services answer with is reported with the comment it was setting.
func TestRunCommentStatement_FailurePath_ServerAnswers(t *testing.T) {
	tests := []struct {
		name     string
		query    ydbcomment.Query
		alterErr error
		wantErr  string
	}{
		{name: "a missing table",
			query:   ydbcomment.Query{Statement: ydbcomment.Statement{Object: ydbcomment.Table, Path: "missing", Comment: "x"}},
			wantErr: `set the comment on table /local/missing: SCHEME_ERROR: Path not found`},
		{name: "a refused change",
			query:    ydbcomment.Query{Statement: ydbcomment.Statement{Object: ydbcomment.Column, Path: "shop/users", Name: "email", Comment: "x"}},
			alterErr: errors.New("BAD_REQUEST: UserAttributes::CheckLimits: user attributes too big: 10241"),
			wantErr:  `set the comment on column "email" of table /local/shop/users: BAD_REQUEST: UserAttributes::CheckLimits: user attributes too big: 10241`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			service := usersDatabase()
			service.alterErr = test.alterErr
			err := ydbschema.RunCommentStatement(context.Background(), service, "/local", "/local", test.query)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(service.altered, qt.HasLen, 0)
		})
	}
}

// A comment statement is refused by the connection before it reaches YDB:
// with arguments, inside a transaction, as a query that should return rows,
// and on a connection with no driver to reach the table service through.
// Nothing reaches the SDK connection in any of them.
func TestBindingConnector_FailurePath_CommentStatements(t *testing.T) {
	c := qt.New(t)
	var got []driver.NamedValue
	closed := 0
	db := sql.OpenDB(ydbschema.NewBindingConnector(recordingConnector{got: &got, closed: &closed}, nil))
	c.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()

	_, err := db.ExecContext(ctx, "COMMENT ON TABLE t IS 'x'", 1)
	c.Assert(err, qt.ErrorIs, ydbcomment.ErrStatement)
	c.Assert(err, qt.ErrorMatches, `invalid comment statement: a comment statement takes no arguments, and 1 were given`)

	_, err = db.ExecContext(ctx, "COMMENT ON TABLE t IS 'x'")
	c.Assert(err, qt.ErrorMatches, `this YDB connection reaches no table service`)

	_, err = db.ExecContext(ctx, "COMMENT ON TABLE t IS 5")
	c.Assert(err, qt.ErrorIs, ydbcomment.ErrStatement)

	rows, err := db.QueryContext(ctx, "COMMENT ON TABLE t IS 'x'")
	c.Assert(err, qt.ErrorMatches, `invalid comment statement: a comment statement returns no rows; execute it`)
	c.Assert(rows, qt.IsNil)

	transaction, err := db.BeginTx(ctx, nil)
	c.Assert(err, qt.IsNil)
	_, err = transaction.ExecContext(ctx, "COMMENT ON TABLE t IS 'x'")
	c.Assert(err, qt.ErrorMatches, `invalid comment statement: a comment statement runs outside a transaction, as YDB runs every scheme statement, and this connection has one open`)
	c.Assert(transaction.Rollback(), qt.IsNil)

	c.Assert(got, qt.IsNil)
}
