package ydb

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb-go-genproto/draft/protos/Ydb_View"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/ydbcomment"
	"ptah.run/internal/ydbview"
)

// view adds one described view, whose body is the query text the server
// stores, with the comment its attributes hold (see [Reader.viewComment]).
//
// The text is the server's own form of the query -- its tokens joined by
// single spaces, without comments, after any pragma the CREATE VIEW ran under
// -- rather than what the CREATE VIEW said; the comparison reads a declaration
// into the same form (see [ptah.run/internal/ydbview.QueryText]). The description carries no security
// mode: YDB creates no view without `security_invoker = TRUE`, so there is
// none to record. A field the pinned protocol buffers do not model refuses
// the read, as a column's does, because a view setting a newer YDB added
// would otherwise be lost in silence.
//
// Inside a dev realm, the leading pragma added by the connection is removed:
// the catalog already resolves relative paths from that realm.
func (r *Reader) view(
	ctx context.Context,
	source Source,
	schema, name string,
	described *Ydb_View.DescribeViewResult,
	db *catalog.Database,
) error {
	if unknown := unknownFields(described); len(unknown) > 0 {
		return fmt.Errorf("YDB view %s carries field %s of its description, which this build of Ptah does not read",
			r.absolute(schema, name), joinNumbers(unknown))
	}
	comment, err := r.viewComment(ctx, source, schema, name)
	if err != nil {
		return err
	}
	body := described.GetQueryText()
	if r.realm {
		body = ydbview.RealmQueryText(body, r.database)
	}
	db.Views = append(db.Views, catalog.View{Name: name, Schema: schema, Body: body, Comment: comment})
	return nil
}

// viewComment reads a view's comment from its user attributes, on a target
// with [capability.ViewComments]. The view service's description carries no
// attributes; the table service's does, for a view too: measured on 25.1.4.7
// and 26.2.1.14, DescribeTable on a view's path reports the attributes
// AlterTable set on it.
func (r *Reader) viewComment(ctx context.Context, source Source, schema, name string) (string, error) {
	if !r.caps.Has(capability.ViewComments) {
		return "", nil
	}
	described, err := source.DescribeTable(ctx, r.absolute(schema, name))
	if err != nil {
		return "", err
	}
	return ydbcomment.Read(described.GetAttributes()).Own, nil
}
