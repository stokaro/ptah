package ydb

import (
	"fmt"

	"github.com/ydb-platform/ydb-go-genproto/draft/protos/Ydb_View"

	"ptah.run/catalog"
)

// view adds one described view, whose body is the query text the server
// stores.
//
// The text is the server's own form of the query -- its tokens joined by
// single spaces, without comments, after any pragma the CREATE VIEW ran under
// -- rather than what the CREATE VIEW said; the comparison reads a declaration
// into the same form (see [ptah.run/internal/ydbview.QueryText]). The description carries no security
// mode: YDB creates no view without `security_invoker = TRUE`, so there is
// none to record. A field the pinned protocol buffers do not model refuses
// the read, as a column's does, because a view setting a newer YDB added
// would otherwise be lost in silence.
func (r *Reader) view(schema, name string, described *Ydb_View.DescribeViewResult, db *catalog.Database) error {
	if unknown := unknownFields(described); len(unknown) > 0 {
		return fmt.Errorf("YDB view %s carries field %s of its description, which this build of Ptah does not read",
			r.absolute(schema, name), joinNumbers(unknown))
	}
	db.Views = append(db.Views, catalog.View{Name: name, Schema: schema, Body: described.GetQueryText()})
	return nil
}
