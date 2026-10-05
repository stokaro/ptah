package ydb

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Scheme_V1"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Table_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"

	"ptah.run/internal/ydbcomment"
	"ptah.run/internal/yqlservice"
)

// Attributes is what Ptah asks YDB to run a comment statement: the entry at a
// path, a row table's description, and a change of the user attributes of a
// row table or a view. Each takes an absolute path. AlterAttributes sets each
// key to its value, and removes a key whose value is empty, as the table
// service does.
type Attributes interface {
	DescribePath(ctx context.Context, absolute string) (*Ydb_Scheme.Entry, error)
	DescribeTable(ctx context.Context, absolute string) (*Ydb_Table.DescribeTableResult, error)
	AlterAttributes(ctx context.Context, absolute string, attributes map[string]string) error
}

// grpcAttributes answers through raw scheme and table service calls. A table
// service call takes no session: measured on 25.1.4.7 and 26.2.1.14,
// AlterTable with an empty session id changes the attributes it names.
type grpcAttributes struct {
	scheme grpcScheme
	table  Ydb_Table_V1.TableServiceClient
}

// newGRPCAttributes answers through the services of connection's driver.
func newGRPCAttributes(scheme Ydb_Scheme_V1.SchemeServiceClient, table Ydb_Table_V1.TableServiceClient) grpcAttributes {
	return grpcAttributes{scheme: grpcScheme{client: scheme}, table: table}
}

func (a grpcAttributes) DescribePath(ctx context.Context, absolute string) (*Ydb_Scheme.Entry, error) {
	entry, err := a.scheme.DescribePath(ctx, absolute)
	return entry, WithoutStackFrames(err)
}

func (a grpcAttributes) DescribeTable(ctx context.Context, absolute string) (*Ydb_Table.DescribeTableResult, error) {
	return (&grpcSource{table: a.table}).DescribeTable(ctx, absolute)
}

func (a grpcAttributes) AlterAttributes(ctx context.Context, absolute string, attributes map[string]string) error {
	response, err := a.table.AlterTable(ctx, &Ydb_Table.AlterTableRequest{Path: absolute, AlterAttributes: attributes})
	if err != nil {
		return WithoutStackFrames(err)
	}
	return operationStatus(response.GetOperation())
}

// RunCommentStatement runs query, one of Ptah's comment statements (see
// [ydbcomment.Recognize]), through service on the database whose absolute
// path is database. root is the absolute path the connection treats as its
// database: database itself, or a dev realm's directory under it.
//
// The statement sets one user attribute of a row table or a view, the one
// [ydbcomment.Key] names, and removes it for an empty comment. Before that it
// refuses, with [ydbcomment.ErrStatement]:
//
//   - an object outside root, and one under a directory whose name starts
//     with a dot, which belongs to the server or to the dev realms;
//   - an object of another kind than the statement names. A column table is
//     refused by name, because YDB accepts an attribute on one and does not
//     keep it (measured on 25.1.4.7 and 26.2.1.14: DescribeTable then reports
//     none);
//   - a comment on a column or an index the table does not have, which YDB
//     would keep under a key nothing reads. Removing one is accepted, since a
//     plan removes the comment of a column or an index it drops, and YDB
//     keeps the attribute after the drop.
func RunCommentStatement(
	ctx context.Context,
	service Attributes,
	database, root string,
	query ydbcomment.Query,
) error {
	database = "/" + strings.Trim(database, "/")
	absolute, err := query.Absolute(database)
	if err != nil {
		return err
	}
	relative, inside := yqlservice.Within(absolute, root)
	if !inside {
		return fmt.Errorf("%w: %s is not in %s", ydbcomment.ErrStatement, absolute, "/"+strings.Trim(root, "/"))
	}
	if slices.ContainsFunc(strings.Split(relative, "/"), func(segment string) bool {
		return strings.HasPrefix(segment, ".")
	}) {
		return fmt.Errorf("%w: %s is under a directory whose name starts with a dot, which belongs to the "+
			"server or to the dev realms", ydbcomment.ErrStatement, absolute)
	}
	subject := commentSubject(query.Statement, absolute)
	entry, err := service.DescribePath(ctx, absolute)
	if err != nil {
		return fmt.Errorf("set %s: %w", subject, err)
	}
	if err := checkCommentTarget(query.Statement, absolute, entry.GetType()); err != nil {
		return err
	}
	if query.Comment != "" && (query.Object == ydbcomment.Column || query.Object == ydbcomment.Index) {
		described, err := service.DescribeTable(ctx, absolute)
		if err != nil {
			return fmt.Errorf("set %s: %w", subject, err)
		}
		if !holds(described, query.Statement) {
			return fmt.Errorf("%w: table %s has no %s %q", ydbcomment.ErrStatement, absolute,
				strings.ToLower(query.Object.Keyword()), query.Name)
		}
	}
	if err := service.AlterAttributes(ctx, absolute, map[string]string{query.Key(): query.Comment}); err != nil {
		return fmt.Errorf("set %s: %w", subject, err)
	}
	return nil
}

// errNoTableService is returned for a comment statement on a connection that
// reaches no table service.
var errNoTableService = errors.New("this YDB connection reaches no table service")

// commentSubject names the comment a statement sets, for a message.
func commentSubject(statement ydbcomment.Statement, absolute string) string {
	switch statement.Object {
	case ydbcomment.Column:
		return fmt.Sprintf("the comment on column %q of table %s", statement.Name, absolute)
	case ydbcomment.Index:
		return fmt.Sprintf("the comment on index %q of table %s", statement.Name, absolute)
	case ydbcomment.View:
		return "the comment on view " + absolute
	default:
		return "the comment on table " + absolute
	}
}

// checkCommentTarget refuses an entry of another kind than the statement
// names: a view for COMMENT ON VIEW, a row table for every other statement.
func checkCommentTarget(statement ydbcomment.Statement, absolute string, entryType Ydb_Scheme.Entry_Type) error {
	want := Ydb_Scheme.Entry_TABLE
	if statement.Object == ydbcomment.View {
		want = Ydb_Scheme.Entry_VIEW
	}
	switch {
	case entryType == want:
		return nil
	case entryType == Ydb_Scheme.Entry_COLUMN_TABLE:
		return fmt.Errorf("%w: %s is a column table, which takes an attribute and does not keep it, so YDB "+
			"has nowhere to keep its comments", ydbcomment.ErrStatement, absolute)
	default:
		return fmt.Errorf("%w: COMMENT ON %s names %s, which is a %s", ydbcomment.ErrStatement,
			statement.Object.Keyword(), absolute, entryTypeName(entryType))
	}
}

// holds reports whether a described table has the column or the index a
// statement names.
func holds(described *Ydb_Table.DescribeTableResult, statement ydbcomment.Statement) bool {
	if statement.Object == ydbcomment.Column {
		return slices.ContainsFunc(described.GetColumns(), func(column *Ydb_Table.ColumnMeta) bool {
			return column.GetName() == statement.Name
		})
	}
	return slices.ContainsFunc(described.GetIndexes(), func(index *Ydb_Table.TableIndexDescription) bool {
		return index.GetName() == statement.Name
	})
}
