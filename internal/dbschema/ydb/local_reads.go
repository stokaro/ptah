package ydb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path"
	"strings"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Scheme_V1"
	"github.com/ydb-platform/ydb-go-genproto/draft/Ydb_View_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"
	ydbsdk "github.com/ydb-platform/ydb-go-sdk/v3"

	"ptah.run/core/platform"
	"ptah.run/internal/sqlreach"
)

// ErrReadLeavesDatabase is wrapped by every refusal of [ProveLocalReads] that
// found a read whose rows come from outside the database: an external table,
// read by name or through a view.
var ErrReadLeavesDatabase = errors.New("the query reads rows from outside the database")

// ReadCatalog answers what [ProveLocalReads] asks of a database: its root, the
// type of the object at an absolute path, and the query a view at an absolute
// path stores.
type ReadCatalog interface {
	Database() string
	EntryType(ctx context.Context, path string) (Ydb_Scheme.Entry_Type, error)
	ViewQuery(ctx context.Context, path string) (string, error)
}

// ProveLocalReads proves that query, a YQL SELECT sent to a database whose
// relative names mean the directory root, reads only rows the database
// stores, and returns an error naming the first read it cannot prove.
//
// The text names what the query reads (see [sqlreach.ReadYQLSources]), and
// the catalog says what each name is. A row table, a column table and a
// system view are read where they are stored. A view is followed into the
// query it stores, to a depth of [sqlreach.MaxNesting]. Every SELECT, the
// query's own and each view's, is held to [sqlreach.Scan] too, and one that
// reaches outside the database is refused with [ErrReadLeavesDatabase], as an
// external table is: measured on 25.1.4.7 and 26.2.1.14, a SELECT over one in
// a read-only transaction lists and fetches its files from the object storage
// its data source names, and a SELECT over a view that reads one does the
// same. Every other object, and a name the catalog cannot find, is refused
// too, since nothing then says where its rows come from.
//
// The proof is as old as the catalog's answers. An object created, replaced or
// dropped between the proof and the query is not seen.
func ProveLocalReads(ctx context.Context, catalog ReadCatalog, root, query string) error {
	return proveLocalReads(ctx, catalog, root, query, 0)
}

func proveLocalReads(ctx context.Context, catalog ReadCatalog, root, query string, depth int) error {
	reads, err := sqlreach.ReadYQLSources(query)
	if err != nil {
		return err
	}
	for _, statement := range reads.Selects {
		finding, reaches, err := sqlreach.Scan(statement, platform.YDB)
		switch {
		case err != nil:
			return fmt.Errorf("the query %w", err)
		case reaches:
			return fmt.Errorf("%w: the query uses %s, which %s", ErrReadLeavesDatabase, finding.Construct, finding.Reach)
		}
	}
	base := root
	if reads.Prefix != "" {
		base = absolutePath(root, reads.Prefix)
	}
	for _, source := range reads.Sources {
		target := absolutePath(base, source)
		entryType, err := catalog.EntryType(ctx, target)
		if err != nil {
			return fmt.Errorf("the query reads %s, which cannot be described: %w", target, err)
		}
		switch entryType {
		case Ydb_Scheme.Entry_TABLE, Ydb_Scheme.Entry_COLUMN_TABLE, Ydb_Scheme.Entry_SYS_VIEW:
			continue
		case Ydb_Scheme.Entry_EXTERNAL_TABLE:
			return fmt.Errorf("%w: %s is an external table, whose rows the server fetches from its data source",
				ErrReadLeavesDatabase, target)
		case Ydb_Scheme.Entry_VIEW:
			if err := proveViewReads(ctx, catalog, target, depth); err != nil {
				return err
			}
		default:
			return fmt.Errorf("the query reads %s, a %s, which the proof does not follow",
				target, entryTypeName(entryType))
		}
	}
	return nil
}

// proveViewReads proves the query the view at target stores. A name in it
// means a path under the database root, unless a TablePathPrefix pragma the
// view was created under, which the server stores with the query, says
// otherwise.
func proveViewReads(ctx context.Context, catalog ReadCatalog, target string, depth int) error {
	if depth >= sqlreach.MaxNesting {
		return fmt.Errorf("the query reads the view %s through more than %d views, which the proof does not follow",
			target, sqlreach.MaxNesting)
	}
	stored, err := catalog.ViewQuery(ctx, target)
	if err != nil {
		return fmt.Errorf("the query reads the view %s, whose query cannot be read: %w", target, err)
	}
	if err := proveLocalReads(ctx, catalog, catalog.Database(), stored, depth+1); err != nil {
		return fmt.Errorf("the query reads the view %s: %w", target, err)
	}
	return nil
}

// absolutePath is name, a path as a query writes it, under base unless it is
// absolute already.
func absolutePath(base, name string) string {
	if strings.HasPrefix(name, "/") {
		return path.Clean(name)
	}
	return path.Join(base, name)
}

// SessionReadCatalog returns the catalog of the database session belongs to,
// a connection from a pool [Open] made, and the directory a relative name in
// that connection's queries means: the dev realm's, when the URL named one,
// or the database's. A connection from any other pool is an error.
func SessionReadCatalog(session *sql.Conn) (ReadCatalog, string, error) {
	var (
		sdk  *ydbsdk.Driver
		root string
	)
	err := session.Raw(func(raw any) error {
		bound, ok := raw.(conn)
		if !ok || bound.driver == nil {
			return fmt.Errorf("%T is not a connection to a YDB database Ptah opened", raw)
		}
		sdk, root = bound.driver, bound.root
		return nil
	})
	if err != nil {
		return nil, "", err
	}
	if root == "" {
		root = sdk.Name()
	}
	connection := ydbsdk.GRPCConn(sdk)
	return grpcReadCatalog{
		database: sdk.Name(),
		scheme:   grpcScheme{client: Ydb_Scheme_V1.NewSchemeServiceClient(connection)},
		views:    &grpcSource{view: Ydb_View_V1.NewViewServiceClient(connection)},
	}, root, nil
}

// grpcReadCatalog answers through raw scheme and view service calls.
type grpcReadCatalog struct {
	database string
	scheme   grpcScheme
	views    *grpcSource
}

func (c grpcReadCatalog) Database() string { return c.database }

func (c grpcReadCatalog) EntryType(ctx context.Context, path string) (Ydb_Scheme.Entry_Type, error) {
	entry, err := c.scheme.DescribePath(ctx, path)
	if err != nil {
		return Ydb_Scheme.Entry_TYPE_UNSPECIFIED, err
	}
	return entry.GetType(), nil
}

func (c grpcReadCatalog) ViewQuery(ctx context.Context, path string) (string, error) {
	described, err := c.views.DescribeView(ctx, path)
	if err != nil {
		return "", err
	}
	return described.GetQueryText(), nil
}
