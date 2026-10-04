package ydb

import (
	"context"
	"database/sql"
	"path"
	"slices"
	"strings"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Scheme_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"
	ydbsdk "github.com/ydb-platform/ydb-go-sdk/v3"
)

// TableNames lists, sorted, the row and column tables directly in directory,
// relative to the root of the database session is connected to, with "" for
// the root. The directories under it are not read. A directory that does not
// exist holds no tables, and a directory whose name starts with a dot -- a
// server directory such as .sys -- is not read at all.
//
// It asks the scheme service for one listing, where reading the schema would
// describe every table.
func TableNames(ctx context.Context, session *sql.Conn, directory string) ([]string, error) {
	if slices.ContainsFunc(strings.Split(directory, "/"), func(segment string) bool {
		return strings.HasPrefix(segment, ".")
	}) {
		return nil, nil
	}
	driver, err := DriverOf(session)
	if err != nil {
		return nil, err
	}
	scheme := grpcScheme{client: Ydb_Scheme_V1.NewSchemeServiceClient(ydbsdk.GRPCConn(driver))}
	entries, err := scheme.ListDirectory(ctx, path.Join("/"+strings.Trim(driver.Name(), "/"), directory))
	if isSchemeError(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var names []string
	for _, entry := range entries {
		switch entry.GetType() {
		case Ydb_Scheme.Entry_TABLE, Ydb_Scheme.Entry_COLUMN_TABLE:
			names = append(names, entry.GetName())
		}
	}
	slices.Sort(names)
	return names, nil
}
