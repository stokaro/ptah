// Package ydbdocker declares the YDB image internal/devdocker starts from a
// docker URL, and the readiness probe that image needs.
package ydbdocker

import (
	"context"
	"errors"
	"fmt"
	"net/url"

	"ptah.run/dbschema"
	"ptah.run/internal/devdocker/dockerengine"
	"ptah.run/internal/sqlident"
	"ptah.run/internal/ydbready"
)

// Engines returns the YDB engine under the name a docker URL gives it. Each
// call returns new values.
//
// local-ydb serves the one database /local and takes every connection
// without a credential. It advertises its node through discovery under the
// container's host name, which nothing outside the container resolves, so the
// URL disables the balancer and the SDK connects to the published port it was
// given; measured on 26.2.1.14 with a port the daemon chose and no --hostname,
// the connection reads the database at once. In-memory disks start the server
// fastest and hold nothing a throwaway database needs to keep.
func Engines() []dockerengine.Engine {
	return []dockerengine.Engine{{
		Name:  "ydb",
		Image: "ydbplatform/local-ydb",
		Port:  "2136",
		Env: func(_, _ string) []string {
			return []string{"YDB_USE_IN_MEMORY_PDISKS=true"}
		},
		URL: func(hostPort, database, _, query string) string {
			return fmt.Sprintf("ydb://%s/%s%s", hostPort, url.PathEscape(database), dockerengine.QuerySuffix(query))
		},
		Params:          map[string]string{"go_balancer": "disable"},
		ServerDatabase:  "local",
		DefaultDatabase: "local",
		Databases:       []string{"local"},
		Routing:         []string{"database", "dev_realm", "monitoring"},
		Anonymous:       true,
		Ready:           Ready,
	}}
}

// readyTable is the table the readiness probe creates and drops.
const readyTable = "ptah_ready"

// Ready is the readiness probe of a `docker://ydb` server. A YDB server
// answers a query before it takes DDL: measured on local-ydb 26.2.1.14
// started twice with in-memory disks, SELECT Version() answered 1.1 seconds
// after the start and CREATE TABLE answered `database doesn't have storage
// pools at all` for another 0.8 and 1.3 seconds. So the probe connects, then
// creates and drops a table through [ydbready.Until], which waits out that one
// refusal, as the capability probe does before its first schema change.
func Ready(ctx context.Context, rawURL string) (err error) {
	conn, err := dbschema.ConnectToServer(ctx, rawURL)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, conn.Close()) }()
	table := sqlident.Quote(conn.Info().Dialect, readyTable)
	return ydbready.Until(ctx, func(ctx context.Context) error {
		// #nosec G202 -- the table name is a constant quoted through sqlident.
		if _, err := conn.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS "+table+
			" (`id` Int64 NOT NULL, PRIMARY KEY (`id`))"); err != nil {
			return err
		}
		// #nosec G202 -- the table name is a constant quoted through sqlident.
		_, err := conn.ExecContext(ctx, "DROP TABLE IF EXISTS "+table)
		return err
	})
}
