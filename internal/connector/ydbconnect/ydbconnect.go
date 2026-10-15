// Package ydbconnect connects to YDB through ydb-go-sdk. The pool is the
// SDK's database/sql connector, opened by internal/dbschema/ydb rather than by
// sql.Open, because the connection binds arguments itself and closes the SDK
// driver with the pool; the reader and the writer reach the scheme and table
// services through the same SDK connection.
package ydbconnect

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"slices"

	"ptah.run/catalog"
	"ptah.run/core/dataext"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/connector"
	ydbschema "ptah.run/internal/dbschema/ydb"
	"ptah.run/internal/sqlrunner"
	"ptah.run/internal/ydbflags"
	"ptah.run/internal/ydburl"
)

// Registrations registers the service under YDB.
func Registrations() []connector.Registration {
	return []connector.Registration{{Dialect: platform.YDB, Service: Service{}}}
}

// Service is YDB's connector.
type Service struct {
	connector.Base
}

// pool is what a YDB connection keeps beside its database/sql pool.
type pool struct {
	// connection is the SDK connection the reader and the writer use.
	connection *ydbschema.Connection
	// ticket hands over the credential the connection presents.
	ticket ticketSource
}

// ticketSource returns the credential a connection presents to its server.
// Nil stands for a connection that has none to present.
type ticketSource func(context.Context) (string, error)

// read asks the source for the credential; a nil source has none.
func (src ticketSource) read(ctx context.Context) (string, error) {
	if src == nil {
		return "", nil
	}
	return src(ctx)
}

// Open opens the ydb:// URL itself: internal/dbschema/ydb translates it to the
// SDK's grpc:// form and checks its parameters and credentials.
func (Service) Open(ctx context.Context, request connector.Request) (*connector.Pool, error) {
	connection, err := ydbschema.Open(ctx, request.URL)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}
	if err := connector.Ping(ctx, connection.DB); err != nil {
		return nil, err
	}
	return &connector.Pool{
		DB:     connection.DB,
		Native: pool{connection: connection, ticket: connection.Ticket},
	}, nil
}

// native is the YDB state of a pool this service opened.
func native(opened *connector.Pool) pool {
	state, _ := opened.Native.(pool)
	return state
}

// Describe reads the version. Schema stays empty, as does the default schema
// identifier.ForDialect gives YDB: a schema is a directory, and the database
// root is the one an unqualified name means.
func (Service) Describe(
	ctx context.Context,
	opened *connector.Pool,
	_ connector.Request,
	info catalog.ServerInfo,
) (catalog.ServerInfo, error) {
	var version string
	if err := opened.DB.QueryRowContext(ctx, "SELECT Version()").Scan(&version); err != nil {
		return info, fmt.Errorf("failed to get YDB version: %w", err)
	}
	info.Version = version
	return info, nil
}

// Refine decides the keys a YDB cluster's feature flags gate, when the URL
// names the cluster's monitoring endpoint.
//
// A preset describes a release line with its default flags, and a cluster an
// operator configured can do more or less than that: measured on 26.2.1.14
// started with enable_add_unique_index, a unique index is added to a table
// holding rows, which the line's preset says is refused. The flags are read
// once, here, because they belong to the cluster rather than to a session.
//
// Without the monitoring parameter the preset stands. A failed read fails the
// connection rather than falling back to the preset: the operator asked for
// the cluster's answer, and planning without it would be planning with an
// answer nobody asked for.
//
// The page is read with the connection's own credential, because a cluster
// that enforces authentication serves the page only to a user it knows
// (measured on 26.2.1.14 and 25.1.4.7). A credential the connection carries
// over TLS is not sent to a plain http:// endpoint.
func (Service) Refine(
	ctx context.Context,
	opened *connector.Pool,
	request connector.Request,
	_ catalog.ServerInfo,
	caps capability.Capabilities,
) (capability.Capabilities, error) {
	parsed, err := ydburl.Parse(request.URL)
	if err != nil {
		return capability.Capabilities{}, fmt.Errorf("invalid YDB URL: %w", err)
	}
	if parsed.Monitoring == nil {
		return caps, nil
	}
	credential, err := native(opened).ticket.read(ctx)
	if err != nil {
		return capability.Capabilities{}, fmt.Errorf("read YDB feature flags: %w", err)
	}
	if credential != "" && parsed.Secure && parsed.Monitoring.Scheme == "http" {
		return capability.Capabilities{}, fmt.Errorf("read YDB feature flags from %s: the connection's credential travels over TLS, "+
			"and Ptah does not send it to a plain http:// endpoint; name the endpoint as https://%s",
			parsed.Monitoring, parsed.Monitoring.Host)
	}
	flags, err := ydbflags.Read(ctx, parsed.Monitoring, parsed.Database, credential)
	if err != nil {
		return capability.Capabilities{}, err
	}
	refined := flags.Refine(caps)
	if changed := deltaKeys(caps, refined); len(changed) > 0 {
		slog.Debug("YDB feature flags changed the capability set", "keys", changed)
	}
	return refined, nil
}

// deltaKeys names, sorted, the keys two capability sets disagree on.
func deltaKeys(before, after capability.Capabilities) []capability.Capability {
	var changed []capability.Capability
	for _, key := range capability.All() {
		if before.Has(key) != after.Has(key) {
			changed = append(changed, key)
		}
	}
	slices.Sort(changed)
	return changed
}

// NewReader returns the YDB reader. It asks the scheme and table services
// rather than SQL, so it reads through the driver whatever runner a session
// hands it; YDB has no session state a schema read could see.
func (Service) NewReader(opened *connector.Pool, info catalog.ServerInfo, _ sqlrunner.Runner) catalog.SchemaReader {
	return ydbschema.NewReader(native(opened).connection, info.Capabilities)
}

// NewWriter returns the YDB writer.
func (Service) NewWriter(opened *connector.Pool, _ catalog.ServerInfo, runner sqlrunner.Runner, _ *sql.Conn) catalog.SchemaWriter {
	connection := native(opened).connection
	return ydbschema.NewWriter(runner, connection.Driver, connection.Realm)
}

// ReadOnlyTransaction carries a read-only request with the isolation it
// implies: measured with ydb-go-sdk v3.153.2 on YDB 26.2.1.14, a read-only
// transaction at the default isolation is refused (`unsupported transaction
// options`), a snapshot read-only one is accepted, and a write inside it
// answers `Operation 'Upsert' can't be performed in read only transaction`.
func (Service) ReadOnlyTransaction(opts *sql.TxOptions) (*sql.TxOptions, string) {
	if opts == nil || !opts.ReadOnly {
		return opts, ""
	}
	carried := *opts
	if carried.Isolation == sql.LevelDefault {
		carried.Isolation = sql.LevelSnapshot
	}
	return &carried, ""
}

// TransactionEnded reports the rollback of a transaction the server had
// already ended. YDB ends a transaction when a statement in it fails, and the
// rollback that follows answers NOT_FOUND (`Transaction not found`), measured
// on 26.2.1.14; there is nothing left to undo, and the failure that ended it
// is the one the caller is told about.
func (Service) TransactionEnded(rollbackErr error) bool {
	return ydbschema.TransactionNotFound(rollbackErr)
}

// HoldsBytes reports YDB's binary types, String and Yson.
func (Service) HoldsBytes(databaseType string) bool {
	return ydbschema.HoldsBytes(databaseType)
}

// RowValue turns the driver's own Decimal and its local moments into plain
// values.
func (Service) RowValue(scanned any) (any, error) {
	return ydbschema.RowValue(scanned)
}

// Seeds returns YDB's seed service: a seed file runs as the queries YQL reads
// it into, in one serializable transaction with its record, and the tables a
// seed run guards are listed through the scheme service.
func (Service) Seeds() dataext.Seeds {
	return ydbschema.Seeds{}
}
