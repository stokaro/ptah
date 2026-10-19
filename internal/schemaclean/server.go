package schemaclean

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"ptah.run/catalog"
	"ptah.run/dbschema"
	"ptah.run/internal/dbreset"
	"ptah.run/internal/envbool"
)

// AllowServerCleanEnvVar lets a cleanup drop every user database of a whole
// MySQL or MariaDB server, the scope a URL naming no database selects.
//
// Without it the cleanup refuses, because a server is often shared and one
// confirmation, or --auto-approve in a script, would drop databases the
// operator never meant to name. The pinned community binary v1.3.0 drops them
// at exit 0, measured on MySQL 8.4.11 and MariaDB 11.8.9, so the refusal is a
// deliberate difference, and the variable restores the binary's behavior
// (stokaro/ptah#3789). It is an environment variable rather than a flag for
// the reason [ptah.run/internal/pgreservedrole.AllowEnvVar] gives.
const AllowServerCleanEnvVar = "PTAH_ALLOW_SERVER_CLEAN"

// It is [envbool.Retained]: a true value restores what the pinned community
// binary does anyway, so strict mode keeps it; refusing it there would make
// strict mode stricter than the binary it matches.
var allowServerClean = envbool.New(AllowServerCleanEnvVar, false, envbool.Retained)

// ErrServerCleanRefused is the refusal [ServerCleanPolicy.Refuse] wraps.
var ErrServerCleanRefused = errors.New(
	"refusing to clean a whole MySQL or MariaDB server without " + AllowServerCleanEnvVar + "=1",
)

// ServerCleanPolicy decides whether a cleanup may drop the databases of a
// whole MySQL or MariaDB server. The zero value refuses.
type ServerCleanPolicy struct {
	allowed bool
}

// ResolveServerCleanPolicy reads [AllowServerCleanEnvVar]. A command that
// cleans resolves it before it does any work, so a malformed value is refused
// on every run and not only on one that reaches a whole server.
func ResolveServerCleanPolicy() (ServerCleanPolicy, error) {
	allowed, err := allowServerClean.Resolve()
	if err != nil {
		return ServerCleanPolicy{}, err
	}
	return ServerCleanPolicy{allowed: allowed}, nil
}

// Refuse answers [ErrServerCleanRefused] for a plan that drops a database of
// a whole server, unless the policy allows it. A connection to one database
// and a plan that drops no database are never refused. The refusal lists the
// databases in the form a dry run prints them, so the operator sees what
// setting the variable would drop.
func (p ServerCleanPolicy) Refuse(info catalog.ServerInfo, plan Plan) error {
	if p.allowed || !info.WholeServer {
		return nil
	}
	var drops strings.Builder
	for _, change := range plan.Changes {
		if change.Type == ObjectTypeSchema {
			fmt.Fprintf(&drops, "\n- %s", change.Cmd)
		}
	}
	if drops.Len() == 0 {
		return nil
	}
	return fmt.Errorf("%w: the URL names no database, and the cleanup would drop every user database on the server:%s\n"+
		"Set %s=1 to clean the server, or name a database in the URL to clean that database alone",
		ErrServerCleanRefused, drops.String(), AllowServerCleanEnvVar)
}

// ObjectTypeSchema is a schema a cleanup drops whole: on MySQL and MariaDB, a
// user database of a whole server, which `DROP DATABASE` drops with every
// object in it.
const ObjectTypeSchema = "schema"

// IsServer reports whether conn is a whole server, whose cleanup drops every
// user database rather than the objects of one; see
// [catalog.ServerInfo.WholeServer].
func IsServer(conn *dbschema.DatabaseConnection) bool {
	return conn.Info().WholeServer
}

// serverCatalog is a catalog strategy that also lists what a cleanup of a
// whole server drops, for a connection whose URL names no database; see
// [catalog.ServerInfo.WholeServer].
type serverCatalog interface {
	ServerObjects(ctx context.Context, q dbreset.Querier) ([]dbreset.CleanupObject, error)
}

// inspectServer plans the cleanup of a whole server: what the writer's
// catalog strategy lists for it, each database a schema dropped whole, after
// every foreign key one database keeps into another.
func inspectServer(ctx context.Context, conn *dbschema.DatabaseConnection) ([]Object, error) {
	return serverObjects(ctx, cleanupCatalogOf(conn), conn, conn.Info().Dialect)
}

// serverObjects lists, through q, what strategy says a cleanup of a whole
// server drops, as plan rows. It refuses a strategy that lists no server, and
// a kind the plan has no name for, naming dialect.
func serverObjects(ctx context.Context, strategy dbreset.CleanupCatalog, q dbreset.Querier, dialect string) ([]Object, error) {
	server, ok := strategy.(serverCatalog)
	if !ok {
		return nil, fmt.Errorf("inspect cleanup databases: the %s writer does not list the databases of a whole server", dialect)
	}
	listed, err := server.ServerObjects(ctx, q)
	if err != nil {
		return nil, err
	}
	return planObjects(listed, dialect)
}
