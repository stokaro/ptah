package schemaclean

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"ptah.run/catalog"
	"ptah.run/dbschema"
	"ptah.run/internal/envbool"
	"ptah.run/internal/systemschema"
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
// the reason [ptah.run/internal/reservedrole.AllowEnvVar] gives.
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

// IsServer reports whether conn is a whole MySQL or MariaDB server, whose
// cleanup drops every user database rather than the objects of one; see
// [catalog.ServerInfo.WholeServer].
func IsServer(conn *dbschema.DatabaseConnection) bool {
	return conn.Info().WholeServer
}

// inspectServer plans the cleanup of a whole MySQL or MariaDB server: every
// user database, dropped whole, after every foreign key one database keeps
// into another.
//
// The pinned community binary v1.3.0 drops the databases alone, in name order,
// and stops at the first one another database's foreign key references:
// measured on MySQL 8.4.11, `schema clean` against a server holding r1 and a
// table of r4 referencing r1.t answers `drop schema named "r1": Error 3730
// (HY000): Cannot drop table 't' referenced by a foreign key constraint 'zfk'
// on table 'z'`. Dropping those keys first empties the server whatever order
// the databases come in. A database the server owns is never listed; see
// [systemschema.IsMySQLSystemDatabase].
func inspectServer(ctx context.Context, conn *dbschema.DatabaseConnection) ([]Object, error) {
	databases, err := serverDatabases(ctx, conn)
	if err != nil {
		return nil, err
	}
	keys, err := crossDatabaseForeignKeys(ctx, conn)
	if err != nil {
		return nil, err
	}
	objects := make([]Object, 0, len(databases)+len(keys))
	objects = append(objects, keys...)
	for _, database := range databases {
		objects = append(objects, Object{Type: ObjectTypeSchema, Schema: database, Name: database})
	}
	return objects, nil
}

// serverDatabases lists the user databases of a whole server.
func serverDatabases(ctx context.Context, conn *dbschema.DatabaseConnection) ([]string, error) {
	rows, err := conn.QueryContext(ctx, systemschema.MySQLUserDatabasesQuery())
	if err != nil {
		return nil, fmt.Errorf("inspect cleanup databases: %w", err)
	}
	defer rows.Close()
	var databases []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan cleanup database: %w", err)
		}
		databases = append(databases, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate cleanup databases: %w", err)
	}
	return databases, nil
}

// crossDatabaseForeignKeys lists the foreign keys of a user database that
// reference a table of another database.
func crossDatabaseForeignKeys(ctx context.Context, conn *dbschema.DatabaseConnection) ([]Object, error) {
	rows, err := conn.QueryContext(ctx, systemschema.MySQLCrossDatabaseForeignKeysQuery())
	if err != nil {
		return nil, fmt.Errorf("inspect cleanup foreign keys between databases: %w", err)
	}
	defer rows.Close()
	var keys []Object
	for rows.Next() {
		var key Object
		if err := rows.Scan(&key.Schema, &key.Table, &key.Name); err != nil {
			return nil, fmt.Errorf("scan cleanup foreign key: %w", err)
		}
		key.Type = ObjectTypeForeignKey
		keys = append(keys, key)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate cleanup foreign keys: %w", err)
	}
	return keys, nil
}
