package atlasschema

import (
	"context"
	"fmt"

	"ptah.run/catalog"
	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/devclean"
	"ptah.run/internal/devlock"
	"ptah.run/internal/sqlident"
)

// A rehearsal on a whole MySQL or MariaDB dev server, a --dev-url that names no
// database (stokaro/ptah#3885).
//
// A rehearsal on one dev database re-scopes the plan into that database,
// claims and resets only it, and recreates the target there. None of that fits
// a plan for a whole server, which creates and drops databases and names each
// table by its database. So the dev server is reached as a server, claimed as
// one, and reset as one: the claim refuses a server holding any user database,
// and the reset drops every user database, the foreign keys between them first
// (stokaro/ptah#3789). The plan runs as written, confined by the replay guard
// of that realm, which refuses a role, a user, a privilege or a stored body the
// reset would leave behind.

// claimSimulationDevServer claims a whole dev server for a rehearsal.
//
// The claim comes first, and it only reads. It refuses a server that holds any
// user database in the pinned community binary v1.3.0's words, `connected
// database is not clean: found schema "app"`, which is also what that binary
// answers when the dev server is the target server and the target holds a
// database, measured on MySQL 8.4.11 and MariaDB 11.8.9. The identity check
// then refuses a target server with no database, reached as the dev server.
//
// A target that is a whole server rehearses on the dev server itself. A target
// naming one database rehearses in a database of the same name, created on the
// dev server; see [openRehearsalDatabase]. On an error nothing this call
// created is left on the dev server, and the caller closes server.
func claimSimulationDevServer(
	ctx context.Context,
	server *dbschema.DatabaseConnection,
	serverURL string,
	target catalog.ServerInfo,
	protected []devlock.Protected,
) (simulationDev, error) {
	serverBaseline, err := devclean.Claim(ctx, server)
	if err != nil {
		return simulationDev{}, err
	}
	if err := devlock.EnsureDistinct(ctx, server, protected...); err != nil {
		return simulationDev{}, err
	}
	if target.WholeServer {
		return simulationDev{conn: server, baseline: serverBaseline, release: func() {}}, nil
	}
	return openRehearsalDatabase(ctx, server, serverURL, serverBaseline, target.Schema)
}

// openRehearsalDatabase creates a database named like the target's on the
// claimed dev server and opens it for a rehearsal of a plan for one database.
//
// The name is the target's, so the plan re-scopes onto a database of the same
// name and the check that every statement stays in it runs unchanged. The
// server was claimed empty, so the name is free. The returned release drops it
// by resetting the server, and then closes the server connection; the caller
// closes the database connection before it releases.
func openRehearsalDatabase(
	ctx context.Context,
	server *dbschema.DatabaseConnection,
	serverURL string,
	serverBaseline devclean.Baseline,
	name string,
) (simulationDev, error) {
	dropCreated := func() { discardDevRehearsalArtifacts(ctx, server, serverBaseline) }
	create := "CREATE DATABASE " + sqlident.Quote(server.Info().Dialect, name)
	if err := executeApplyStatements(ctx, server.Writer(), []string{create}); err != nil {
		dropCreated()
		return simulationDev{}, fmt.Errorf("create the rehearsal database %q on the dev server: %w", name, err)
	}
	databaseURL, err := atlasurl.WithDatabaseName(serverURL, name)
	if err != nil {
		dropCreated()
		return simulationDev{}, err
	}
	conn, err := dbschema.ConnectToDatabase(ctx, databaseURL)
	if err != nil {
		dropCreated()
		return simulationDev{}, fmt.Errorf("connect to the rehearsal database %q on the dev server: %w", name, err)
	}
	baseline, err := devclean.Claim(ctx, conn)
	if err != nil {
		dbschema.CloseAndWarn(conn)
		dropCreated()
		return simulationDev{}, err
	}
	return simulationDev{
		conn:     conn,
		baseline: baseline,
		release: func() {
			dropCreated()
			dbschema.CloseAndWarn(server)
		},
	}, nil
}

// guardRehearsedPlan refuses a statement of a plan rehearsal whose effect the
// cleanup of the dev server would leave behind, before any statement runs.
//
// On a dev database the operator named, that is anything past the database: a
// role, a routine body that writes elsewhere, a comment the reset keeps. The
// baseline is held to the same realm (see [guardRehearsalBaseline]), and the
// plan comes from the less trusted desired schema, so before this the plan was
// the weaker half: a CREATE ROLE ran for real on the shared server, outlived
// the run, and failed the real apply on the role it had left
// (stokaro/ptah#4294). On a whole MySQL or MariaDB dev server it is a role, a
// user, a privilege or a stored body; databases, and everything in them, the
// reset drops; see [devclean.ReplayRealmServerDatabases].
//
// On a server the run owns, started by a docker URL or declared disposable,
// nothing outlives the run that matters, and the rehearsal runs what a
// migration replay there runs; see [devclean.DevReplayRealm]. On any other
// server a refusal that ownership would lift names the two ways to it.
func guardRehearsedPlan(statements []string, dev catalog.ServerInfo) error {
	place := "the dev database"
	if dev.WholeServer {
		place = "a whole dev server"
	}
	guard := devclean.NewDevReplayGuard(dev)
	for i, statement := range statements {
		if err := guard.ValidateStatement(statement); err != nil {
			return fmt.Errorf("statement %d cannot be rehearsed on %s: %w", i+1, place, err)
		}
	}
	return nil
}
