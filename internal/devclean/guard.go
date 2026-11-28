package devclean

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/dbschema"
	"ptah.run/internal/devclean/replay"
	"ptah.run/internal/devclean/replayguards"
	"ptah.run/internal/devdocker"
	"ptah.run/internal/replaytrust"
)

// ReplayGuard rejects migration statements whose effects cannot be confined to
// the disposable database realm cleaned after migration replay.
//
// It answers two questions apart. The realm is ownership: how much of the dev
// server the run may change and later cleans. Trust in the SQL is the
// operator's statement that they accept effects the guard cannot confine, with
// the privileges of the dev connection; see [replay.Scope.TrustedSQL]. Trust
// lifts refusals within the realm and never changes the realm, so a trusted
// replay on a server the run does not own still cleans only its dev database.
type ReplayGuard struct {
	info  catalog.ServerInfo
	realm ReplayRealm
	// trustSQL answers [replay.Scope.TrustedSQL]; see [NewDevReplayGuard].
	trustSQL bool
	// trustRemedy ends a refusal that trust would lift, when trustSQL is not
	// set; see [NewDevReplayGuard].
	trustRemedy string
	// guard is the replay guard of the connection's dialect, and nil for a
	// dialect no engine guards.
	guard replay.Guard
	// remedy ends a refusal that [ReplayRealmServer] lifts; see
	// [ReplayGuard.WithServerRealmRemedy].
	remedy string
	// provisionedRemedy ends a refusal that only
	// [ReplayRealmProvisionedServer] lifts, and is
	// [devdocker.ProvisionedServerRemedy] when empty; see
	// [NewShadowReplayGuardFor].
	provisionedRemedy string
	// baseline words a refusal as the rehearsal baseline's; see
	// [BaselineGuard].
	baseline bool
	// schemaCommentReturned answers [replay.Scope.SchemaCommentReturned] when a
	// statement is validated; see [ReplayGuard.WithSchemaCommentReturned].
	schemaCommentReturned func() bool
	// schemaStateReturned is [replay.Scope.SchemaStateReturned]; see
	// [ReplayGuard.WithSchemaStateReturned].
	schemaStateReturned bool
	// publications answers [replay.Scope.Publications] when a statement is
	// validated; see [ReplayGuard.WithPublications]. Nil is a cleanup that
	// drops no publication.
	publications func() replay.PublicationScope
	// survivors answers [replay.Scope.Survivors] when a statement is
	// validated; see [ReplayGuard.WithSurvivors]. Nil names none.
	survivors func() replay.CleanupSurvivors
	// databaseRoles answers [replay.Scope.DatabaseRoles] when a statement is
	// validated; see [ReplayGuard.WithDatabaseRoles]. Nil is a cleanup that
	// drops no database role.
	databaseRoles func() replay.DatabaseRoleScope
	// session is set only by a caller that discards its pinned replay session.
	session bool
	// ownTransactions is set only by a caller that replays an existing history
	// outside an enclosing transaction; see [ReplayGuard.WithOwnTransactions].
	ownTransactions bool
	// unwrapped reads such a file as a runner that sends it as it is runs it;
	// see [ReplayGuard.WithUnwrappedTransactions].
	unwrapped bool
}

// ReplayRealm is how much of the dev server a replay may change.
type ReplayRealm int

const (
	// ReplayRealmDatabase confines a replay to the dev database. It is the
	// realm of a server the operator named: that server may hold other
	// databases and roles, and a role or a database a replay creates outlives
	// the cleanup, which empties the dev database and nothing else.
	ReplayRealmDatabase ReplayRealm = iota
	// ReplayRealmServer lets a replay change the whole server. It is the realm
	// of a server the run owns as a whole, where a role, another database or a
	// routine body cannot reach anything the run does not own: one the
	// operator declared disposable, and, as [ReplayRealmProvisionedServer],
	// one this process provisioned.
	//
	// It lifts only the refusals whose reason is the realm. A statement that
	// changes the replay session, the server's catalogs or its configuration,
	// or that reaches past the server, is refused here too: those would
	// mislead the rest of the run, or leave the container.
	ReplayRealmServer
	// ReplayRealmServerDatabases confines a replay to the user databases of a
	// MySQL or MariaDB server the operator named as a whole, with a dev URL
	// that names no database (stokaro/ptah#3789). The claim found no user
	// database there and the cleanup drops every one, so the replay may
	// create, change and drop databases and write in any of them. A role, a
	// user, a privilege or a stored body outlives that cleanup, and is refused
	// as it is in [ReplayRealmDatabase].
	ReplayRealmServerDatabases
	// ReplayRealmProvisionedServer is [ReplayRealmServer] on a server this
	// process provisioned and removes afterwards. It also lifts a comment on
	// an extension or a schema of the dev database. A server the operator
	// declared disposable outlives the run, and its reset keeps the extensions
	// and schemas it found, so a comment written there would reach the next
	// run's reading of the dev database. A comment on one the run created is
	// accepted in every realm; see [ReplayGuard.WithSurvivors].
	ReplayRealmProvisionedServer
)

// ownsServer reports a realm that lets a replay change the whole server.
func (r ReplayRealm) ownsServer() bool {
	return r == ReplayRealmServer || r == ReplayRealmProvisionedServer
}

// NewReplayGuard creates a dialect-aware migration replay guard for a replay
// confined to realm.
func NewReplayGuard(info catalog.ServerInfo, realm ReplayRealm) *ReplayGuard {
	return &ReplayGuard{info: info, realm: realm, guard: replayGuard(info.Dialect)}
}

// NewDevReplayGuard is the guard for statements a run executes on the dev
// server reached through info: a migration replay, a desired-state SQL file
// loaded there, and a plan rehearsal with the baseline it restores. The realm
// is [DevReplayRealm]'s, and a refusal the server realm would lift ends with
// [devdocker.OwnedServerRemedy], the two ways to that realm.
//
// The guard trusts the SQL when the run that resolved info.URL trusts it, as
// [replaytrust.Trusted] answers, and a refusal that trust would lift ends with
// [replaytrust.Remedy] otherwise. The answer is read by URL for the reason
// [DevReplayRealm] gives for ownership: every consumer resolves its dev URL
// through internal/devdocker with the command's context, and no guard
// constructor takes one.
func NewDevReplayGuard(info catalog.ServerInfo) *ReplayGuard {
	guard := NewReplayGuard(info, DevReplayRealm(info)).WithServerRealmRemedy(devdocker.OwnedServerRemedy)
	guard.trustSQL = replaytrust.Trusted(info.URL)
	guard.trustRemedy = replaytrust.Remedy
	return guard
}

// replayGuardOwner is a schema writer whose owner judges the statements a run
// replays on it, because the host cannot read the dialect: an executable
// provider's session. A nil guard is an owner that offers none.
type replayGuardOwner interface {
	ReplayGuard() replay.Guard
}

// NewDevReplayGuardFor is [NewDevReplayGuard] for the dev database conn
// reaches. On a connection whose owner judges its own replays, the owner's
// guard reads every statement; an owner that offers no guard leaves every
// statement refused, as a dialect no engine guards is.
//
// A dialect whose guard reads a statement by the session it runs on, as
// [replay.SessionGuard] says, has conn's session read here, once, and the
// guard it returns validates the run's statements. A session the guard
// refuses refuses the run: the error names every way the operator can lift
// the refusal. conn is the session the run executes its statements on, or a
// pool whose every session has the settings and the account that one starts
// with.
func NewDevReplayGuardFor(ctx context.Context, conn *dbschema.DatabaseConnection) (*ReplayGuard, error) {
	return guardFor(ctx, conn, NewDevReplayGuard(conn.Info()))
}

// ShadowTrust is a caller's answer to trust for a replay on a shadow
// database, with the text that ends a refusal the caller can lift.
type ShadowTrust struct {
	// Trusted answers [replay.Scope.TrustedSQL].
	Trusted bool
	// Remedy ends a refusal that trust would lift, when Trusted is false.
	// Empty appends none.
	Remedy string
	// ServerRemedy ends a refusal that a server the run discards would lift.
	// Empty appends none.
	ServerRemedy string
}

// NewShadowReplayGuardFor is [NewDevReplayGuardFor] for a migration history
// replayed on the shadow database conn reaches, with the realm
// [DevReplayRealm] gives. Trust and the remedies are the caller's, not the
// record [replaytrust.Trusted] reads and the text a dev database refusal ends
// with: a shadow database is resolved by a library caller as well as by a
// command, and never declared disposable, so the only server that lifts a
// refusal there is one the run discards, and trust.ServerRemedy says how to
// reach it.
func NewShadowReplayGuardFor(ctx context.Context, conn *dbschema.DatabaseConnection, trust ShadowTrust) (*ReplayGuard, error) {
	guard := NewReplayGuard(conn.Info(), DevReplayRealm(conn.Info())).WithServerRealmRemedy(trust.ServerRemedy)
	guard.provisionedRemedy = trust.ServerRemedy
	guard.trustSQL = trust.Trusted
	guard.trustRemedy = trust.Remedy
	return guardFor(ctx, conn, guard)
}

// guardFor gives guard the replay guard conn's owner offers, when it offers
// one, and refuses a session that guard refuses; see [NewDevReplayGuardFor].
func guardFor(ctx context.Context, conn *dbschema.DatabaseConnection, guard *ReplayGuard) (*ReplayGuard, error) {
	if owner, ok := conn.SchemaWriter().(replayGuardOwner); ok {
		guard.guard = owner.ReplayGuard()
	}
	return guard.forSession(ctx, conn)
}

// forSession returns a copy of the guard that validates the statements a run
// executes on session, as [NewDevReplayGuardFor] does for its connection. A
// dialect whose guard reads a statement by the session it runs on, as
// [replay.SessionGuard] says, has session read here, once; a session the
// guard refuses is an error that names every way the operator can lift the
// refusal. A guard that read no session cannot tell an account a MySQL or
// MariaDB statement spells out from the one the run connects as, and refuses
// the statement.
func (g *ReplayGuard) forSession(ctx context.Context, session replay.Session) (*ReplayGuard, error) {
	guard := *g
	if err := guard.validateSession(ctx, session); err != nil {
		return nil, err
	}
	return &guard, nil
}

// validateSession asks the dialect's guard about session, when the guard
// reads statements by the session they run on, and keeps the guard it
// returns. It ends a refusal with the trust remedy when trusting the SQL
// would lift it. Ownership lifts no such refusal: what the guard misreads, it
// misreads in every realm.
func (g *ReplayGuard) validateSession(ctx context.Context, session replay.Session) error {
	reader, ok := g.guard.(replay.SessionGuard)
	if !ok {
		return nil
	}
	scope := g.realm.scope(g.info.Schema)
	scope.TrustedSQL = g.trustSQL
	guard, err := reader.ValidateSession(ctx, session, scope)
	if err == nil {
		g.guard = guard
		return nil
	}
	var refusal *replay.SessionRefusal
	if g.trustSQL || g.trustRemedy == "" || !errors.As(err, &refusal) {
		return err
	}
	scope.TrustedSQL = true
	if _, trusted := reader.ValidateSession(ctx, session, scope); trusted != nil {
		return err
	}
	return fmt.Errorf("%w; %s", err, g.trustRemedy)
}

// WithSessionScope returns a guard for a replay that owns one pinned physical
// session and discards it on success, failure, and cancellation, or drops its
// session objects with [SessionObjects] where the database keeps that session.
// The caller must enforce that lifecycle. Database and server confinement remain in force.
func (g *ReplayGuard) WithSessionScope() *ReplayGuard {
	guard := *g
	guard.session = true
	return &guard
}

// WithOwnTransactions returns a guard that accepts the transaction a migration
// file opens and closes itself, when the file's whole transaction structure
// is one the dialect's guard accounts for (see [replay.TransactionGuard]).
//
// The caller must run such a file outside any transaction of its own and roll
// the session back when the file fails. A BEGIN inside an enclosing
// transaction is a warning, and the COMMIT after it ends the enclosing one,
// so the file would not run the way its author wrote it. Without this option
// every BEGIN, COMMIT and ROLLBACK stays refused.
func (g *ReplayGuard) WithOwnTransactions() *ReplayGuard {
	guard := *g
	guard.ownTransactions = true
	return &guard
}

// WithUnwrappedTransactions returns a guard that reads a file's own
// transactions the way a runner that sends the file as it is, with no
// transaction around it, runs them, as golang-migrate does (see
// [replay.UnwrappedTransactionGuard]), in place of the reading
// [ReplayGuard.WithOwnTransactions] gives. It changes nothing without that
// option. The caller replays the file the way the apply of its directory runs
// it.
func (g *ReplayGuard) WithUnwrappedTransactions() *ReplayGuard {
	guard := *g
	guard.unwrapped = true
	return &guard
}

// TransactionBoundaries returns the indexes of the statements of one migration
// file that are its own transaction control, or nil when the guard was not
// given [ReplayGuard.WithOwnTransactions] or the dialect's guard reads none.
// A file whose transaction control the dialect's guard does not accept is
// refused as a whole.
func (g *ReplayGuard) TransactionBoundaries(statements []string) ([]int, error) {
	if !g.ownTransactions || g.guard == nil {
		return nil, nil
	}
	if g.unwrapped {
		reader, ok := g.guard.(replay.UnwrappedTransactionGuard)
		if !ok {
			return nil, nil
		}
		return reader.UnwrappedTransactionControl(statements)
	}
	reader, ok := g.guard.(replay.TransactionGuard)
	if !ok {
		return nil, nil
	}
	return reader.TransactionBoundaries(statements)
}

// DevReplayRealm is how much of the dev server reached through info a run may
// change: the whole server when the run owns it, every user database of a
// MySQL-family server whose URL named no database, and the dev database
// otherwise. The run owns a server this process provisioned and removes
// afterwards, and a server the operator declared disposable with
// [devdocker.DisposableServerEnvVar].
//
// Both answers come from the one record [devdocker.RunOwned] reads, keyed on
// the URL the connection was opened with, and the two cases cannot be asked
// apart: a second question would be a second place for the next consumer to
// forget. The answer is not re-derived from the operator's `--dev-url`
// spelling, because every consumer resolves a docker URL before it connects
// and hands the connectable URL on, so by the time statements run no docker
// URL is left to read. [Claim] reads the same record to decide whether the
// server's default database may be reset.
func DevReplayRealm(info catalog.ServerInfo) ReplayRealm {
	if devdocker.Provisioned(info.URL) {
		return ReplayRealmProvisionedServer
	}
	if devdocker.RunOwned(info.URL) {
		return ReplayRealmServer
	}
	// A dev URL naming no MySQL-family database gave the run every user
	// database on the server, and the claim found none there.
	if info.WholeServer {
		return ReplayRealmServerDatabases
	}
	return ReplayRealmDatabase
}

// WithServerRealmRemedy returns a copy of the guard that ends a refusal with
// remedy when [ReplayRealmServer] would have accepted the statement, so the
// operator reads how to reach that realm in the refusal itself.
//
// Whether the server realm would accept it is asked of the server realm's own
// rules, not of a second list: a list kept beside them would agree when it is
// written and stop agreeing when either is extended. A refusal the server realm
// keeps, such as ALTER SYSTEM, ends as it did, because reaching that realm
// would not lift it.
//
// A statement that only [ReplayRealmProvisionedServer] accepts ends with
// [devdocker.ProvisionedServerRemedy] instead, since declaring the server
// disposable would not lift it. Only a caller whose realm the operator can
// raise to the server passes a remedy; a guard built for a fixed realm has
// nothing to suggest. A guard for [ReplayRealmProvisionedServer] appends
// neither, since it keeps every refusal it makes.
func (g *ReplayGuard) WithServerRealmRemedy(remedy string) *ReplayGuard {
	guard := *g
	guard.remedy = remedy
	return &guard
}

// ValidateStatement rejects statements whose effects cannot be confined to
// the replay database realm.
func (g *ReplayGuard) ValidateStatement(stmt string) error {
	err := g.validate(stmt, g.realm, g.trustSQL)
	if err == nil {
		return nil
	}
	if unsafe, ok := errors.AsType[*replay.Refusal](err); ok && g.baseline {
		refusal := *unsafe
		refusal.Baseline = true
		err = &refusal
	}
	return g.withRemedy(stmt, err)
}

// WithSchemaStateReturned returns a copy of the guard for a plan rehearsal, or
// the baseline under it, on a dev database whose every reset returns the schemas
// it keeps to what the claim recorded; see
// [BaselineGuard.WithSchemaStateReturned].
func (g *ReplayGuard) WithSchemaStateReturned(returned bool) *ReplayGuard {
	guard := *g
	guard.schemaStateReturned = returned
	return &guard
}

// WithPublications returns a copy of the guard for a run whose cleanups do with
// the dev database's publications what publications answers, as
// [Baseline.Publications] does for a claim. A replay can build its guard
// before it claims the database, so publications is asked when a statement is
// validated.
//
// On PostgreSQL the copy accepts the publication DDL of a run whose every
// cleanup drops the publications the run created, and refuses a change to or
// the drop of one the claim found. A guard without it refuses every
// publication statement, since nothing would take a publication away.
func (g *ReplayGuard) WithPublications(publications func() replay.PublicationScope) *ReplayGuard {
	guard := *g
	guard.publications = publications
	return &guard
}

// WithDatabaseRoles returns a copy of the guard for a run whose cleanups do
// with the dev database's database roles what roles answers, as
// [Baseline.DatabaseRoles] does for a claim. It is asked when a statement is
// validated, as [ReplayGuard.WithPublications] is.
//
// On SQL Server the copy accepts CREATE ROLE, and DROP ROLE of a role the
// claim did not find, for a run whose every cleanup drops the database roles
// the run created. A guard without it refuses every database principal, since
// nothing would take one away.
func (g *ReplayGuard) WithDatabaseRoles(roles func() replay.DatabaseRoleScope) *ReplayGuard {
	guard := *g
	guard.databaseRoles = roles
	return &guard
}

// WithSurvivors returns a copy of the guard for a run whose every cleanup
// leaves in place the extensions and schemas survivors names and drops the
// others, as [Baseline.Survivors] answers for a claim. A replay can build its
// guard before it claims the database, so survivors is asked when a statement
// is validated.
//
// On the PostgreSQL family the copy accepts a comment on an extension or a
// schema survivors does not name: the run created it, and every cleanup drops
// it with its comment. pg_dump writes such a comment for every extension it
// dumps, and Ptah renders one for an extension that declares a comment. A
// comment on one survivors names stays refused, since the next run would read
// it as the dev database's own.
func (g *ReplayGuard) WithSurvivors(survivors func() replay.CleanupSurvivors) *ReplayGuard {
	guard := *g
	guard.survivors = survivors
	return &guard
}

// WithSchemaCommentReturned returns a copy of the guard for a migration replay on
// a dev database whose every reset may return the schemas it keeps to what the
// claim recorded. A replay builds its guard before it claims the database, so
// returned is asked when a statement is validated, and answers
// [Baseline.ReturnsSchemaState] for the claim.
//
// The copy accepts a comment on a schema where returned answers true: the reset
// gives the comment back, so the effect is confined whoever wrote the statement.
// The pinned community binary v1.3.0 replays the same files, measured on
// PostgreSQL 18. A privilege on a schema stays refused, because it depends on the
// server's roles, and so does a comment on a protected namespace.
func (g *ReplayGuard) WithSchemaCommentReturned(returned func() bool) *ReplayGuard {
	guard := *g
	guard.schemaCommentReturned = returned
	return &guard
}

// withRemedy ends err, the refusal of stmt, with every way the operator can
// lift it: the guard's remedy when the server realm would have accepted stmt,
// [devdocker.ProvisionedServerRemedy] when only a provisioned server would, and
// the trust remedy when trusting the SQL would. Ownership comes first, since it
// is the remedy that keeps the run confined.
func (g *ReplayGuard) withRemedy(stmt string, err error) error {
	remedies := make([]string, 0, 2)
	if remedy := g.serverRemedy(stmt); remedy != "" {
		remedies = append(remedies, remedy)
	}
	if g.trustRemedy != "" && !g.trustSQL && g.validate(stmt, g.realm, true) == nil {
		remedies = append(remedies, g.trustRemedy)
	}
	if len(remedies) == 0 {
		return err
	}
	return fmt.Errorf("%w; %s", err, strings.Join(remedies, "; "))
}

// serverRemedy is the ownership remedy of stmt's refusal, or "" when owning the
// server would not lift it; see [ReplayGuard.WithServerRealmRemedy]. It is
// asked with the guard's own trust, so a trusted run is not told to own a
// server for a refusal trust has already decided.
func (g *ReplayGuard) serverRemedy(stmt string) string {
	switch {
	case g.remedy == "" || g.realm == ReplayRealmProvisionedServer:
		return ""
	case g.realm != ReplayRealmServer && g.validate(stmt, ReplayRealmServer, g.trustSQL) == nil:
		return g.remedy
	case g.validate(stmt, ReplayRealmProvisionedServer, g.trustSQL) == nil:
		if g.provisionedRemedy != "" {
			return g.provisionedRemedy
		}
		return devdocker.ProvisionedServerRemedy
	}
	return ""
}

// validate is [ReplayGuard.ValidateStatement] for one realm and one answer to
// trust. It asks the guard of the connection's dialect, and refuses a dialect
// no engine guards.
func (g *ReplayGuard) validate(stmt string, realm ReplayRealm, trusted bool) error {
	if g.guard == nil {
		unsupportedDialect := strings.TrimSpace(g.info.Dialect)
		if unsupportedDialect == "" {
			unsupportedDialect = "unknown"
		}
		return replay.Refuse(unsupportedDialect, "unsupported dialect")
	}
	scope := realm.scope(g.info.Schema)
	scope.Session = g.session
	scope.SchemaStateReturned = g.schemaStateReturned
	scope.SchemaCommentReturned = g.schemaCommentReturned != nil && g.schemaCommentReturned()
	scope.TrustedSQL = trusted
	if g.publications != nil {
		scope.Publications = g.publications()
	}
	if g.survivors != nil {
		scope.Survivors = g.survivors()
	}
	if g.databaseRoles != nil {
		scope.DatabaseRoles = g.databaseRoles()
	}
	return g.guard.Validate(stmt, scope)
}

// replayGuard returns the guard of the engine dialect names, folded through
// platform.NormalizeDialect, and nil when no engine guards the dialect.
func replayGuard(dialect string) replay.Guard {
	name := platform.NormalizeDialect(dialect)
	for _, service := range replayguards.All() {
		if name != "" && service.Dialect == name {
			return service.Guard
		}
	}
	return nil
}

// scope is what realm lets a statement change, on the dev connection that
// selected database.
func (r ReplayRealm) scope(database string) replay.Scope {
	return replay.Scope{
		Database:    database,
		Server:      r.ownsServer(),
		Provisioned: r == ReplayRealmProvisionedServer,
		Databases:   r == ReplayRealmServerDatabases,
	}
}
