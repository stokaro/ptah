package devclean

import (
	"errors"
	"fmt"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/dbschema"
	"ptah.run/internal/devclean/replay"
	"ptah.run/internal/devclean/replayguards"
	"ptah.run/internal/devdocker"
)

// ReplayGuard rejects migration statements whose effects cannot be confined to
// the disposable database realm cleaned after migration replay.
type ReplayGuard struct {
	info  catalog.ServerInfo
	realm ReplayRealm
	// guard is the replay guard of the connection's dialect, and nil for a
	// dialect no engine guards.
	guard replay.Guard
	// remedy ends a refusal that [ReplayRealmServer] lifts; see
	// [ReplayGuard.WithServerRealmRemedy].
	remedy string
	// baseline words a refusal as the rehearsal baseline's; see
	// [BaselineGuard].
	baseline bool
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
	// run's reading of the dev database.
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
// server reached through info: a migration replay, and a plan rehearsal on a
// whole MySQL or MariaDB server. The realm is [DevReplayRealm]'s, and a
// refusal the server realm would lift ends with [devdocker.OwnedServerRemedy],
// the two ways to that realm.
func NewDevReplayGuard(info catalog.ServerInfo) *ReplayGuard {
	return NewReplayGuard(info, DevReplayRealm(info)).WithServerRealmRemedy(devdocker.OwnedServerRemedy)
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
func NewDevReplayGuardFor(conn *dbschema.DatabaseConnection) *ReplayGuard {
	guard := NewDevReplayGuard(conn.Info())
	if owner, ok := conn.SchemaWriter().(replayGuardOwner); ok {
		guard.guard = owner.ReplayGuard()
	}
	return guard
}

// WithSessionScope returns a guard for a replay that owns one pinned physical
// session and discards it on success, failure, and cancellation. The caller
// must enforce that lifecycle. Database and server confinement remain in force.
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
	err := g.validate(stmt, g.realm)
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

// withRemedy ends err, the refusal of stmt, with the guard's remedy when the
// server realm would have accepted stmt, and with
// [devdocker.ProvisionedServerRemedy] when only a provisioned server would.
func (g *ReplayGuard) withRemedy(stmt string, err error) error {
	switch {
	case g.remedy == "" || g.realm == ReplayRealmProvisionedServer:
		return err
	case g.realm != ReplayRealmServer && g.validate(stmt, ReplayRealmServer) == nil:
		return fmt.Errorf("%w; %s", err, g.remedy)
	case g.validate(stmt, ReplayRealmProvisionedServer) == nil:
		return fmt.Errorf("%w; %s", err, devdocker.ProvisionedServerRemedy)
	}
	return err
}

// validate is [ReplayGuard.ValidateStatement] for one realm. It asks the
// guard of the connection's dialect, and refuses a dialect no engine guards.
func (g *ReplayGuard) validate(stmt string, realm ReplayRealm) error {
	if g.guard == nil {
		unsupportedDialect := strings.TrimSpace(g.info.Dialect)
		if unsupportedDialect == "" {
			unsupportedDialect = "unknown"
		}
		return replay.Refuse(unsupportedDialect, "unsupported dialect")
	}
	scope := realm.scope(g.info.Schema)
	scope.Session = g.session
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
