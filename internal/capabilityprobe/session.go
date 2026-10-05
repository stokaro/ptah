package capabilityprobe

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"path"
	"slices"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/dbschema"
	"ptah.run/internal/sqlident"
	"ptah.run/internal/ydbpool"
	"ptah.run/internal/ydbready"
	"ptah.run/internal/ydbtype"
	"ptah.run/internal/ydburl"
)

// Attempt is one statement and the server's answer to it.
//
// Both halves are kept. A row that says only "the server refused" cannot be
// argued with; a row that quotes the statement and the server's own error text
// can be re-executed by hand from the report.
type Attempt struct {
	Statement string
	Accepted  bool
	ServerErr string
}

// String renders the attempt as one line of evidence.
func (a Attempt) String() string {
	if a.Accepted {
		return "ACCEPTED  " + collapse(a.Statement)
	}
	return "REFUSED   " + collapse(a.Statement) + "  -> " + collapse(a.ServerErr)
}

func collapse(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

// session is a pinned physical connection inside a throwaway namespace.
//
// It is pinned because the namespace is session state: a pooled connection
// would run half the statements in the probe schema and half in the caller's.
// It is a namespace rather than a transaction because PostgreSQL refuses
// CREATE INDEX CONCURRENTLY inside an explicit transaction block, so
// BEGIN/ROLLBACK isolation would report two true capabilities as false.
type session struct {
	conn      *dbschema.DatabaseConnection
	dialect   string
	namespace string
	// database is the absolute path of the YDB database the run works in,
	// such as /local, and empty on every other dialect: a YDB namespace is a
	// directory in it, and the catalog reads that locate an object take
	// absolute paths.
	database string
	// prefix is sent ahead of every statement. On YDB it is the pragma that
	// makes the namespace directory the place an unqualified name means; the
	// pragma lasts one query, so it has to travel with each. Empty elsewhere,
	// where the namespace is session state the enter statements set once.
	prefix string
	// roles are cluster-scoped objects the probe created; dropping the
	// namespace does not remove them. On YDB they are groups.
	roles []string
	// users are the YDB users the probe created, which DROP GROUP does not
	// reach.
	users []string
	// resourcePools and resourcePoolClassifiers are the YDB resource pools
	// and classifiers the probe created. Both belong to the database, so
	// removing the namespace directory leaves them.
	resourcePools           []string
	resourcePoolClassifiers []string
	// rowPolicies are the same kind of leftover on ClickHouse. Measured on
	// 26.7.3.19: dropping the database a policy names leaves the policy behind
	// in system.row_policies, so a run that forgot them would make the next one
	// inherit objects it did not create.
	rowPolicies []string
	// broken records the transport failure that ended the run, if any.
	broken error
	// inExplicitTransaction suspends the liveness check.
	//
	// One decider deliberately provokes a refusal inside an explicit
	// transaction block, and PostgreSQL then answers every statement —
	// including SELECT 1 — with "current transaction is aborted". Reading that
	// as a dead session would abort the run at exactly the statement the run
	// exists to make. The check resumes after the ROLLBACK, where a truly dead
	// session still fails.
	inExplicitTransaction bool
}

// exec runs one statement and reports what the server did.
//
// A refusal is an observation, not an error: the whole point of the probe is
// that some statements must be refused. A DROPPED CONNECTION is a different
// thing entirely, and reading it as a refusal would report every remaining
// capability as absent. So after any failure the session is asked whether it
// is still alive, and a dead session poisons the run instead of answering it.
func (s *session) exec(ctx context.Context, statement string) Attempt {
	if s.broken != nil {
		return Attempt{Statement: statement, ServerErr: "session already broken: " + s.broken.Error()}
	}
	_, err := s.conn.ExecContext(ctx, s.prefix+statement)
	if err == nil {
		return Attempt{Statement: statement, Accepted: true}
	}
	if s.inExplicitTransaction {
		return Attempt{Statement: statement, ServerErr: err.Error()}
	}
	if alive := s.alive(ctx); alive != nil {
		s.broken = fmt.Errorf("session died executing %q: %w (statement error: %w)", statement, alive, err)
		return Attempt{Statement: statement, ServerErr: s.broken.Error()}
	}
	return Attempt{Statement: statement, ServerErr: err.Error()}
}

// query runs a single-value query and reports the value alongside the attempt.
func (s *session) query(ctx context.Context, statement string) (int64, Attempt) {
	if s.broken != nil {
		return 0, Attempt{Statement: statement, ServerErr: "session already broken: " + s.broken.Error()}
	}
	var value int64
	err := s.conn.QueryRowContext(ctx, s.prefix+statement).Scan(&value)
	if err == nil {
		return value, Attempt{Statement: statement, Accepted: true}
	}
	if alive := s.alive(ctx); alive != nil {
		s.broken = fmt.Errorf("session died executing %q: %w (statement error: %w)", statement, alive, err)
		return 0, Attempt{Statement: statement, ServerErr: s.broken.Error()}
	}
	return 0, Attempt{Statement: statement, ServerErr: err.Error()}
}

// alive returns nil while the session can still answer.
func (s *session) alive(ctx context.Context) error {
	statement := livenessSQL(s.dialect)
	var one int
	if err := s.conn.QueryRowContext(ctx, statement).Scan(&one); err != nil {
		return err
	}
	if one != 1 {
		return fmt.Errorf("session answered %s with %d", statement, one)
	}
	return nil
}

// livenessSQL is the smallest query the dialect answers.
//
// `SELECT 1` needs no FROM clause only from Oracle 23: measured, 21.3 answers
// ORA-00923, FROM keyword not found where expected. That turned every ordinary
// REFUSED verdict on 21 into a dead session, because the check that asks
// whether the connection survived was itself refused -- so the run ended at its
// own nonsense control, before a single capability question. YDB answers
// `SELECT 1` on every line from 25.1.4.7 to 26.2.1.14, and the check needs no
// namespace pragma, so it is sent without one.
func livenessSQL(dialect string) string {
	if platform.NormalizeDialect(dialect) == platform.Oracle {
		return "SELECT 1 FROM dual"
	}
	return "SELECT 1"
}

// runAll executes statements in order and stops at the first refusal.
func (s *session) runAll(ctx context.Context, statements []string) ([]Attempt, bool) {
	attempts := make([]Attempt, 0, len(statements))
	for _, statement := range statements {
		attempt := s.exec(ctx, statement)
		attempts = append(attempts, attempt)
		if !attempt.Accepted {
			return attempts, false
		}
	}
	return attempts, true
}

// tryInTransaction runs one statement inside an explicit transaction block and
// always rolls back.
//
// The block is the measurement, not the isolation: a real concurrent index
// build cannot live inside a transaction and a parser that merely swallows the
// CONCURRENTLY keyword has no reason to refuse it, so the refusal is the
// evidence. Liveness checking is suspended for the duration because an aborted
// PostgreSQL transaction answers SELECT 1 with an error too, and resumes on the
// far side of the ROLLBACK.
func (s *session) tryInTransaction(ctx context.Context, statement string) ([]Attempt, bool) {
	begin := s.exec(ctx, beginSQL(s.dialect))
	if !begin.Accepted {
		return []Attempt{begin}, false
	}
	s.inExplicitTransaction = true
	inside := s.exec(ctx, statement)
	s.inExplicitTransaction = false
	rollback := s.exec(ctx, "ROLLBACK")
	return []Attempt{begin, inside, rollback}, true
}

// beginSQL opens an explicit transaction block in the dialect's own spelling.
//
// `BEGIN` alone is not a transaction on SQL Server. It opens a BEGIN/END
// statement block, and the bare word answers `Could not find stored procedure
// 'BEGIN'` -- a refusal the caller would read as a server that declines
// transactions. ROLLBACK needs no arm: T-SQL takes it as written.
//
// YDB has no arm because no YDB experiment opens a block: YQL has no BEGIN, a
// transaction is the driver's wrapper around a query, and a schema statement
// is refused inside one. The YDB plan says which keys that leaves undecided.
func beginSQL(dialect string) string {
	if platform.NormalizeDialect(dialect) == platform.SQLServer {
		return "BEGIN TRANSACTION"
	}
	return "BEGIN"
}

// nonsenseControl is the statement the server MUST refuse.
//
// Every "the server accepted it" row is only worth reading if the server is
// capable of saying no on this session. A connection that answers OK to
// everything — a proxy that swallows DDL, a dry-run wrapper, a driver that
// defers errors — would otherwise fill the matrix with agreements nobody
// earned. The capability presets already use this exact control in their own
// doc comments; here it gates the whole run.
const nonsenseControl = "CREATE NONSENSE ptah_capability_probe_control"

// namespaceSQL returns the statements that create and enter the throwaway
// namespace, and the statement that removes it.
func namespaceSQL(dialect, namespace string) (enter []string, leave string) {
	if platform.IsPostgresFamily(dialect) {
		return []string{
				"CREATE SCHEMA " + namespace,
				"SET search_path TO " + namespace,
			},
			"DROP SCHEMA " + namespace + " CASCADE"
	}
	if platform.NormalizeDialect(dialect) == platform.Oracle {
		// In Oracle a schema IS a user, so the throwaway namespace is an
		// account. CREATE DATABASE below is an instance-level statement there
		// and answers ORA-01501 against a mounted database, which is what a
		// probe run reported before this arm existed.
		//
		// It needs a privileged connection -- an ordinary account answers
		// ORA-01031, insufficient privileges -- which is the same requirement
		// the CREATE DATABASE arm carries for MySQL.
		//
		// Measured on 23.26 that the isolation is real rather than merely
		// accepted: after ALTER SESSION SET CURRENT_SCHEMA, an unqualified
		// CREATE TABLE lands with the throwaway account as its owner, which is
		// exactly what confirmNamespace goes on to check. The Spanner failure
		// that check exists for -- a namespace accepted and then ignored --
		// does not happen here.
		return []string{
				"CREATE USER " + namespace + " IDENTIFIED BY ptah_capability_probe QUOTA UNLIMITED ON users",
				// The privileges an ordinary schema owner has, and no more.
				//
				// They are granted because without them the probe measures the
				// ACCOUNT rather than the engine: measured, a namespace with
				// only a quota answers ORA-01031, insufficient privileges, to
				// CREATE MATERIALIZED VIEW -- and the run recorded that as the
				// server not supporting materialized views. The same refusal
				// silently agreed with the preset on role_management, which is
				// the worse half: a privilege the account lacked read as a
				// capability the engine lacks.
				"GRANT CREATE SESSION, CREATE TABLE, CREATE VIEW, CREATE MATERIALIZED VIEW, " +
					"CREATE SEQUENCE, CREATE TRIGGER, CREATE PROCEDURE, CREATE TYPE, " +
					"CREATE SYNONYM, CREATE ROLE TO " + namespace,
				"ALTER SESSION SET CURRENT_SCHEMA = " + namespace,
			},
			"DROP USER " + namespace + " CASCADE"
	}
	if platform.NormalizeDialect(dialect) == platform.SQLServer {
		// The leave statement changes database before it drops one, because
		// SQL Server refuses to drop the database the session is sitting in:
		// measured, `DROP DATABASE ptah_capprobe_...` from inside it answers
		// `Cannot drop database "..." because it is currently in use.` Both
		// halves travel in one batch because the caller has one string to
		// spend, and T-SQL applies USE to the statements that follow it in the
		// same batch.
		return []string{
				"CREATE DATABASE " + namespace,
				"USE " + namespace,
			},
			"USE master; DROP DATABASE " + namespace
	}
	if platform.NormalizeDialect(dialect) == platform.YDB {
		// YDB has no CREATE SCHEMA or CREATE DATABASE. The namespace is a
		// directory under the database root, which the first CREATE TABLE in
		// it creates, and every statement is sent after the pragma that names
		// it (see session.prefix). No SQL removes a directory, so the scheme
		// service removes it with everything in it; see session.leave.
		return nil, ""
	}
	if platform.NormalizeDialect(dialect) == platform.SQLite {
		// SQLite has no namespace inside a database to create and enter: there
		// is no CREATE DATABASE, and no statement that switches schema. The
		// throwaway namespace is the database the probe was pointed at, which
		// is why the matrix addresses this line with its own in-memory
		// database rather than a file something else might hold.
		//
		// An empty leave says there is nothing to drop, and measure skips it.
		// A statement here would have to be one that does nothing, and a
		// cleanup row reporting a no-op reads as a cleanup that ran.
		return nil, ""
	}
	return []string{
			"CREATE DATABASE " + namespace,
			"USE " + namespace,
		},
		"DROP DATABASE " + namespace
}

// namespaceIsolatesTheRun reports whether entering the throwaway namespace
// separates what this run creates from whatever else the target holds.
//
// It is false for exactly one dialect, and the consequence is the point.
// SQLite's namespace is the database itself, so every object the run creates
// lands beside whatever that database already holds -- which is the state
// namespaceProblem refuses. The run is therefore allowed on an empty database
// and on no other, and the occupancy count it already takes is what decides.
func namespaceIsolatesTheRun(dialect string) bool {
	return platform.NormalizeDialect(dialect) != platform.SQLite
}

// sentinelKeyType names a 64-bit integer in the dialect's own spelling.
//
// `bigint` is not universal, and the sentinel is the one statement that must
// succeed for a run to start: Oracle answers ORA-00902, invalid datatype, so a
// probe there failed before it asked a single capability question.
func sentinelKeyType(dialect string) string {
	if platform.NormalizeDialect(dialect) == platform.Oracle {
		return "NUMBER(19)"
	}
	return "bigint"
}

// sentinelTable is the object confirmNamespace creates to find out where
// unqualified DDL actually lands.
const sentinelTable = "ptah_capprobe_sentinel"

// confirmNamespace proves the throwaway namespace took effect, and refuses the
// run when it did not.
//
// Entering it is not evidence that it applies. Measured on the Cloud Spanner
// emulator through PGAdapter: CREATE SCHEMA is accepted, SET search_path is
// accepted, and an unqualified CREATE TABLE lands in `public` regardless. Every
// object a run creates then outlives it, the DROP at the end removes an empty
// schema, and the next run against the same server reads the previous run's
// leftovers as its own findings -- which is the exact failure newNamespace
// exists to prevent, arriving through a different door.
//
// It is not hypothetical and it is not loud. Two runs against one server
// answered differently, both exiting non-zero for unrelated-looking reasons:
// nine capability disagreements on the fresh server, three on the second run,
// with thirteen `Duplicate name in schema` refusals in between
// (stokaro/ptah#942).
//
// The check is one sentinel table and one catalog count, so it costs nothing
// and it holds for every dialect: a namespace that stops applying anywhere is
// caught the first time it happens rather than the first time somebody notices
// two runs disagreeing.
func (s *session) confirmNamespace(ctx context.Context) ([]Attempt, error) {
	attempts := s.createSentinel(ctx)
	created := attempts[len(attempts)-1]
	if !created.Accepted {
		return attempts, fmt.Errorf(
			"the throwaway namespace %s could not be confirmed: creating the sentinel table was refused (%s)",
			s.namespace, created.ServerErr)
	}

	// A dialect with no namespace to enter has nothing to ask the catalog:
	// the sentinel landed in the database the probe connected to, and zero
	// here is the true answer to "did it land in the throwaway namespace".
	// What follows then decides on occupancy alone, which is the rule that
	// belongs to such a target.
	var count int64
	asked := Attempt{Statement: "no namespace to locate the sentinel in", Accepted: true}
	if namespaceIsolatesTheRun(s.dialect) {
		count, asked = s.query(ctx, sentinelLocationSQL(s.dialect, s.database, s.namespace))
		attempts = append(attempts, asked)
	}
	dropped := s.exec(ctx, "DROP TABLE "+sentinelTable)
	attempts = append(attempts, dropped)

	if !asked.Accepted {
		return attempts, fmt.Errorf(
			"the throwaway namespace %s could not be confirmed: the catalog would not say where the sentinel table landed (%s)",
			s.namespace, asked.ServerErr)
	}
	// The occupancy count is taken on every run rather than only when the
	// namespace failed, so the decision below is a pure function of two
	// numbers and can be measured without a server.
	occupants, counted := s.query(ctx, occupancySQLFor(s.dialect, s.database))
	attempts = append(attempts, counted)
	if !counted.Accepted {
		return attempts, fmt.Errorf(
			"the throwaway namespace %s could not be confirmed: the catalog would not say what else is on this server (%s)",
			s.namespace, counted.ServerErr)
	}
	return attempts, namespaceProblem(s.namespace, count, occupants)
}

// createSentinel creates the sentinel table, which is the first schema change
// of the run, and returns every attempt it took.
//
// On YDB it waits out the moment a fresh server answers queries and refuses
// to create a table, through [ydbready.Until]: the connection the matrix waits
// for is not yet a server that can be measured, and the sentinel is retried
// while the server says so, a bounded number of times.
func (s *session) createSentinel(ctx context.Context) []Attempt {
	var attempts []Attempt
	try := func(context.Context) error {
		created := s.exec(ctx, sentinelTableSQL(s.dialect))
		attempts = append(attempts, created)
		if created.Accepted {
			return nil
		}
		return errors.New(created.ServerErr)
	}
	// The attempts carry every answer, so the error each call returns, which
	// repeats the last of them, is not kept.
	if platform.NormalizeDialect(s.dialect) != platform.YDB {
		_ = try(ctx)
		return attempts
	}
	_ = ydbready.Until(ctx, try)
	return attempts
}

// occupancySQL counts the tables on the server that are not the catalog's own.
// The sentinel is dropped before this runs, so a server the probe has to itself
// counts zero.
const occupancySQL = "SELECT COUNT(*) FROM information_schema.tables " +
	"WHERE table_schema NOT IN ('information_schema', 'pg_catalog', 'spanner_sys', " +
	"'mysql', 'performance_schema', 'sys')"

// oracleOccupancySQL is occupancySQL for a catalog that has no
// information_schema.
//
// The exclusion is a fact the server records rather than a list of names:
// ALL_USERS.ORACLE_MAINTAINED is 'Y' for every account Oracle created for
// itself, and a list would go stale the first time a release added one.
// Measured on 23.26 and 21.3, the count moves 0 -> 1 -> 0 as one user table
// appears and is dropped, so it is a count rather than a constant.
const oracleOccupancySQL = "SELECT COUNT(*) FROM all_tables t " +
	"JOIN all_users u ON u.username = t.owner WHERE u.oracle_maintained = 'N'"

// sqliteOccupancySQL is occupancySQL for a catalog that is one relation.
//
// SQLite has no information_schema and no namespace column to exclude: every
// object in the database is in sqlite_master, and the ones the engine made for
// itself carry the reserved sqlite_ prefix. The count is what decides whether
// a run may proceed at all here, because the database IS the namespace.
const sqliteOccupancySQL = "SELECT COUNT(*) FROM sqlite_master " +
	"WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"

// occupancySQLFor returns the statement that counts what else is on the server.
// database is the YDB database path and unused elsewhere.
//
// YDB has no information_schema. .sys/partition_stats lists a row for every
// partition of every row table, with its absolute path, and measured on
// 26.2.1.14 it lists a table the moment the CREATE returns and drops it the
// moment the DROP does. The dot-directories are the server's own, which is
// the rule the schema reader applies too.
func occupancySQLFor(dialect, database string) string {
	switch platform.NormalizeDialect(dialect) {
	case platform.Oracle:
		return oracleOccupancySQL
	case platform.SQLite:
		return sqliteOccupancySQL
	case platform.YDB:
		// Concatenated rather than joined: path.Join would clean the dot away
		// and exclude every table in the database.
		return fmt.Sprintf("SELECT COUNT(DISTINCT Path) FROM %s WHERE NOT StartsWith(Path, %s)",
			ydbSystemView(database, "partition_stats"), ydbString(strings.TrimSuffix(database, "/")+"/."))
	default:
		return occupancySQL
	}
}

// sentinelTableSQL creates the sentinel in the dialect's own spelling. YQL
// declares a key only in its own clause: `n Int64 PRIMARY KEY` is a parse
// error on every line.
func sentinelTableSQL(dialect string) string {
	if platform.NormalizeDialect(dialect) == platform.YDB {
		return "CREATE TABLE " + sentinelTable + " (n Int64 NOT NULL, PRIMARY KEY (n))"
	}
	return "CREATE TABLE " + sentinelTable + " (n " + sentinelKeyType(dialect) + " PRIMARY KEY)"
}

// sentinelLocationSQL asks the catalog where the sentinel table landed.
// database is the YDB database path and unused elsewhere.
//
// Oracle's answer comes from ALL_TABLES keyed by OWNER, and both halves are
// upper-cased because an unquoted identifier is folded there: the namespace is
// created as `ptah_capprobe_...` and stored as PTAH_CAPPROBE_....
func sentinelLocationSQL(dialect, database, namespace string) string {
	if platform.NormalizeDialect(dialect) == platform.YDB {
		// A YDB table is a path, and the namespace is the directory the
		// pragma names, so the sentinel landed there when the partition
		// statistics list it under that directory.
		return fmt.Sprintf("SELECT COUNT(DISTINCT Path) FROM %s WHERE Path = %s",
			ydbSystemView(database, "partition_stats"), ydbString(path.Join(database, namespace, sentinelTable)))
	}
	if platform.NormalizeDialect(dialect) == platform.Oracle {
		return fmt.Sprintf(
			"SELECT COUNT(*) FROM all_tables WHERE table_name = UPPER('%s') AND owner = UPPER('%s')",
			sentinelTable, namespace)
	}
	if platform.NormalizeDialect(dialect) == platform.SQLServer {
		// A SQL Server namespace is a database, and its information_schema
		// names one in TABLE_CATALOG. TABLE_SCHEMA holds `dbo` for every table
		// the probe creates, so the shared projection below counts zero and the
		// run reads as a namespace that was entered and then ignored.
		return fmt.Sprintf(
			"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '%s' AND table_catalog = '%s'",
			sentinelTable, namespace)
	}
	return fmt.Sprintf(
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '%s' AND table_schema = '%s'",
		sentinelTable, namespace)
}

// namespaceProblem decides whether a run may proceed, from where the sentinel
// landed and what else is on the server.
//
// A namespace that applies is the whole answer and the server's contents are
// none of the probe's business. A namespace that does not apply is survivable
// on a server this run has to itself and on no other: the objects will land
// beside whatever is already there, outlive the run, and be read by the next
// run as findings.
//
// It is deliberately one run per server rather than a cleanup pass. The run's
// own leftovers make the NEXT run refuse, which is the same protection arriving
// one step later -- where a cleanup pass that missed an object would instead
// hand it over silently.
func namespaceProblem(namespace string, sentinelInNamespace, occupants int64) error {
	if sentinelInNamespace == 1 {
		return nil
	}
	if occupants > 0 {
		return fmt.Errorf(
			"the throwaway namespace %s was entered but does not apply, and this server already holds %d table(s): "+
				"objects this run creates would land beside them and be read as findings by the next run. "+
				"Point the probe at a server of its own",
			namespace, occupants)
	}
	return nil
}

// newNamespace returns a fresh identifier no other run will collide with. The
// machine this is developed on routinely has forty containers and several
// agents pointed at the same server, and a fixed name turns another run's
// leftovers into this run's capability findings.
func newNamespace() (string, error) {
	raw := make([]byte, 8)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generate probe namespace: %w", err)
	}
	return "ptah_capprobe_" + hex.EncodeToString(raw), nil
}

// dropRole removes one role in the dialect's own spelling.
//
// Oracle has neither DROP OWNED BY nor an IF EXISTS guard on DROP ROLE, so the
// PostgreSQL pair below would leave the role behind and report two refusals
// while doing it.
func (s *session) dropRole(ctx context.Context, role string) []Attempt {
	switch platform.NormalizeDialect(s.dialect) {
	case platform.Oracle:
		return []Attempt{s.exec(ctx, "DROP ROLE "+role)}
	case platform.YDB:
		// The YDB role is a group, and a group owns nothing a drop would
		// have to reassign. Its grants on the namespace leave with the
		// directory; the one on the database does not, and DROP GROUP
		// leaves it behind (measured on 25.1.4.7 and 26.2.1.14), so it is
		// revoked first.
		return []Attempt{s.revokeOnDatabase(ctx, role), s.exec(ctx, "DROP GROUP IF EXISTS "+role)}
	}
	return []Attempt{
		s.exec(ctx, "DROP OWNED BY "+role),
		s.exec(ctx, "DROP ROLE IF EXISTS "+role),
	}
}

// dropRoles removes the cluster-scoped roles the probe created. Roles outlive
// the schema, so a run that forgot them would leave the server dirtier every
// time it ran.
func (s *session) dropRoles(ctx context.Context) []Attempt {
	attempts := make([]Attempt, 0, 2*len(s.roles)+len(s.rowPolicies))
	for _, statement := range s.rowPolicies {
		attempts = append(attempts, s.exec(ctx, statement))
	}
	for _, user := range s.users {
		attempts = append(attempts, s.revokeOnDatabase(ctx, user), s.exec(ctx, "DROP USER IF EXISTS "+user))
	}
	for _, role := range s.roles {
		attempts = append(attempts, s.dropRole(ctx, role)...)
	}
	return attempts
}

// dropResourcePools removes the YDB classifiers the probe created and then
// its pools, which outlive the namespace directory the way a user does.
func (s *session) dropResourcePools(ctx context.Context) []Attempt {
	attempts := make([]Attempt, 0, len(s.resourcePoolClassifiers)+len(s.resourcePools))
	for _, classifier := range s.resourcePoolClassifiers {
		attempts = append(attempts, s.exec(ctx, ydbpool.DropClassifierStatement(classifier)))
	}
	for _, pool := range s.resourcePools {
		attempts = append(attempts, s.exec(ctx, ydbpool.DropPoolStatement(pool)))
	}
	return attempts
}

// revokeOnDatabase takes every permission principal holds on the YDB database
// itself. Revoking what a principal does not hold succeeds, so it runs whether
// or not a grant was made.
func (s *session) revokeOnDatabase(ctx context.Context, principal string) Attempt {
	return s.exec(ctx, "REVOKE ALL ON "+sqlident.Quote(platform.YDB, s.database)+" FROM "+principal)
}

// leave removes the throwaway namespace: the statement namespaceSQL returned,
// or on YDB the namespace directory with everything in it.
//
// A dialect whose namespace is the database the probe connected to has nothing
// to leave, and an empty statement is skipped rather than executed: a refusal
// the run did not earn would sit in the one place a reader looks to see that
// the server was left as it was found.
func (s *session) leave(ctx context.Context, statement string) []Attempt {
	if platform.NormalizeDialect(s.dialect) == platform.YDB {
		return []Attempt{s.removeDirectory(ctx)}
	}
	if statement == "" {
		return nil
	}
	return []Attempt{s.exec(ctx, statement)}
}

// leftovers asks the server whether anything the run created outlived the
// teardown, and returns the reads it made and what each found left. removal
// is what leave returned.
//
// Only YDB is asked. Its namespace is a directory the scheme service removes,
// rather than a statement whose acceptance says the namespace is gone, and
// the group the role experiment creates and the resource pool and classifier
// another one creates are outside the directory. A refused
// removal is a leftover by itself: DropDirectory refuses a tree holding an
// object it has no statement for, such as a coordination node, before it
// drops anything, and the partition statistics list row tables only, so they
// would count no table under a directory still standing. The tables are read
// from the partition statistics, which list a row table under its path the
// moment it exists; a read the server refuses is itself a leftover, because
// the run cannot say the server is clean.
func (s *session) leftovers(ctx context.Context, removal []Attempt) ([]Attempt, []string) {
	if platform.NormalizeDialect(s.dialect) != platform.YDB {
		return nil, nil
	}
	var reads []Attempt
	var remaining []string
	directory := path.Join(s.database, s.namespace)
	for _, attempt := range removal {
		if !attempt.Accepted {
			remaining = append(remaining, "the directory "+directory+", which the teardown did not remove: "+
				attempt.ServerErr)
		}
	}
	tables, read := s.query(ctx, fmt.Sprintf("SELECT COUNT(DISTINCT Path) FROM %s WHERE StartsWith(Path, %s)",
		ydbSystemView(s.database, "partition_stats"), ydbString(directory+"/")))
	reads = append(reads, read)
	switch {
	case !read.Accepted:
		remaining = append(remaining, "the tables under "+directory+", which the server would not count")
	case tables > 0:
		remaining = append(remaining, fmt.Sprintf("%d table(s) under %s", tables, directory))
	}
	for _, object := range slices.Concat(
		workloadObjects(s.resourcePools, "resource_pools", "resource pool "),
		workloadObjects(s.resourcePoolClassifiers, "resource_pool_classifiers", "resource pool classifier "),
	) {
		found, read := s.query(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE Name = %s",
			ydbSystemView(s.database, object.view), ydbString(object.name)))
		reads = append(reads, read)
		switch {
		case !read.Accepted:
			remaining = append(remaining, object.what+", which the server would not look up")
		case found > 0:
			remaining = append(remaining, object.what)
		}
	}
	for _, principal := range slices.Concat(s.roles, s.users) {
		for _, view := range []struct{ name, what string }{
			{"auth_groups", "group " + principal},
			{"auth_users", "user " + principal},
			{"auth_permissions", "the permissions of " + principal},
		} {
			found, read := s.query(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE Sid = %s",
				ydbSystemView(s.database, view.name), ydbString(principal)))
			reads = append(reads, read)
			switch {
			case !read.Accepted:
				remaining = append(remaining, view.what+", which the server would not look up")
			case found > 0:
				remaining = append(remaining, view.what)
			}
		}
	}
	return reads, remaining
}

// workloadObject is a YDB resource pool or classifier the teardown confirms
// gone, by its name in the system view that lists it.
type workloadObject struct {
	name, view, what string
}

// workloadObjects lists names as objects of one system view.
func workloadObjects(names []string, view, kind string) []workloadObject {
	objects := make([]workloadObject, 0, len(names))
	for _, name := range names {
		objects = append(objects, workloadObject{name: name, view: view, what: kind + name})
	}
	return objects
}

// directoryDropper is what the YDB teardown needs of the connection's schema
// writer: no SQL removes a directory, and the writer reaches the scheme
// service that does.
type directoryDropper interface {
	DropDirectory(ctx context.Context, dir string) error
}

// removeDirectory drops the YDB namespace directory and every object in it.
// It runs on a broken session too: the scheme service is a channel of its own,
// and an attempt that fails says so in the cleanup record.
func (s *session) removeDirectory(ctx context.Context) Attempt {
	attempt := Attempt{Statement: "remove the directory " + path.Join(s.database, s.namespace) +
		" and everything in it through the scheme service"}
	dropper, ok := s.conn.SchemaWriter().(directoryDropper)
	if !ok {
		attempt.ServerErr = fmt.Sprintf("the connection's schema writer %T cannot remove a directory", s.conn.SchemaWriter())
		return attempt
	}
	if err := dropper.DropDirectory(ctx, s.namespace); err != nil {
		attempt.ServerErr = err.Error()
		return attempt
	}
	attempt.Accepted = true
	return attempt
}

// enterYDBDirectory points the session at the namespace directory: the
// database comes from the connection's URL, and every statement from here on
// is sent after the pragma that names the directory.
func (s *session) enterYDBDirectory() error {
	parsed, err := ydburl.Parse(s.conn.Info().URL)
	if err != nil {
		return fmt.Errorf("read the YDB database the probe connected to: %w", err)
	}
	s.database = parsed.Database
	s.prefix = "PRAGMA TablePathPrefix(" + ydbString(path.Join(s.database, s.namespace)) + ");\n"
	return nil
}

// ydbString is a YQL String literal holding value.
func ydbString(value string) string {
	return ydbtype.StringLiteral(value)
}

// ydbSystemView is the quoted absolute path of one of the database's system
// views. With the namespace pragma in effect a relative `.sys/...` would name
// a path inside the namespace, so the path is absolute.
func ydbSystemView(database, view string) string {
	return sqlident.Quote(platform.YDB, path.Join(database, ".sys", view))
}
