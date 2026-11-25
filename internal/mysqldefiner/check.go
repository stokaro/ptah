package mysqldefiner

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/dbschema"
	"ptah.run/internal/lexer"
	"ptah.run/internal/mysqlstored"
)

// The global privileges that let an account name another definer, as SHOW
// GRANTS spells them.
const (
	privilegeSuper                   = "SUPER"
	privilegeSetAnyDefiner           = "SET_ANY_DEFINER"
	privilegeSetUserID               = "SET_USER_ID"
	privilegeAllowNonexistentDefiner = "ALLOW_NONEXISTENT_DEFINER"
	privilegeSetUser                 = "SET USER"
	privilegeAll                     = "ALL PRIVILEGES"
)

// Existence is what the server said about a definer account.
type Existence int

const (
	// ExistenceUnknown means the account could not ask: reading mysql.user
	// needs a privilege a deploy account seldom holds.
	ExistenceUnknown Existence = iota
	// Exists means the account is on the server.
	Exists
	// Missing means the account is not on the server.
	Missing
)

// Verdict is what [Decide] answers for one definer.
type Verdict struct {
	// Decided is false where the grants do not settle the question; the
	// statement is then left to the server.
	Decided bool
	// Missing is the privilege the account lacks, empty where it may name the
	// definer.
	Missing string
}

// GlobalPrivileges returns the privileges the SHOW GRANTS lines grant on *.*,
// in upper case, and whether any line granted on *.* at all.
//
// Plain SHOW GRANTS, without FOR, lists what the session holds. MySQL 8.4 and
// 26.10 merge the privileges of an active role into the account's own lines.
// MariaDB 10.11 to 12.3 list an active role's grants as lines of their own,
// nested roles included. Neither lists the grants of a role that is granted
// but not active, which the server does not apply either. SHOW GRANTS FOR
// CURRENT_USER() and information_schema.USER_PRIVILEGES leave role privileges
// out, so neither can answer here.
func GlobalPrivileges(lines []string) (map[string]bool, bool) {
	privileges := make(map[string]bool)
	found := false
	for _, line := range lines {
		rest, ok := strings.CutPrefix(strings.TrimSpace(line), "GRANT ")
		if !ok {
			continue
		}
		list, scope, ok := strings.Cut(rest, " ON ")
		if !ok || !strings.HasPrefix(strings.TrimSpace(scope), "*.*") {
			continue
		}
		found = true
		for privilege := range strings.SplitSeq(list, ",") {
			privileges[strings.ToUpper(strings.TrimSpace(privilege))] = true
		}
	}
	return privileges, found
}

// serverOf is the server a connection reports. The session's backslash rule
// is left at the default: [Scan] leaves a quoted account holding a backslash
// out whatever the session reads it as.
func serverOf(info catalog.ServerInfo) mysqlstored.Server {
	return mysqlstored.Server{Dialect: platform.NormalizeDialect(info.Dialect), VersionID: mysqlstored.VersionID(info.Version)}
}

// The MySQL releases the definer privileges change at, as
// [mysqlstored.VersionID] spells them: 8.0 brought SET_USER_ID, and 8.2 SET_ANY_DEFINER and
// ALLOW_NONEXISTENT_DEFINER in its place.
const (
	mysql80 = 80000
	mysql82 = 80200
)

// beforeSetAnyDefiner reports a MySQL server known to be before 8.2.
func beforeSetAnyDefiner(server mysqlstored.Server) bool {
	return server.Dialect == platform.MySQL && server.VersionID > 0 && server.VersionID < mysql82
}

// Decide answers whether an account holding privileges may name a definer on
// server, where definerExists is what the server said about that definer.
//
// On MySQL SET_ANY_DEFINER names an existing account, and naming one the
// server lacks needs ALLOW_NONEXISTENT_DEFINER too; SUPER covers both, and
// SET_USER_ID is the older name of the first, the only one from 8.0 to 8.1
// and gone from 26.10. Measured on MySQL 8.0.46, 8.4 and 26.10. A server
// before 8.0, whose privilege is SUPER alone, is left undecided. On MariaDB
// SET USER names any account,
// existing or not. SUPER alone is accepted by MariaDB 10.11 and refused by
// 11.4, 11.8 and 12.3, and the grants cannot tell those servers apart, so an
// account holding only SUPER is left undecided.
func Decide(server mysqlstored.Server, privileges map[string]bool, definerExists Existence) Verdict {
	if server.Dialect == platform.MariaDB {
		switch {
		case privileges[privilegeSetUser] || privileges[privilegeAll]:
			return Verdict{Decided: true}
		case privileges[privilegeSuper]:
			return Verdict{}
		default:
			return Verdict{Decided: true, Missing: privilegeSetUser}
		}
	}
	switch {
	case privileges[privilegeSuper] || privileges[privilegeSetUserID] || privileges[privilegeAll]:
		return Verdict{Decided: true}
	case beforeSetAnyDefiner(server) && server.VersionID < mysql80:
		return Verdict{}
	case beforeSetAnyDefiner(server):
		return Verdict{Decided: true, Missing: privilegeSetUserID}
	case !privileges[privilegeSetAnyDefiner]:
		if definerExists == Missing && !privileges[privilegeAllowNonexistentDefiner] {
			return Verdict{Decided: true, Missing: privilegeSetAnyDefiner + " and " + privilegeAllowNonexistentDefiner}
		}
		return Verdict{Decided: true, Missing: privilegeSetAnyDefiner}
	case privileges[privilegeAllowNonexistentDefiner] || definerExists == Exists:
		return Verdict{Decided: true}
	case definerExists == Missing:
		return Verdict{Decided: true, Missing: privilegeAllowNonexistentDefiner}
	default:
		return Verdict{}
	}
}

// Source is statements that run in one run, and where they come from.
type Source struct {
	// Name says where the statements come from in a refusal, such as a
	// migration file, and is empty for a plan.
	Name string
	// Statements are the statements, in the order they run.
	Statements []string
}

// Check refuses statements that name a definer the account conn runs as may
// not name, before any of them runs. source says where the statements come
// from in the refusal, such as a migration file, and may be empty.
func Check(ctx context.Context, conn *dbschema.DatabaseConnection, source string, statements []string) error {
	return CheckSources(ctx, conn, []Source{{Name: source, Statements: statements}})
}

// CheckSources is [Check] over the statements of several sources that run in
// one run, in order, such as the files of a migration run.
//
// It asks only on the MySQL family, and only for a definer other than the
// connected account: naming oneself needs no privilege. Where the server does
// not answer, or the grants do not decide, it refuses nothing, and a statement
// the account may not run fails on the server with ERROR 1227 as it would
// without the check. A host-less definer on MariaDB, which may be a role, is
// not decided; neither is whether a definer exists where the run creates or
// renames an account, since the server is read before any of it runs.
//
// The grants are read on a session of conn's pool, which holds the account's
// default roles. A role a statement activates with SET ROLE is not counted.
func CheckSources(ctx context.Context, conn *dbschema.DatabaseConnection, sources []Source) error {
	if conn == nil || !appliesTo(conn.Info().Dialect) {
		return nil
	}
	type found struct {
		Named
		source string
	}
	server := serverOf(conn.Info())
	var named []found
	for _, source := range sources {
		for _, one := range Scan(server, source.Statements) {
			named = append(named, found{Named: one, source: source.Name})
		}
	}
	if len(named) == 0 {
		return nil
	}
	createsAccounts := slices.ContainsFunc(sources, func(source Source) bool {
		return slices.ContainsFunc(source.Statements, func(statement string) bool {
			return changesAccounts(server, statement)
		})
	})
	var account string
	if err := conn.QueryRowContext(ctx, "SELECT CURRENT_USER()").Scan(&account); err != nil {
		return nil
	}
	var foreign []found
	for _, one := range named {
		if !sameAccount(one.Definer, account) && (server.Dialect != platform.MariaDB || !one.Hostless) {
			foreign = append(foreign, one)
		}
	}
	if len(foreign) == 0 {
		return nil
	}
	lines, err := showGrants(ctx, conn)
	if err != nil {
		return nil
	}
	privileges, granted := GlobalPrivileges(lines)
	if !granted {
		return nil
	}
	var missing []MissingPrivilege
	for _, one := range foreign {
		existence := ExistenceUnknown
		if !createsAccounts {
			existence = definerExistence(ctx, conn, server, privileges, one.Definer)
		}
		verdict := Decide(server, privileges, existence)
		if !verdict.Decided || verdict.Missing == "" {
			continue
		}
		missing = append(missing, MissingPrivilege{
			Object: one.Object, Definer: one.Definer, Account: account, Privilege: verdict.Missing, Source: one.source,
		})
	}
	if len(missing) == 0 {
		return nil
	}
	return &PrivilegeError{Missing: missing}
}

// changesAccounts reports whether statement, read as server reads it, creates
// or renames an account or a role. A run holding such a statement changes
// which accounts exist after the server was read, so [CheckSources] does not
// decide from that read.
//
// A statement holding an executable comment the reader cannot read may hold
// such a statement, and counts as one.
func changesAccounts(server mysqlstored.Server, statement string) bool {
	tokens, err := mysqlstored.Tokens(server, statement)
	if err != nil {
		return true
	}
	return len(tokens) >= 2 &&
		(isWord(tokens[0], "CREATE") && (isWord(tokens[1], "USER") || isWord(tokens[1], "ROLE")) ||
			isWord(tokens[0], "RENAME") && isWord(tokens[1], "USER"))
}

// isWord reports an unquoted word, its ASCII letters in either case, as the
// server matches a keyword.
func isWord(token lexer.Token, word string) bool {
	return token.MatchIdentifierValue(word)
}

// appliesTo reports whether dialect is one whose statements name definers
// with this grammar and whose server checks them with these privileges.
func appliesTo(dialect string) bool {
	switch platform.NormalizeDialect(dialect) {
	case platform.MySQL, platform.MariaDB:
		return true
	default:
		return false
	}
}

func showGrants(ctx context.Context, conn *dbschema.DatabaseConnection) ([]string, error) {
	rows, err := conn.QueryContext(ctx, "SHOW GRANTS")
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var lines []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			return nil, err
		}
		lines = append(lines, line)
	}
	return lines, rows.Err()
}

// definerExistence asks whether a definer account exists, only where the
// answer changes the verdict: on MySQL 8.2 and later, for an account that
// holds SET_ANY_DEFINER without ALLOW_NONEXISTENT_DEFINER or SUPER.
func definerExistence(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	server mysqlstored.Server,
	privileges map[string]bool,
	account string,
) Existence {
	if server.Dialect == platform.MariaDB || beforeSetAnyDefiner(server) ||
		privileges[privilegeSuper] || privileges[privilegeAllowNonexistentDefiner] {
		return ExistenceUnknown
	}
	at := strings.LastIndexByte(account, '@')
	if at < 0 {
		return ExistenceUnknown
	}
	var count int
	err := conn.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM mysql.user WHERE User = ? AND Host = LOWER(?)",
		account[:at], account[at+1:],
	).Scan(&count)
	switch {
	case err != nil:
		return ExistenceUnknown
	case count == 0:
		return Missing
	default:
		return Exists
	}
}

// MissingPrivilege is one statement the connected account may not run
// because of the definer it names.
type MissingPrivilege struct {
	// Object is what the statement writes, such as "view my_docs".
	Object string `json:"object"`
	// Definer is the account the statement names.
	Definer string `json:"definer"`
	// Account is the connected account, CURRENT_USER().
	Account string `json:"account"`
	// Privilege is the global privilege the account lacks, or two joined by
	// "and".
	Privilege string `json:"privilege"`
	// Source is where the statement is, such as a migration file, and empty
	// for a plan.
	Source string `json:"source,omitempty"`
}

// PrivilegeError is the refusal [Check] returns.
type PrivilegeError struct {
	// Missing lists each statement refused, in statement order.
	Missing []MissingPrivilege
}

func (e *PrivilegeError) Error() string {
	parts := make([]string, 0, len(e.Missing))
	for _, missing := range e.Missing {
		where := ""
		if missing.Source != "" {
			where = " in " + missing.Source
		}
		parts = append(parts, fmt.Sprintf(
			"%s%s names the definer %s, and the connected account %s lacks %s, which naming another account "+
				"needs. Grant it with GRANT %s ON *.* TO %s, or apply as %s",
			missing.Object, where, missing.Definer, missing.Account, missing.Privilege,
			grantList(missing.Privilege), quotedAccount(missing.Account), missing.Definer))
	}
	return "refusing before any statement runs: " + strings.Join(parts, "; ")
}

func grantList(privilege string) string {
	return strings.ReplaceAll(privilege, " and ", ", ")
}

// quotedAccount writes user@host as 'user'@'host' for a GRANT the reader
// copies.
func quotedAccount(account string) string {
	at := strings.LastIndexByte(account, '@')
	if at < 0 {
		return "'" + account + "'"
	}
	return "'" + account[:at] + "'@'" + account[at+1:] + "'"
}
