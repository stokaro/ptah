package mysqldefiner

import (
	"context"
	"fmt"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/dbschema"
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

// Decide answers whether an account holding privileges may name a definer on
// dialect, where definerExists is what the server said about that definer.
//
// On MySQL SET_ANY_DEFINER names an existing account, and naming one the
// server lacks needs ALLOW_NONEXISTENT_DEFINER too; SUPER covers both, and
// SET_USER_ID is the older name of the first. Measured on MySQL 8.4 and 26.10.
// On MariaDB SET USER names any account, existing or not. SUPER alone is
// accepted by MariaDB 10.11 and refused by 11.4, 11.8 and 12.3, and the grants
// cannot tell those servers apart, so an account holding only SUPER is left
// undecided.
func Decide(dialect string, privileges map[string]bool, definerExists Existence) Verdict {
	if dialect == platform.MariaDB {
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

// Check refuses statements that name a definer the account conn runs as may
// not name, before any of them runs. source says where the statements come
// from in the refusal, such as a migration file, and may be empty.
//
// It asks only on the MySQL family, and only for a definer other than the
// connected account: naming oneself needs no privilege. Where the server does
// not answer, or the grants do not decide, it refuses nothing, and a statement
// the account may not run fails on the server with ERROR 1227 as it would
// without the check.
func Check(ctx context.Context, conn *dbschema.DatabaseConnection, source string, statements []string) error {
	if conn == nil || !appliesTo(conn.Info().Dialect) {
		return nil
	}
	named := Scan(statements)
	if len(named) == 0 {
		return nil
	}
	dialect := platform.NormalizeDialect(conn.Info().Dialect)
	var account string
	if err := conn.QueryRowContext(ctx, "SELECT CURRENT_USER()").Scan(&account); err != nil {
		return nil
	}
	var foreign []Named
	for _, one := range named {
		if !sameAccount(one.Definer, account) {
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
	privileges, found := GlobalPrivileges(lines)
	if !found {
		return nil
	}
	var missing []MissingPrivilege
	for _, one := range foreign {
		verdict := Decide(dialect, privileges, definerExistence(ctx, conn, dialect, privileges, one.Definer))
		if !verdict.Decided || verdict.Missing == "" {
			continue
		}
		missing = append(missing, MissingPrivilege{
			Object: one.Object, Definer: one.Definer, Account: account, Privilege: verdict.Missing, Source: source,
		})
	}
	if len(missing) == 0 {
		return nil
	}
	return &PrivilegeError{Missing: missing}
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
// answer changes the verdict: on MySQL, for an account that holds
// SET_ANY_DEFINER without ALLOW_NONEXISTENT_DEFINER or SUPER.
func definerExistence(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	dialect string,
	privileges map[string]bool,
	account string,
) Existence {
	if dialect == platform.MariaDB || privileges[privilegeSuper] || privileges[privilegeAllowNonexistentDefiner] {
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
