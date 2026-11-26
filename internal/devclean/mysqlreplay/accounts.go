package mysqlreplay

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"ptah.run/internal/devclean/replay"
	"ptah.run/internal/lexer"
	"ptah.run/internal/mysqlstored"
)

// sessionQuery reads, in one round trip, what [guard.ValidateSession] needs
// to know about the dev session: its client character set, the account the
// server matched it to, and the names of the roles that account holds,
// directly or through another role, as a JSON array. Both servers list a
// nested role and a role that is granted but not active in APPLICABLE_ROLES,
// measured on MySQL 8.4.13 and MariaDB 11.8.9, and neither needs a privilege
// to read it.
const sessionQuery = "SELECT @@character_set_client, CURRENT_USER(), " +
	"(SELECT COALESCE(JSON_ARRAYAGG(ROLE_NAME), JSON_ARRAY()) FROM information_schema.APPLICABLE_ROLES)"

// sessionAccount is the account the replay session runs as and the roles it
// holds. Its zero value is the account of a guard that read no session: it
// knows the spellings that name the session's account whoever runs it, such
// as CURRENT_USER, and reads every account spelled out as another one. Every
// guard a run replays with reads its session first
// (devclean.NewDevReplayGuardFor).
type sessionAccount struct {
	// read reports that the account was read from a session.
	read bool
	// user and host are the account CURRENT_USER() names, the row the server
	// matched the session to. The host is that row's host, which can be a
	// pattern such as %.
	user, host string
	// roles are the names of the roles the account holds. A MariaDB role has
	// no host, and a MySQL role's host is left out, so a role matches an
	// account of its name on any host.
	roles []string
}

// readSessionAccount reads the account CURRENT_USER() answered, user@host,
// and the JSON array of role names [sessionQuery] reads. The host is what
// follows the last at sign, since a quoted user name may hold one and a host
// may not.
func readSessionAccount(current, roles string) (sessionAccount, error) {
	at := strings.LastIndexByte(current, '@')
	if at < 0 {
		return sessionAccount{}, fmt.Errorf("CURRENT_USER() answered %q, which names no host", current)
	}
	account := sessionAccount{read: true, user: current[:at], host: current[at+1:]}
	if err := json.Unmarshal([]byte(roles), &account.roles); err != nil {
		return sessionAccount{}, fmt.Errorf("read the roles the account holds from %q: %w", roles, err)
	}
	return account, nil
}

// reachOperation is the refusal of a statement whose head reaches the
// account the run connects as.
const reachOperation = " that reaches the account the run connects as"

// unreadOperation is the refusal of a statement whose head names accounts
// the guard cannot read, and so cannot tell from the run's own.
const unreadOperation = " whose accounts cannot be read"

// operation names the refusal of a statement that can remove, rename, lock or
// expire the account the run connects as, change its credentials or its
// privileges, or give a new connection of the run another account in its
// place, and returns "" for any other statement.
//
// Every realm refuses such a statement, and so does trust. The run and its
// cleanup connect as that account for the rest of the command, and on a
// server the operator named it is the operator's own. Measured on MySQL 8.4.13
// and MariaDB 11.8.9, the session that runs DROP USER CURRENT_USER or REVOKE
// ALL PRIVILEGES, GRANT OPTION FROM CURRENT_USER goes on, and a new connection
// of the account is refused or cannot drop the dev database, so the cleanup
// leaves it on the server. The same holds for each statement below.
//
// The statements it reads by the accounts they name:
//
//   - DROP USER and DROP ROLE, of the account or a role it holds: MySQL drops
//     either with either, and the account loses what a dropped role gave it,
//     nested roles included;
//   - REVOKE in any form, from the account or a role it holds, and a REVOKE
//     PROXY on the account, which the session of a proxy user runs as;
//   - CREATE OR REPLACE USER or ROLE of either, which MariaDB drops and
//     creates again without the privileges it held;
//   - GRANT to the account with IDENTIFIED, REQUIRE or a resource limit, which
//     MariaDB takes and applies to a new connection: a password, a TLS demand
//     or a limit such as MAX_QUERIES_PER_HOUR 1 refuses it;
//   - CREATE USER, CREATE ROLE, CREATE OR REPLACE and GRANT of an account that
//     shadows the session's: the server matches a new connection to the
//     account with the most specific host, and to a named user before the
//     anonymous one, so 'u'@'127.0.0.1' and the anonymous user on 127.0.0.1
//     take the connections of 'u'@'%' from that address, with their own
//     passwords. MariaDB creates the account a GRANT names when the GRANT
//     says IDENTIFIED, or when the session's sql_mode lacks
//     NO_AUTO_CREATE_USER.
//
// An account names the run's when it is spelled CURRENT_USER, CURRENT_USER(),
// USER() or CURRENT_ROLE, when it is PUBLIC without a host, which MariaDB
// reads as the role every account holds, or when it names the account the
// session read: the user name compared exactly, and the host as the server
// compares a host, in either case and with % and _ as wildcards, so that %
// covers the session's host. A host left out is %, and so is an empty one,
// which MariaDB reads as %. A role is named by its name. MySQL reads CURRENT_ROLE and
// PUBLIC as ordinary names, and the guard refuses them there too, since a
// mysql URL can reach MariaDB.
//
// ALTER USER, RENAME USER, SET PASSWORD and SET DEFAULT ROLE are refused
// whatever account they name. Each changes an account the server already
// has, and SET PASSWORD and MariaDB's SET DEFAULT ROLE change the run's own
// when they name none. MariaDB takes SET PASSWORD and SET DEFAULT ROLE as any
// item of a SET list: SET @a = 1, PASSWORD FOR 'u'@'%' = PASSWORD('x')
// changes the password of u.
//
// A statement that names accounts the guard cannot read is refused, since it
// cannot tell them from the run's. The rule reads a statement as the history
// writes it: a routine body, which a server the run owns lifts, and a
// prepared statement, which trust lifts, can run any of these unread, as
// [validateMySQLReplayStatement] says.
func (s sessionAccount) operation(stmt string, tokens []lexer.Token, reading mysqlstored.Server) string {
	switch {
	case len(tokens) == 0:
		return ""
	case replay.TokenSequenceAt(tokens, 0, "ALTER", "USER"):
		return "ALTER USER"
	case replay.TokenSequenceAt(tokens, 0, "RENAME", "USER"):
		return "RENAME USER"
	case replay.TokenSequenceAt(tokens, 0, "SET"):
		return credentialsSetItem(tokens)
	case replay.TokenSequenceAt(tokens, 0, "DROP", "USER"), replay.TokenSequenceAt(tokens, 0, "DROP", "ROLE"):
		at := 2
		if replay.TokenSequenceAt(tokens, at, "IF", "EXISTS") {
			at += 2
		}
		return s.listOperation(stmt, tokens, at, reading, "DROP "+replay.NormalizedIdentifier(tokens[1]), nil, s.names)
	case replay.TokenSequenceAt(tokens, 0, "CREATE"):
		return s.createOperation(stmt, tokens, reading)
	case replay.TokenSequenceAt(tokens, 0, "REVOKE"):
		return s.revokeOperation(stmt, tokens, reading)
	case replay.TokenSequenceAt(tokens, 0, "GRANT"):
		return s.grantOperation(stmt, tokens, reading)
	default:
		return ""
	}
}

// credentialsSetItem names a SET whose list holds PASSWORD or DEFAULT ROLE as
// an item, and returns "" for any other SET.
func credentialsSetItem(tokens []lexer.Token) string {
	for _, item := range setItems(tokens) {
		switch {
		case replay.TokenSequenceAt(tokens, item, "PASSWORD"):
			return "SET PASSWORD"
		case replay.TokenSequenceAt(tokens, item, "DEFAULT", "ROLE"):
			return "SET DEFAULT ROLE"
		}
	}
	return ""
}

// createOperation reads CREATE [OR REPLACE] USER or ROLE [IF NOT EXISTS] and
// its list. A DEFAULT ROLE clause ends the list of a CREATE USER, whose roles
// are not accounts it creates.
func (s sessionAccount) createOperation(stmt string, tokens []lexer.Token, reading mysqlstored.Server) string {
	at := 1
	replaces := replay.TokenSequenceAt(tokens, at, "OR", "REPLACE")
	if replaces {
		at += 2
	}
	kind := replay.NormalizedIdentifier(replay.TokenAt(tokens, at))
	if kind != "USER" && kind != "ROLE" {
		return ""
	}
	at++
	if replay.TokenSequenceAt(tokens, at, "IF", "NOT", "EXISTS") {
		at += 3
	}
	match := s.shadows
	if replaces {
		match = s.namesOrShadows
	}
	return s.listOperation(stmt, tokens, at, reading, "CREATE "+kind, []string{"DEFAULT"}, match)
}

// revokeOperation reads the account a REVOKE PROXY is on and the grantees
// after FROM.
func (s sessionAccount) revokeOperation(stmt string, tokens []lexer.Token, reading mysqlstored.Server) string {
	if replay.TokenSequenceAt(tokens, 1, "PROXY", "ON") {
		named, _, ok := readNamedAccount(stmt, tokens, 3, reading)
		switch {
		case !ok:
			return "REVOKE" + unreadOperation
		case s.names(named):
			return "REVOKE" + reachOperation
		}
	}
	from := topLevelKeyword(tokens, 1, "FROM")
	if from < 0 {
		return "REVOKE" + unreadOperation
	}
	return s.listOperation(stmt, tokens, from+1, reading, "REVOKE", nil, s.names)
}

// grantOperation reads the grantees of a GRANT. A grantee that shadows the
// session's account is refused in any GRANT, which MariaDB can create it
// with. The account itself is refused only in a GRANT that gives it a
// password, a TLS requirement or a resource limit: a GRANT without one adds
// privileges or roles and takes nothing away. MySQL's AS ... WITH ROLE ends
// the list of grantees: its roles are the grantor's.
func (s sessionAccount) grantOperation(stmt string, tokens []lexer.Token, reading mysqlstored.Server) string {
	to := topLevelKeyword(tokens, 1, "TO")
	if to < 0 {
		return ""
	}
	match := s.shadowsOnly
	if slices.ContainsFunc(tokens[to+1:], locksOut) {
		match = s.namesOrShadows
	}
	return s.listOperation(stmt, tokens, to+1, reading, "GRANT", []string{"AS"}, match)
}

// locksOut reports a word of a GRANT that sets what a new connection of a
// grantee must present or may do.
func locksOut(token lexer.Token) bool {
	return isKeyword(token, "IDENTIFIED", "REQUIRE", "MAX_QUERIES_PER_HOUR", "MAX_UPDATES_PER_HOUR",
		"MAX_CONNECTIONS_PER_HOUR", "MAX_USER_CONNECTIONS", "MAX_STATEMENT_TIME")
}

// listOperation reads the comma-separated accounts that begin at at, each
// one with whatever follows it up to the next comma, until a word of stops
// outside parentheses or the end of the statement. It names the refusal of
// head when match accepts one of them, or when one cannot be read, and
// returns "" otherwise.
func (s sessionAccount) listOperation(
	stmt string,
	tokens []lexer.Token,
	at int,
	reading mysqlstored.Server,
	head string,
	stops []string,
	match func(namedAccount) bool,
) string {
	for at >= 0 {
		named, next, ok := readNamedAccount(stmt, tokens, at, reading)
		switch {
		case !ok:
			return head + unreadOperation
		case match(named):
			return head + reachOperation
		}
		at = nextListItem(tokens, next, stops)
	}
	return ""
}

// nextListItem returns the index after the first comma outside parentheses
// at or after at, and -1 where the list ends first, at a word of stops
// outside parentheses or at the end of the statement.
func nextListItem(tokens []lexer.Token, at int, stops []string) int {
	depth := 0
	for ; at < len(tokens); at++ {
		switch {
		case tokens[at].MatchOperatorValue("("):
			depth++
		case tokens[at].MatchOperatorValue(")"):
			depth = max(depth-1, 0)
		case depth == 0 && tokens[at].MatchOperatorValue(","):
			return at + 1
		case depth == 0 && isKeyword(tokens[at], stops...):
			return -1
		}
	}
	return -1
}

// topLevelKeyword returns the index of the first unquoted word at or after
// at, outside parentheses, and -1 where there is none.
func topLevelKeyword(tokens []lexer.Token, at int, word string) int {
	depth := 0
	for ; at < len(tokens); at++ {
		switch {
		case tokens[at].MatchOperatorValue("("):
			depth++
		case tokens[at].MatchOperatorValue(")"):
			depth = max(depth-1, 0)
		case depth == 0 && isKeyword(tokens[at], word):
			return at
		}
	}
	return -1
}

// namedAccount is an account a statement names, as the server reads it.
type namedAccount struct {
	// run reports a spelling that names the run's own account or a role it
	// holds, whoever the session runs as.
	run bool
	// user and host are the account spelled out, unquoted, with the host %
	// where none is written.
	user, host string
}

// readNamedAccount reads the account at at, and returns it, the index of the
// token after it, and false where no account it can read is there.
func readNamedAccount(stmt string, tokens []lexer.Token, at int, reading mysqlstored.Server) (namedAccount, int, bool) {
	token := replay.TokenAt(tokens, at)
	if isKeyword(token, "USER") && isOperatorAt(tokens, at+1, "(") && isOperatorAt(tokens, at+2, ")") {
		return namedAccount{run: true}, at + 3, true
	}
	account, next, err := mysqlstored.ReadAccount(stmt, tokens, at)
	if err != nil {
		return namedAccount{}, at, false
	}
	if isKeyword(account.User, "CURRENT_USER", "CURRENT_ROLE") {
		return namedAccount{run: true}, next, true
	}
	named := namedAccount{user: accountText(account.User, reading), host: "%"}
	switch {
	case account.Host == nil:
		named.run = replay.NormalizedIdentifier(account.User) == "PUBLIC"
	case accountText(*account.Host, reading) != "":
		named.host = accountText(*account.Host, reading)
	}
	return named, next, true
}

// names reports whether named is the session's account, or a role it holds.
func (s sessionAccount) names(named namedAccount) bool {
	switch {
	case named.run:
		return true
	case !s.read:
		return false
	case named.user == s.user && hostCovers(named.host, s.host):
		return true
	default:
		return slices.Contains(s.roles, named.user)
	}
}

// shadows reports whether a new connection of the run can be matched to
// named in place of the session's account: an account of the session's user
// on a host the session's own host covers, or the anonymous user on a host
// the session's host covers and is not covered by. The session's account is
// one of them, as a host covers itself.
func (s sessionAccount) shadows(named namedAccount) bool {
	switch {
	case named.run:
		return true
	case !s.read || !hostCovers(s.host, named.host):
		return false
	case named.user == s.user:
		return true
	default:
		return named.user == "" && !hostCovers(named.host, s.host)
	}
}

// shadowsOnly reports whether named shadows the session's account and is
// neither the account nor a role it holds.
func (s sessionAccount) shadowsOnly(named namedAccount) bool {
	return s.shadows(named) && !s.names(named)
}

// namesOrShadows reports whether named is either.
func (s sessionAccount) namesOrShadows(named namedAccount) bool {
	return s.names(named) || s.shadows(named)
}

// hostCovers reports whether pattern, read as the server reads the host of
// an account, matches host: ASCII letters in either case, % for any run of
// characters and _ for one character. A host covers itself.
func hostCovers(pattern, host string) bool {
	return wildcardMatch([]rune(lowerASCII(pattern)), []rune(lowerASCII(host)))
}

// wildcardMatch reports whether pattern matches text, with % matching any
// run of characters and _ any one character.
func wildcardMatch(pattern, text []rune) bool {
	p, t := 0, 0
	star, mark := -1, 0
	for t < len(text) {
		switch {
		case p < len(pattern) && (pattern[p] == '_' || pattern[p] == text[t]):
			p++
			t++
		case p < len(pattern) && pattern[p] == '%':
			star, mark = p, t
			p++
		case star >= 0:
			p = star + 1
			mark++
			t = mark
		default:
			return false
		}
	}
	for p < len(pattern) && pattern[p] == '%' {
		p++
	}
	return p == len(pattern)
}

// lowerASCII is s with the letters A to Z in lower case and every other byte
// as written. Unicode case folding would read the Kelvin sign as k, which the
// server does not.
func lowerASCII(s string) string {
	return strings.Map(func(r rune) rune {
		if 'A' <= r && r <= 'Z' {
			return r + ('a' - 'A')
		}
		return r
	}, s)
}

// accountText is the user or host a token spells, as a session that reads by
// reading reads it: a quoted name without its quotes, and a string with its
// backslash escapes read unless reading sets NO_BACKSLASH_ESCAPES, so that
// 'd\ev' names dev. [guard.Validate] checks a statement holding a backslash
// under both rules.
func accountText(token lexer.Token, reading mysqlstored.Server) string {
	value := token.Value
	if token.Type != lexer.TokenString || reading.NoBackslashEscapes || !strings.Contains(value, `\`) ||
		len(value) < 2 || value[0] != '\'' && value[0] != '"' || value[len(value)-1] != value[0] {
		return mysqlstored.Unquote(token)
	}
	quote, inner := value[0], value[1:len(value)-1]
	var text strings.Builder
	for index := 0; index < len(inner); index++ {
		switch {
		case inner[index] == '\\' && index+1 < len(inner):
			index++
			text.WriteString(stringEscape(inner[index]))
		case inner[index] == quote && index+1 < len(inner) && inner[index+1] == quote:
			index++
			text.WriteByte(quote)
		default:
			text.WriteByte(inner[index])
		}
	}
	return text.String()
}

// stringEscape is what the server reads a backslash and b as in a string:
// the control characters of \0, \b, \n, \r, \t and \Z, the two characters of
// \% and \_, and b itself for any other byte.
func stringEscape(b byte) string {
	switch b {
	case '0':
		return "\x00"
	case 'b':
		return "\b"
	case 'n':
		return "\n"
	case 'r':
		return "\r"
	case 't':
		return "\t"
	case 'Z':
		return "\x1a"
	case '%', '_':
		return `\` + string(b)
	default:
		return string(b)
	}
}
