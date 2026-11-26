package mysqlreplay

import (
	"encoding/json"
	"fmt"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"unicode"

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
// as CURRENT_USER, and cannot tell any account spelled out from the
// session's, so it refuses a statement that names one. Every guard a run
// replays with reads its session first (devclean.NewDevReplayGuardFor).
type sessionAccount struct {
	// read reports that the account was read from a session.
	read bool
	// user and host are the account CURRENT_USER() names, the row the server
	// matched the session to. The host is that row's host, which can be a
	// pattern such as %, and is % where the row's host is empty.
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
	account := sessionAccount{read: true, user: current[:at], host: orAnyHost(current[at+1:])}
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

// reachRemedy ends the refusal of a statement that reaches the account the
// run connects as. Neither owning the server nor trust lifts that refusal,
// and the same statement replays when the run connects as another account.
const reachRemedy = "connect as an account the history does not name"

// refuseAccountOperation is the refusal of operation, which
// [sessionAccount.operation] names, with [reachRemedy] at the end of a
// refusal of a statement that reaches the run's account.
func refuseAccountOperation(dialect, operation string) error {
	if strings.HasSuffix(operation, reachOperation) {
		return replay.RefuseWithRemedy(dialect, operation, reachRemedy)
	}
	return replay.Refuse(dialect, operation)
}

// operation names the refusal of a statement that can remove, rename, lock or
// expire the account the run connects as, change its credentials or its
// privileges, or give a new connection of the run another account in its
// place, and returns "" for any other statement. database is the database the
// statement runs in, the one an unqualified table name is in.
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
//     shadows the session's, as [sessionAccount.shadowedBy] reads it: the
//     server matches a new connection to the account with the most specific
//     host, and to a named user before the anonymous one, so 'u'@'127.0.0.1'
//     and the anonymous user on 127.0.0.1 take the connections of 'u'@'%'
//     from that address, with their own passwords. MariaDB creates the
//     account a GRANT names when the GRANT says IDENTIFIED, or when the
//     session's sql_mode lacks NO_AUTO_CREATE_USER.
//
// An account names the run's when it is spelled CURRENT_USER, CURRENT_USER(),
// USER() or CURRENT_ROLE, when it is PUBLIC without a host, which MariaDB
// reads as the role every account holds, or when it is the account the
// session read: the same user name and the same host, in either case. A role
// is named by its name. A statement names one account exactly, so DROP USER
// 'u'@'%' leaves 'u'@'localhost' as it is; a host pattern counts only where
// an account shadows the session's. A name is read as each server reads it
// ([accountReadings]), and the statement is refused when either reading
// reaches the account, since a mysql URL can reach MariaDB. MySQL reads
// CURRENT_ROLE and PUBLIC as ordinary names, and the guard refuses them there
// too.
//
// ALTER USER, RENAME USER, SET PASSWORD and SET DEFAULT ROLE are refused
// whatever account they name. Each changes an account the server already
// has, and SET PASSWORD and MariaDB's SET DEFAULT ROLE change the run's own
// when they name none. MariaDB takes SET PASSWORD and SET DEFAULT ROLE as any
// item of a SET list: SET @a = 1, PASSWORD FOR 'u'@'%' = PASSWORD('x')
// changes the password of u.
//
// A write to a table that holds accounts is refused as well
// ([accountTables]), and each of these statements in the body of a routine, a
// trigger or an event is refused at the statement that writes the body
// ([bodyStatements]).
//
// A statement that names accounts the guard cannot read is refused, since it
// cannot tell them from the run's. The rule reads a statement and a body as
// the history writes them: SQL that a body or a prepared statement builds at
// run time, which a server the run owns and trust lift, can run any of these
// unread, as [validateMySQLReplayStatement] says.
func (s sessionAccount) operation(stmt string, tokens []lexer.Token, reading mysqlstored.Server, database string) string {
	if start, ok := mysqlstored.Wrapped(tokens); ok {
		return s.operation(stmt, tokens[start:], reading, database)
	}
	if len(tokens) == 0 {
		return ""
	}
	if operation, ok := s.accountStatementOperation(stmt, tokens, reading); ok {
		return operation
	}
	if definition, ok, _ := mysqlstored.Read(stmt, tokens); ok && definition.Body() {
		return s.bodyOperation(stmt, tokens, reading, definition, database)
	}
	return accountTableOperation(tokens, database)
}

// accountStatementOperation is [sessionAccount.operation] for a statement
// that changes accounts, roles or privileges, or a SET, and reports false for
// any other statement.
func (s sessionAccount) accountStatementOperation(stmt string, tokens []lexer.Token, reading mysqlstored.Server) (string, bool) {
	switch {
	case replay.TokenSequenceAt(tokens, 0, "ALTER", "USER"):
		return "ALTER USER", true
	case replay.TokenSequenceAt(tokens, 0, "RENAME", "USER"):
		return "RENAME USER", true
	case replay.TokenSequenceAt(tokens, 0, "SET"):
		return credentialsSetItem(tokens), true
	case replay.TokenSequenceAt(tokens, 0, "DROP", "USER"), replay.TokenSequenceAt(tokens, 0, "DROP", "ROLE"):
		at := 2
		if replay.TokenSequenceAt(tokens, at, "IF", "EXISTS") {
			at += 2
		}
		return s.listOperation(stmt, tokens, at, reading, "DROP "+replay.NormalizedIdentifier(tokens[1]), nil, s.names), true
	case replay.TokenSequenceAt(tokens, 0, "CREATE"):
		return s.createOperation(stmt, tokens, reading)
	case replay.TokenSequenceAt(tokens, 0, "REVOKE"):
		return s.revokeOperation(stmt, tokens, reading), true
	case replay.TokenSequenceAt(tokens, 0, "GRANT"):
		return s.grantOperation(stmt, tokens, reading), true
	default:
		return "", false
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
// its list, and reports false for a CREATE of any other kind. A DEFAULT ROLE
// clause ends the list of a CREATE USER, whose roles are not accounts it
// creates.
func (s sessionAccount) createOperation(stmt string, tokens []lexer.Token, reading mysqlstored.Server) (string, bool) {
	at := 1
	replaces := replay.TokenSequenceAt(tokens, at, "OR", "REPLACE")
	if replaces {
		at += 2
	}
	kind := replay.NormalizedIdentifier(replay.TokenAt(tokens, at))
	if kind != "USER" && kind != "ROLE" {
		return "", false
	}
	at++
	if replay.TokenSequenceAt(tokens, at, "IF", "NOT", "EXISTS") {
		at += 3
	}
	match := s.shadows
	if replaces {
		match = s.namesOrShadows
	}
	return s.listOperation(stmt, tokens, at, reading, "CREATE "+kind, []string{"DEFAULT"}, match), true
}

// revokeOperation reads the account a REVOKE PROXY is on and the grantees
// after FROM.
func (s sessionAccount) revokeOperation(stmt string, tokens []lexer.Token, reading mysqlstored.Server) string {
	if replay.TokenSequenceAt(tokens, 1, "PROXY", "ON") {
		named, _, ok := readNamedAccount(stmt, tokens, 3, reading)
		switch {
		case !ok || s.cannotTell(named):
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
// head when match accepts one of them, or when one cannot be read or told
// from the session's, and returns "" otherwise.
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
		case !ok || s.cannotTell(named):
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

// namedAccount is an account a statement names, as the servers read it.
type namedAccount struct {
	// run reports a spelling that names the run's own account or a role it
	// holds, whoever the session runs as.
	run bool
	// readings are the account spelled out, as each server reads it; see
	// [accountReadings].
	readings []accountName
}

// accountName is a user and a host as a server reads them, without their
// quotes.
type accountName struct {
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
	host := "%"
	if account.Host != nil {
		host = accountText(*account.Host, reading)
	}
	named := namedAccount{readings: accountReadings(accountText(account.User, reading), host)}
	named.run = account.Host == nil && slices.ContainsFunc(named.readings, func(name accountName) bool {
		return strings.EqualFold(name.user, "PUBLIC")
	})
	return named, next, true
}

// mysqlNameSpace is what MySQL trims from both ends of a user, host or role
// name.
const mysqlNameSpace = " \t\n\v\f\r"

// accountReadings are the account user@host, as a statement writes them
// without their quotes, as each server reads it: MySQL trims [mysqlNameSpace]
// from both ends of each name, and MariaDB trims the spaces at its end. Both
// keep a no-break space, an ideographic space and a space inside a name. So
// 'u '@'% ' names u@% on both servers, and ' u' and 'u\t' name it on MySQL.
// Measured on MySQL 8.4.13 and 26.10.0, DROP USER 'u '@'% ' refuses the next
// login of u@%. Measured on MariaDB 10.11.19 and 11.8.9, it removes the row of
// u@% from the grant tables, and u logs in until the server reloads them, at
// FLUSH PRIVILEGES or a restart, and is refused after.
//
// A host that is empty once trimmed is read as %. MariaDB reads an empty host
// as %. MySQL keeps it as a host of its own, which matches every connection
// after %, and MariaDB reads a host of spaces as a host of its own, so for
// either the reading errs toward refusing a statement the server would not
// apply to the account.
func accountReadings(user, host string) []accountName {
	readings := []accountName{
		{user: strings.Trim(user, mysqlNameSpace), host: orAnyHost(strings.Trim(host, mysqlNameSpace))},
		{user: strings.TrimRight(user, " "), host: orAnyHost(strings.TrimRight(host, " "))},
	}
	return slices.Compact(readings)
}

// orAnyHost is host, or % where host is empty.
func orAnyHost(host string) string {
	if host == "" {
		return "%"
	}
	return host
}

// cannotTell reports whether the guard cannot tell named from the session's
// account: an account spelled out, to a guard that read no session.
func (s sessionAccount) cannotTell(named namedAccount) bool {
	return !named.run && !s.read
}

// names reports whether named is the session's account, or a role it holds.
func (s sessionAccount) names(named namedAccount) bool {
	return named.run || s.read && slices.ContainsFunc(named.readings, s.isOrHolds)
}

// isOrHolds reports whether name is the session's account, the user name
// compared exactly and the host as [foldHost] reads it, or a role the
// account holds.
func (s sessionAccount) isOrHolds(name accountName) bool {
	return name.user == s.user && foldHost(name.host) == foldHost(s.host) || slices.Contains(s.roles, name.user)
}

// foldHost is host as a server folds it to find an account: each character
// lowercased after it is uppercased, by the Unicode simple mappings.
// Measured with DROP USER, MySQL 8.4.13 and 26.10.0 and MariaDB 10.11.19
// lowercase a host, so a dotted capital I (U+0130) for i and a Kelvin sign
// (U+212A) for k reach dev@ci-host and dev@k-host, and MariaDB 11.8.9 and
// 12.3.3 also lowercase a letter's uppercase, so a long s (U+017F) and a
// dotless i (U+0131) reach them too. An accented letter and a full-width
// letter reach neither. Lowercasing the uppercase equates every host that
// lowercasing alone equates, so the one reading answers for each server and
// errs toward refusal on the others. [strings.EqualFold] keeps the dotted
// capital I and the dotless i apart from i.
func foldHost(host string) string {
	return strings.ToLower(strings.ToUpper(host))
}

// shadows reports whether a new connection of the run can be matched to
// named in place of the session's account.
func (s sessionAccount) shadows(named namedAccount) bool {
	return named.run || s.read && slices.ContainsFunc(named.readings, s.shadowedBy)
}

// shadowedBy reports whether name can take a new connection of the session's
// account: an account of the session's user on a host that [hostShadows]
// reports, or the anonymous user on such a host that does not cover the
// session's own. The session's account is one of them, as a host shadows
// itself.
func (s sessionAccount) shadowedBy(name accountName) bool {
	switch {
	case name.user != s.user && name.user != "", !hostShadows(s.host, name.host):
		return false
	case name.user == s.user:
		return true
	default:
		return !hostCovers(name.host, s.host)
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

// hostShadows reports whether an account on host can be matched to a
// connection the session's host, session, takes. The server tries an account
// whose host is an address before one whose host is a pattern or a name, and
// MySQL reads a host written as an address and a netmask, such as
// 192.168.0.0/255.255.0.0, or as an address and a prefix length, such as
// 192.168.0.0/16, as an address. So an account on such a network takes a
// connection from it ahead of the session's pattern, and an account on an
// address takes one ahead of the session's name, whatever the name resolves
// to. A pattern with a letter in it matches the name the server resolves an
// address to, which the guard cannot know, so an account on an address takes
// a connection ahead of it too. Against a session whose host is an address,
// or a network, the guard compares the addresses. Anywhere else a host covers
// what its pattern matches.
func hostShadows(session, host string) bool {
	sessionAddress, sessionIsAddress := literalAddress(session)
	hostAddress, hostIsAddress := literalAddress(host)
	switch {
	case strings.Contains(host, "/") && sessionIsAddress:
		return networkContains(host, sessionAddress)
	case strings.Contains(host, "/"):
		return true
	case strings.Contains(session, "/"):
		return hostIsAddress && networkContains(session, hostAddress)
	case hostIsAddress && !sessionIsAddress && (!strings.ContainsAny(session, "%_") || strings.ContainsFunc(session, unicode.IsLetter)):
		return true
	default:
		return hostCovers(session, host)
	}
}

// literalAddress reads host as an IP address, and reports false for a host
// that holds a wildcard or a netmask, or that names no address.
func literalAddress(host string) (netip.Addr, bool) {
	if strings.ContainsAny(host, "%_/") {
		return netip.Addr{}, false
	}
	address, err := netip.ParseAddr(host)
	return address.Unmap(), err == nil
}

// networkContains reports whether address is on network, a host written as
// an address and a netmask or a prefix length: address under the mask is
// network's own address, as the server compares them. Measured on MySQL
// 8.4.13 and MariaDB 11.8.9 from 192.168.215.14, an account on
// 192.168.215.0/255.255.255.0 takes the connection and one on
// 192.168.215.77/255.255.255.0 does not, and on MySQL the same holds for
// 192.168.215.0/24 and 192.168.215.77/24. MariaDB reads no prefix length, so
// to it such a host is a name no connection has, which the guard reads as
// MySQL does. A network it cannot read contains every address, so the guard
// refuses what it cannot compare.
func networkContains(network string, address netip.Addr) bool {
	base, mask, _ := strings.Cut(network, "/")
	networkAddress, err := netip.ParseAddr(base)
	if err != nil {
		return true
	}
	networkAddress = networkAddress.Unmap()
	if length, err := strconv.Atoi(mask); err == nil {
		prefix, err := networkAddress.Prefix(length)
		return err != nil || prefix.Addr() == networkAddress && prefix.Contains(address)
	}
	netmask, err := netip.ParseAddr(mask)
	if err != nil || !networkAddress.Is4() || !netmask.Is4() {
		return true
	}
	if !address.Is4() {
		return false
	}
	ip, bits, own := address.As4(), netmask.As4(), networkAddress.As4()
	for index := range ip {
		if ip[index]&bits[index] != own[index] {
			return false
		}
	}
	return true
}

// hostCovers reports whether pattern, read as the server reads the host of
// an account, matches host: letters as [foldHost] folds them, % for any run
// of characters and _ for one character. A host covers itself.
func hostCovers(pattern, host string) bool {
	return wildcardMatch([]rune(foldHost(pattern)), []rune(foldHost(host)))
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
