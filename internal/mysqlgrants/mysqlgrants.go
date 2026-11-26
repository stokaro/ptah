// Package mysqlgrants reads what SHOW GRANTS prints for a MySQL or MariaDB
// session and answers which privileges the session holds on a database or a
// table.
//
// A schema reader and the migrator's preflight need this because
// information_schema lists only what an account holds a privilege on, and neither
// engine has a catalog an ordinary account can read instead. SHOW GRANTS is the one
// statement every account may run about itself, and it covers what the session
// holds through a role: both engines print an active role's privileges, and neither
// prints an inactive role's.
//
// Privileges on one object are not simply added up. These rules were measured on
// MySQL 8.4 and 26.10 and on MariaDB 11.8 and 12.3:
//
//   - A database-level row is a LIKE pattern, and the server applies one row
//     that matches rather than all of them. `GRANT TRIGGER ON `f%`.*` beside
//     `GRANT SELECT ON `fx`.*` leaves an account with SELECT on fx and no
//     TRIGGER. [Options.ApplyingRows] says which row that is.
//   - MariaDB looks the account, the active role with the roles it inherits,
//     and PUBLIC up apart, and adds the results; it prints each role's rows
//     under the role's name. MySQL prints an active role's rows as the
//     account's own, and looks them up together.
//   - With partial_revokes on, MySQL compares a database name literally and
//     prints a partial revoke as a REVOKE line.
//
// Where this package cannot tell which of several rows the server uses, it
// credits only what all of them hold, and a line it cannot read grants nothing.
// A privilege credited wrongly becomes an object a comparison reads as absent; a
// privilege not credited becomes a refusal that names it.
package mysqlgrants

import (
	"maps"
	"strings"

	"ptah.run/internal/mysqlname"
)

// Options are the server facts that decide how a row applies.
type Options struct {
	// LiteralDatabaseNames reports partial_revokes on: `%` and `_` in a
	// database-level row are ordinary characters.
	LiteralDatabaseNames bool
	// FoldCase reports lower_case_table_names other than 0: names compare
	// without regard to case.
	FoldCase bool
	// MariaDB reports a MariaDB server, which orders database-level rows its
	// own way and prints a role's rows under the role's name. Otherwise the
	// server is MySQL, which prints an active role's rows as the account's
	// own: while a role is granted, SHOW GRANTS does not say whether it is
	// active, so a row credits a privilege only where the account's lookup
	// and the role lookup both apply one holding it.
	MariaDB bool
}

// Privileges is a set of privileges on one object.
type Privileges struct {
	all   bool
	named map[string]struct{}
}

// Holds reports whether the set holds any of names. A name is the privilege as
// SHOW GRANTS spells it, in upper case, such as `SHOW VIEW`. `ALL PRIVILEGES`
// holds every name, so a question about a privilege that exists only globally,
// such as SHOW_ROUTINE, belongs on [Grants.Global].
func (p Privileges) Holds(names ...string) bool {
	if p.all {
		return true
	}
	for _, name := range names {
		if _, held := p.named[name]; held {
			return true
		}
	}
	return false
}

// Grantable reports whether the session may grant every one of names onward:
// the set carries the grant option and holds each name. `ALL PRIVILEGES` holds
// every name and not the grant option, which a row carries only when SHOW
// GRANTS prints it WITH GRANT OPTION. With no names it asks for the grant
// option alone.
func (p Privileges) Grantable(names ...string) bool {
	if _, option := p.named[grantOption]; !option {
		return false
	}
	if p.all {
		return true
	}
	for _, name := range names {
		if _, held := p.named[name]; !held {
			return false
		}
	}
	return true
}

// grantOption is the name a row's WITH GRANT OPTION joins its set under.
const grantOption = "GRANT OPTION"

func (p Privileges) union(other Privileges) Privileges {
	merged := Privileges{all: p.all || other.all, named: make(map[string]struct{}, len(p.named)+len(other.named))}
	maps.Copy(merged.named, p.named)
	maps.Copy(merged.named, other.named)
	return merged
}

func (p Privileges) intersect(other Privileges) Privileges {
	switch {
	case p.all:
		return other
	case other.all:
		return p
	}
	common := Privileges{named: make(map[string]struct{}, len(p.named))}
	for name := range p.named {
		if _, held := other.named[name]; held {
			common.named[name] = struct{}{}
		}
	}
	return common
}

func (p Privileges) without(other Privileges) Privileges {
	if other.all {
		return Privileges{}
	}
	left := Privileges{all: p.all, named: make(map[string]struct{}, len(p.named))}
	for name := range p.named {
		if _, gone := other.named[name]; !gone {
			left.named[name] = struct{}{}
		}
	}
	return left
}

// level is what a row applies to.
type level uint8

const (
	levelGlobal level = iota
	levelDatabase
	levelTable
)

// row is one privilege line of SHOW GRANTS.
type row struct {
	revoke     bool
	grantee    string
	level      level
	database   string
	table      string
	privileges Privileges
}

// Grants is what one session's SHOW GRANTS printed.
type Grants struct {
	rows        []row
	options     Options
	roleGranted bool
}

// Parse reads the lines SHOW GRANTS printed.
func Parse(lines []string, options Options) Grants {
	grants := Grants{options: options}
	for _, line := range lines {
		parsed, shape := parseLine(line)
		switch shape {
		case shapePrivileges:
			grants.rows = append(grants.rows, parsed)
		case shapeRole:
			grants.roleGranted = true
		case shapeOther:
		}
	}
	return grants
}

// Global returns what the session holds on every database, less what a partial
// revoke takes away on database. An empty database asks about no one database.
func (g Grants) Global(database string) Privileges {
	held := Privileges{}
	for _, r := range g.rows {
		if r.level == levelGlobal && !r.revoke {
			held = held.union(r.privileges)
		}
	}
	for _, r := range g.rows {
		if r.level == levelDatabase && r.revoke && g.sameName(r.database, database) {
			held = held.without(r.privileges)
		}
	}
	return held
}

// OnDatabase returns what the session holds on every table of database: what it
// holds globally and at the database level, and not what it holds on one table.
func (g Grants) OnDatabase(database string) Privileges {
	return g.Global(database).union(g.databaseLevel(database))
}

// OnTable returns what the session holds on one table of database.
func (g Grants) OnTable(database, table string) Privileges {
	held := g.OnDatabase(database)
	for _, r := range g.rows {
		if r.level == levelTable && !r.revoke && g.sameName(r.database, database) && g.sameName(r.table, table) {
			held = held.union(r.privileges)
		}
	}
	return held
}

// databaseLevel adds what each subject holds through the database-level row
// the server applies to it, as [Options.ApplyingRows] decides. On MariaDB the
// subjects are the account, the active role with the roles it inherits, and
// PUBLIC; on MySQL every row printed is the account's. With a role granted on
// MySQL, SHOW GRANTS does not say whether it is active, so the rows either
// lookup applies are crossed together.
func (g Grants) databaseLevel(database string) Privileges {
	subjects := make(map[subject][]row)
	for _, r := range g.rows {
		if r.level == levelDatabase && !r.revoke {
			key := g.subjectOf(r.grantee)
			subjects[key] = append(subjects[key], r)
		}
	}
	held := Privileges{}
	for _, rows := range subjects {
		held = held.union(g.applied(database, rows))
	}
	return held
}

// applied is what one subject holds at the database level: what every row
// that may apply holds, each name holding what all its rows hold.
func (g Grants) applied(database string, rows []row) Privileges {
	names := make([]string, len(rows))
	for i, r := range rows {
		names[i] = r.database
	}
	applying := g.options.ApplyingRows(AccountLookup, database, names)
	if !g.options.MariaDB && g.roleGranted {
		applying = distinct(append(applying, g.options.ApplyingRows(RoleLookup, database, names)...))
	}
	held := Privileges{}
	for i, name := range applying {
		byName := Privileges{}
		for _, r := range rows {
			if r.database == name {
				byName = byName.union(r.privileges)
			}
		}
		if i == 0 {
			held = byName
			continue
		}
		held = held.intersect(byName)
	}
	return held
}

// subject is one lookup of the database-level rows.
type subject uint8

const (
	subjectAccount subject = iota
	subjectRole
	subjectPublic
)

// subjectOf says which lookup a row printed for grantee belongs to. MySQL
// looks every row it prints up together; MariaDB prints an account as
// `user`@`host`, PUBLIC by that word and a role by its name alone.
func (g Grants) subjectOf(grantee string) subject {
	switch {
	case !g.options.MariaDB:
		return subjectAccount
	case strings.EqualFold(grantee, "PUBLIC"):
		return subjectPublic
	case namesAnAccount(grantee):
		return subjectAccount
	default:
		return subjectRole
	}
}

// namesAnAccount reports whether a grantee holds an `@` outside its quotes,
// which `user`@`host` does and a role name does not.
func namesAnAccount(grantee string) bool {
	var inside byte
	for i := 0; i < len(grantee); i++ {
		switch current := grantee[i]; {
		case inside != 0 && current == inside:
			inside = 0
		case inside == 0 && isQuote(current):
			inside = current
		case inside == 0 && current == '@':
			return true
		}
	}
	return false
}

// MatchesDatabase reports whether a database-level grant row on pattern
// applies to the database name, as the server's privilege check decides it:
// `%` is any run of bytes, `_` is one byte, and a backslash takes the next
// byte literally, whatever the session's sql_mode. A backslash that ends the
// pattern is the byte itself: measured on MySQL 8.4.13 and MariaDB 11.8.9, a
// grant on `ab%\` reaches every table of a database `abx\`. Names compare
// without regard to case where [Options.FoldCase] holds, and literally where
// [Options.LiteralDatabaseNames] does.
//
// It is the one matcher for a database-level row. It says which rows match,
// and [Options.ApplyingRows] says which of them the server applies. The revoke
// refusal reads every row that matches as one that may apply, which fails
// closed for a refusal, so the two policies stay apart.
//
// SQL LIKE does not agree with the server: measured on MySQL 8.4.13 and
// MariaDB 11.8.9, LIKE's `_` matches one character, so `nx__d` matches no
// database `nxéd` while the grant on it reaches every table there, and on
// MySQL NO_BACKSLASH_ESCAPES takes LIKE's escape away, so `ab\_c` matches no
// database `ab_c` while the grant reaches it.
//
// Measured on MySQL 8.4.13, a row a role holds with no `%` or `_` in it
// compares byte for byte, backslashes included, where a user's own row, and
// either row on MariaDB 11.8.9, take a backslash as an escape: through a role
// there, `a\bc` reaches no database `abc` while this reports a match, so the
// read refuses a revoke the server would carry out, which fails closed.
//
// The fold has a unit test and no engine test. No engine test applies a
// pattern on the servers CI starts with lower_case_table_names=1, and a server
// with 2 needs a file system that compares names without case, which no CI
// runner has.
func (o Options) MatchesDatabase(pattern, name string) bool {
	if o.LiteralDatabaseNames {
		return mysqlname.NamesOf(o.FoldCase).Same(pattern, name)
	}
	if o.FoldCase {
		pattern, name = strings.ToLower(pattern), strings.ToLower(name)
	}
	return likeMatch(pattern, name)
}

// sameName reports whether a row's database or table name is name. One setting,
// lower_case_table_names, decides how the server compares both, so a table name
// takes the database name's rule.
func (g Grants) sameName(a, b string) bool {
	return mysqlname.NamesOf(g.options.FoldCase).Same(a, b)
}

// likeMatch is the server's comparison of a name with a privilege pattern: `%`
// is any run of bytes, `_` is one byte, a backslash takes the next byte
// literally, and a backslash that ends the pattern is the byte itself.
func likeMatch(pattern, name string) bool {
	p, n := 0, 0
	retryPattern, retryName := -1, 0
	for n < len(name) {
		switch {
		case p < len(pattern) && pattern[p] == '%':
			p++
			retryPattern, retryName = p, n
		case p < len(pattern) && pattern[p] == '_':
			p++
			n++
		case p+1 < len(pattern) && pattern[p] == '\\' && pattern[p+1] == name[n]:
			p += 2
			n++
		case p < len(pattern) && (pattern[p] != '\\' || p+1 == len(pattern)) && pattern[p] == name[n]:
			p++
			n++
		case retryPattern >= 0:
			retryName++
			p, n = retryPattern, retryName
		default:
			return false
		}
	}
	for p < len(pattern) && pattern[p] == '%' {
		p++
	}
	return p == len(pattern)
}

// shape is what a line of SHOW GRANTS turned out to be.
type shape uint8

const (
	// shapeOther is a line that grants nothing this package reads: SET DEFAULT
	// ROLE, a routine-level row, or a line it cannot read.
	shapeOther shape = iota
	// shapePrivileges is a global, database-level or table-level row.
	shapePrivileges
	// shapeRole is a grant of a role, such as GRANT `r`@`%` TO `a`@`%`.
	shapeRole
)

// allPrivileges is `ALL PRIVILEGES` and its short spelling.
var allPrivileges = map[string]bool{"ALL": true, "ALL PRIVILEGES": true}

func parseLine(line string) (row, shape) {
	s := lineScanner{text: line}
	verb, _, ok := s.next()
	if !ok {
		return row{}, shapeOther
	}
	var parsed row
	switch strings.ToUpper(verb) {
	case "GRANT":
	case "REVOKE":
		parsed.revoke = true
	default:
		return row{}, shapeOther
	}

	privileges, listed := s.privilegeList()
	if listed != shapePrivileges {
		return row{}, listed
	}
	parsed.privileges = privileges

	if !s.scope(&parsed) {
		return row{}, shapeOther
	}
	direction, quoted, ok := s.next()
	if !ok || quoted || (!strings.EqualFold(direction, "TO") && !strings.EqualFold(direction, "FROM")) {
		return row{}, shapeOther
	}
	parsed.grantee = s.rawWord()
	if strings.Contains(strings.ToUpper(s.text[s.at:]), "WITH GRANT OPTION") {
		parsed.privileges.named[grantOption] = struct{}{}
	}
	return parsed, shapePrivileges
}

// lineScanner walks one line of SHOW GRANTS. A quoted token is an identifier in
// backticks, or in double quotes under ANSI_QUOTES, or a string; a doubled quote
// inside it stands for one, and a backslash is an ordinary character, which is
// what keeps a pattern such as `ptah\_test` intact.
type lineScanner struct {
	text string
	at   int
}

func (s *lineScanner) skipSpace() {
	for s.at < len(s.text) && isSpace(s.text[s.at]) {
		s.at++
	}
}

func isSpace(b byte) bool { return b == ' ' || b == '\t' || b == '\n' || b == '\r' }

func isQuote(b byte) bool { return b == '`' || b == '"' || b == '\'' }

func isPunctuation(b byte) bool {
	return b == ',' || b == '(' || b == ')' || b == '.' || b == '@' || b == '*'
}

// next returns the next token, and whether it was quoted.
func (s *lineScanner) next() (token string, quoted, ok bool) {
	s.skipSpace()
	if s.at >= len(s.text) {
		return "", false, false
	}
	first := s.text[s.at]
	switch {
	case isQuote(first):
		return s.quotedToken(first), true, true
	case isPunctuation(first):
		s.at++
		return string(first), false, true
	}
	start := s.at
	for s.at < len(s.text) && !isSpace(s.text[s.at]) && !isQuote(s.text[s.at]) && !isPunctuation(s.text[s.at]) {
		s.at++
	}
	return s.text[start:s.at], false, true
}

func (s *lineScanner) quotedToken(quote byte) string {
	s.at++
	var token strings.Builder
	for s.at < len(s.text) {
		current := s.text[s.at]
		s.at++
		if current != quote {
			token.WriteByte(current)
			continue
		}
		if s.at >= len(s.text) || s.text[s.at] != quote {
			break
		}
		s.at++
		token.WriteByte(quote)
	}
	return token.String()
}

// rawWord returns the text up to the next space outside quotes, which is how a
// grantee reads: `u`@`%`, 'u'@'h', PUBLIC.
func (s *lineScanner) rawWord() string {
	s.skipSpace()
	start := s.at
	var inside byte
	for s.at < len(s.text) {
		current := s.text[s.at]
		switch {
		case inside != 0 && current == inside:
			inside = 0
		case inside == 0 && isQuote(current):
			inside = current
		case inside == 0 && isSpace(current):
			return s.text[start:s.at]
		}
		s.at++
	}
	return s.text[start:]
}

// privilegeList reads up to and including ON. A privilege followed by a column
// list holds on those columns only, so it does not join the set.
func (s *lineScanner) privilegeList() (Privileges, shape) {
	privileges := Privileges{named: make(map[string]struct{})}
	var words []string
	columnLevel := false
	flush := func() {
		name := strings.Join(words, " ")
		switch {
		case columnLevel || name == "":
		case allPrivileges[name]:
			privileges.all = true
		default:
			privileges.named[name] = struct{}{}
		}
		words, columnLevel = nil, false
	}
	for {
		token, quoted, ok := s.next()
		switch {
		case !ok:
			return Privileges{}, shapeOther
		case quoted:
			return Privileges{}, shapeRole
		case token == "(":
			s.skipColumnList()
			columnLevel = true
		case token == ",":
			flush()
		case strings.EqualFold(token, "ON"):
			flush()
			return privileges, shapePrivileges
		case strings.EqualFold(token, "TO"), strings.EqualFold(token, "FROM"):
			return Privileges{}, shapeRole
		default:
			words = append(words, strings.ToUpper(token))
		}
	}
}

func (s *lineScanner) skipColumnList() {
	for {
		token, quoted, ok := s.next()
		if !ok || (!quoted && token == ")") {
			return
		}
	}
}

// scope reads what a row applies to and reports whether it is a global,
// database-level or table-level row. A routine-level row reads PROCEDURE
// `d`.`p`, with no dot after its first word, and is not one.
func (s *lineScanner) scope(parsed *row) bool {
	first, firstQuoted, ok := s.next()
	if !ok {
		return false
	}
	if !firstQuoted && strings.EqualFold(first, "TABLE") {
		first, firstQuoted, ok = s.next()
		if !ok {
			return false
		}
	}
	dot, dotQuoted, ok := s.next()
	if !ok || dotQuoted || dot != "." {
		return false
	}
	second, secondQuoted, ok := s.next()
	if !ok {
		return false
	}
	secondStar := !secondQuoted && second == "*"
	switch {
	case !firstQuoted && first == "*" && secondStar:
		parsed.level = levelGlobal
	case !firstQuoted && first == "*":
		return false
	case secondStar:
		parsed.level, parsed.database = levelDatabase, first
	default:
		parsed.level, parsed.database, parsed.table = levelTable, first, second
	}
	return true
}
