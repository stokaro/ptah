package postgresreplay

import (
	"context"
	"fmt"
	"strings"

	"ptah.run/internal/devclean/replay"
	"ptah.run/internal/lexer"
)

// settingReach is how long a setting may last and leave every later statement
// read the way the guard reads it.
type settingReach int

const (
	// reachNothing is a setting the guard refuses in every scope.
	reachNothing settingReach = iota
	// reachTransaction is a setting the guard accepts for one transaction:
	// SET LOCAL and set_config(name, value, true).
	reachTransaction
	// reachSession is a setting the guard accepts for the rest of the
	// session as well: SET, RESET and set_config(name, value, false).
	reachSession
)

// postgresSettingReach is the one answer the guard gives about a setting of
// the parameter name, folded to upper case, to value. SET, SET LOCAL, RESET
// and both forms of set_config read it, so two spellings of one setting are
// never judged apart.
//
// The question is whether the statements after the setting are still read the
// way the guard reads them. A replay sends its statements only to the dev
// database. On a dev database it runs every migration of a directory on one
// pinned session, which the run discards when the replay ends; on a shadow
// database it runs them on the shadow connection's pool, whose connections the
// caller closes. A SET, a RESET or set_config(name, value, false) lasts for the
// connection it ran on: for the later statements of the replay sent on it, and
// for the reads and the cleanup the run sends on it afterwards. A SET LOCAL or
// set_config(name, value, true) lasts for the rest of its transaction, which
// is the rest of its migration. Measured on PostgreSQL 18.6: a value set
// either way is gone in a new session, and a local one is gone after COMMIT
// and after ROLLBACK.
//
// A parameter of one of these kinds changes how the guard has to read what
// follows, and is refused in every scope unless its value leaves that reading
// as it is:
//
//   - name resolution. search_path decides where an unqualified name lands,
//     and the guard's protected-namespace check reads only qualified names:
//     `SET LOCAL search_path = pg_catalog` followed by an unqualified CREATE
//     FUNCTION creates the function in pg_catalog. The empty path, one empty
//     string literal, is accepted: under it an unqualified CREATE has no
//     schema to land in, and the server answers `no schema has been selected
//     to create in` (CockroachDB: `no database or schema specified`), while
//     an unqualified name is looked up only in pg_catalog and the session's
//     temporary schema, which every path searches first when it does not name
//     them. So a later statement reaches nothing it could not reach under the
//     path the session started with. pg_dump sets search_path to the empty
//     string with set_config, for the session, before its first object. DEFAULT
//     and RESET give back the path the session started with, which is the one
//     the guard reads every statement before them under. SET SCHEMA, its
//     alias, is refused, and so is CockroachDB's database, which picks the
//     database an unqualified name lands in.
//   - identity. role and session_authorization decide who every later
//     statement runs as, and so what it may reach.
//   - how the server reads the bytes of a later statement. The guard reads
//     every statement as UTF-8 with standard-conforming strings, where a
//     backslash in a plain string literal is an ordinary character, and
//     [guard.ValidateSession] refuses a session that starts otherwise.
//     standard_conforming_strings is accepted on, and client_encoding (and SET
//     NAMES, which sets it) in UTF-8. Measured on PostgreSQL 18.6, the other
//     values let a later statement the guard reads as one SELECT run a CREATE
//     ROLE: under `SET LOCAL standard_conforming_strings = off` the backslash
//     in `'x\' AS a, ' ; CREATE ROLE ...; -- '` escapes the quote the guard
//     reads as the end of the string, and under `SET LOCAL client_encoding =
//     'GBK'` the byte 0x81 and the backslash after it are one character, so
//     the escape the guard reads in an E'...' string is not there. PostgreSQL
//     19 refuses standard_conforming_strings off itself, with SQLSTATE 0A000;
//     PostgreSQL 18 and YugabyteDB accept it. DEFAULT and RESET are refused
//     for both. They give back the value the session started with, which only
//     [guard.ValidateSession] reads, and it reads nothing under trust, where
//     these refusals stay.
//   - the catalogs the guard protects. allow_system_table_mods lets a
//     superuser change the structure of pg_catalog.
//
// A setting of any other parameter is accepted for one transaction. Its effect
// ends with the migration it belongs to, which is the scope SET LOCAL has in a
// replay: a lock or statement timeout, a time zone, a DateStyle, or a custom
// parameter such as app.tenant_id that a row-level security policy reads.
//
// The parameters below are accepted for the session too, with any value,
// since none of them changes what a later statement reaches, how it is read,
// or how the run reads the dev database back. They are what the header of a
// `pg_dump --schema-only` sets, measured on pg_dump 18.6:
//
//   - statement_timeout, lock_timeout, idle_in_transaction_session_timeout,
//     idle_session_timeout and transaction_timeout end a statement, a wait or
//     the session early. A statement they end has no effect, and one the run
//     sends on the session afterwards fails with an error rather than doing
//     something else;
//   - client_min_messages decides which notices reach the client;
//   - check_function_bodies decides whether CREATE FUNCTION checks the body,
//     and the guard refuses or trusts a routine as a whole either way;
//   - row_security off makes a query that a row-level security policy would
//     filter fail rather than return fewer rows;
//   - xmloption decides how a value of type xml is parsed, not a statement;
//   - default_tablespace and default_table_access_method name, for a later
//     CREATE that names none, the storage it could name itself with a
//     TABLESPACE or USING clause, which the guard accepts on every CREATE.
//
// A parameter that changes how a value is written out, such as DateStyle,
// IntervalStyle, TimeZone, extra_float_digits or quote_all_identifiers, stays
// in the transaction scope: set for the session, it would change how the run
// reads the dev database back, as the text of a column default.
func postgresSettingReach(name string, value settingValue) settingReach {
	switch name {
	case "SEARCH_PATH":
		if value.reset || value.known && value.word == "" {
			return reachSession
		}
		return reachNothing
	case "STANDARD_CONFORMING_STRINGS":
		if value.known && (value.word == "ON" || value.word == "TRUE") {
			return reachSession
		}
		return reachNothing
	case "CLIENT_ENCODING":
		if value.known && utf8EncodingName(value.word) {
			return reachSession
		}
		return reachNothing
	case "", "SCHEMA", "DATABASE", "ROLE", "SESSION_AUTHORIZATION", "ALLOW_SYSTEM_TABLE_MODS":
		return reachNothing
	case "STATEMENT_TIMEOUT", "LOCK_TIMEOUT", "IDLE_IN_TRANSACTION_SESSION_TIMEOUT",
		"IDLE_SESSION_TIMEOUT", "TRANSACTION_TIMEOUT", "CLIENT_MIN_MESSAGES",
		"CHECK_FUNCTION_BODIES", "ROW_SECURITY", "XMLOPTION", "DEFAULT_TABLESPACE",
		"DEFAULT_TABLE_ACCESS_METHOD":
		return reachSession
	default:
		return reachTransaction
	}
}

// settingValue is the value a setting gives its parameter, as far as the
// guard reads it.
type settingValue struct {
	// reset reports DEFAULT and RESET, which give the parameter back the value
	// the session started with.
	reset bool
	// known reports that the value is one word the guard read, which word
	// holds folded to upper case; the empty string literal reads as the empty
	// word.
	known bool
	word  string
}

// postgresSetting is what one SET, RESET or set_config call changes.
type postgresSetting struct {
	// name is the parameter, folded to upper case, with a custom parameter's
	// prefix and dot: APP.TENANT_ID.
	name  string
	value settingValue
	// local reports SET LOCAL and set_config(name, value, true), whose effect
	// ends with the transaction.
	local bool
}

// accepted reports whether the guard accepts s, as [postgresSettingReach]
// answers for its scope.
func (s postgresSetting) accepted() bool {
	reach := postgresSettingReach(s.name, s.value)
	return reach == reachSession || s.local && reach == reachTransaction
}

// guardReading reports a parameter whose value decides how the guard reads
// the statements after it; see [postgresSettingReach]. Its refusal names it.
func (s postgresSetting) guardReading() bool {
	switch s.name {
	case "SEARCH_PATH", "STANDARD_CONFORMING_STRINGS", "CLIENT_ENCODING":
		return true
	}
	return false
}

// postgresSettingOperation names a SET or RESET the guard refuses, or "" for
// one it accepts: a setting [postgresSetting.accepted] accepts, or SET
// CONSTRAINTS, which lasts for its transaction whatever it names. The refusal
// of a parameter that decides how the guard reads what follows names the
// parameter.
func postgresSettingOperation(first string, tokens []lexer.Token) string {
	if first == "SET" && len(tokens) >= 3 && replay.TokenSequenceAt(tokens, 1, "CONSTRAINTS") {
		return ""
	}
	setting, ok := readPostgresSetting(tokens)
	switch {
	case ok && setting.accepted():
		return ""
	case ok && setting.guardReading():
		return first + " " + strings.ToLower(setting.name)
	default:
		return first + " session or transaction state"
	}
}

// readPostgresSetting reads tokens, a SET or RESET statement, as the setting
// it makes. It reports false for a form it does not read as one parameter set
// to one value: RESET ALL, SET TRANSACTION, SET SESSION CHARACTERISTICS, SET
// ROLE, SET SESSION AUTHORIZATION, SET CONSTRAINTS, SET ... FROM CURRENT, and
// a SET with no value.
func readPostgresSetting(tokens []lexer.Token) (postgresSetting, bool) {
	switch replay.NormalizedIdentifier(replay.TokenAt(tokens, 0)) {
	case "RESET":
		return readPostgresReset(tokens)
	case "SET":
	default:
		return postgresSetting{}, false
	}
	at := 1
	var setting postgresSetting
	switch {
	case replay.TokenSequenceAt(tokens, at, "LOCAL"):
		setting.local = true
		at++
	case replay.TokenSequenceAt(tokens, at, "SESSION"):
		at++
	}
	switch {
	case replay.TokenSequenceAt(tokens, at, "SESSION"), replay.TokenSequenceAt(tokens, at, "ROLE"),
		replay.TokenSequenceAt(tokens, at, "TRANSACTION"), replay.TokenSequenceAt(tokens, at, "CONSTRAINTS"):
		return postgresSetting{}, false
	case replay.TokenSequenceAt(tokens, at, "TIME", "ZONE"):
		setting.name, at = "TIMEZONE", at+2
	case replay.TokenSequenceAt(tokens, at, "XML", "OPTION"):
		setting.name, at = "XMLOPTION", at+2
	case replay.TokenSequenceAt(tokens, at, "NAMES"):
		setting.name, at = "CLIENT_ENCODING", at+1
	case replay.TokenSequenceAt(tokens, at, "SCHEMA"):
		setting.name, at = "SCHEMA", at+1
	default:
		name, next, ok := settingName(tokens, at)
		if !ok || next >= len(tokens) {
			return postgresSetting{}, false
		}
		if !tokens[next].MatchOperatorValue("=") && !replay.TokenSequenceAt(tokens, next, "TO") {
			return postgresSetting{}, false
		}
		setting.name, at = name, next+1
	}
	value := tokens[at:]
	if len(value) == 0 || replay.TokenSequenceAt(value, 0, "FROM", "CURRENT") {
		return postgresSetting{}, false
	}
	setting.value = setStatementValue(value)
	return setting, true
}

// readPostgresReset reads RESET name, RESET TIME ZONE and RESET SESSION
// AUTHORIZATION. RESET ALL gives every parameter back the value the session
// started with, some of which the guard does not read, and is not read.
func readPostgresReset(tokens []lexer.Token) (postgresSetting, bool) {
	reset := settingValue{reset: true}
	switch {
	case replay.TokenSequenceAt(tokens, 1, "ALL"):
		return postgresSetting{}, false
	case len(tokens) == 3 && replay.TokenSequenceAt(tokens, 1, "TIME", "ZONE"):
		return postgresSetting{name: "TIMEZONE", value: reset}, true
	case len(tokens) == 3 && replay.TokenSequenceAt(tokens, 1, "SESSION", "AUTHORIZATION"):
		return postgresSetting{name: "SESSION_AUTHORIZATION", value: reset}, true
	}
	name, next, ok := settingName(tokens, 1)
	if !ok || next != len(tokens) {
		return postgresSetting{}, false
	}
	return postgresSetting{name: name, value: reset}, true
}

// settingName reads the parameter name at index: a word, or a custom
// parameter's prefix and name joined by a dot, either of which may be quoted.
// It returns the name folded to upper case, as the server matches a parameter
// name in any case, and the index after it.
func settingName(tokens []lexer.Token, index int) (string, int, bool) {
	parts, next := replay.QualifiedIdentifierAt(tokens, index)
	if len(parts) == 0 || len(parts) > 2 {
		return "", index, false
	}
	return strings.ToUpper(strings.Join(parts, ".")), next, true
}

// setStatementValue reads the value of a SET: DEFAULT, or one word, which may
// be an unquoted word, a quoted name or a plain string literal, all of which
// SET reads as the text they spell. A list, an escape string, a Unicode
// escape, a dollar-quoted string and an expression are not read as a word.
func setStatementValue(value []lexer.Token) settingValue {
	if len(value) != 1 {
		return settingValue{}
	}
	token := value[0]
	switch {
	case token.Type == lexer.TokenIdentifier && replay.NormalizedIdentifier(token) == "DEFAULT":
		return settingValue{reset: true}
	case token.Type == lexer.TokenIdentifier:
		return settingValue{known: true, word: strings.ToUpper(token.Value)}
	case quotedIdentifier(token):
		return settingValue{known: true, word: strings.ToUpper(replay.IdentifierValue(token))}
	}
	if literal, ok := plainStringLiteral(token); ok {
		return settingValue{known: true, word: strings.ToUpper(literal)}
	}
	return settingValue{}
}

// postgresSetConfig reads the set_config call whose opening parenthesis is at
// open as the setting it makes. set_config(name, value, true) is SET LOCAL
// spelled as a function and ends with its transaction the same way; with
// false it is a plain SET.
//
// It reports false unless the call has three arguments, the name is one plain
// string literal and is_local is the keyword TRUE or FALSE: a name computed at
// run time, or a column in its place, cannot be read here, and a string such
// as 'on' for is_local is left out because the one spelling the guard accepts
// should be the one a reader recognizes. The value is an expression, so it is
// read as a word only when it is one plain string literal: a column named
// utf8 is a value the guard cannot read, and so is NULL, which set_config
// reads as RESET.
func postgresSetConfig(tokens []lexer.Token, open int) (postgresSetting, bool) {
	arguments := postgresCallArguments(tokens, open)
	if len(arguments) != 3 || len(arguments[0]) != 1 || len(arguments[1]) == 0 || len(arguments[2]) != 1 {
		return postgresSetting{}, false
	}
	name, ok := plainStringLiteral(arguments[0][0])
	if !ok {
		return postgresSetting{}, false
	}
	setting := postgresSetting{name: strings.ToUpper(name)}
	if len(arguments[1]) == 1 {
		if word, ok := plainStringLiteral(arguments[1][0]); ok {
			setting.value = settingValue{known: true, word: strings.ToUpper(word)}
		}
	}
	isLocal := arguments[2][0]
	if isLocal.Type != lexer.TokenIdentifier {
		return postgresSetting{}, false
	}
	switch replay.NormalizedIdentifier(isLocal) {
	case "TRUE":
		setting.local = true
	case "FALSE":
	default:
		return postgresSetting{}, false
	}
	return setting, true
}

// plainStringLiteral returns the text of a plain string literal, '...', and
// reports false for any other token: an escape string, a Unicode escape and a
// dollar-quoted string among them.
func plainStringLiteral(token lexer.Token) (string, bool) {
	value := token.Value
	if token.Type != lexer.TokenString || len(value) < 2 || value[0] != '\'' || value[len(value)-1] != '\'' {
		return "", false
	}
	return strings.ReplaceAll(value[1:len(value)-1], "''", "'"), true
}

// utf8EncodingName reports an encoding name PostgreSQL reads as UTF8. The
// server compares a name by its letters and digits alone, in any case, so
// UTF8, utf-8 and Unicode all name it.
func utf8EncodingName(name string) bool {
	cleaned := strings.Map(func(r rune) rune {
		switch {
		case 'A' <= r && r <= 'Z', '0' <= r && r <= '9':
			return r
		case 'a' <= r && r <= 'z':
			return r - ('a' - 'A')
		}
		return -1
	}, name)
	return cleaned == "UTF8" || cleaned == "UNICODE"
}

// ValidateSession refuses a session that does not read a statement's bytes
// the way the guard does, and otherwise returns the guard unchanged: it reads
// a statement the same way on every session it accepts. The guard reads as
// UTF-8, with standard-conforming strings. A dev URL's options, a database's
// or a role's defaults, or the server's configuration can start a session
// otherwise, and the server then reads the same bytes by other rules and can
// run what the guard reads as a string; see [postgresSettingReach] for the
// measurement. CockroachDB accepts no other values, and is asked all the same.
//
// A run that trusts its SQL is not refused, and the session is not read:
// trust lifts a DO block, which can change either setting as well as a SET
// can, so the refusal would protect nothing there. What trust gives up is that
// the guard reads the statements it still refuses under trust as the server
// reads them.
func (g guard) ValidateSession(ctx context.Context, session replay.Session, scope replay.Scope) (replay.Guard, error) {
	if scope.TrustedSQL {
		return g, nil
	}
	var encoding, conforming string
	err := session.QueryRowContext(ctx,
		"SELECT current_setting('client_encoding'), current_setting('standard_conforming_strings')",
	).Scan(&encoding, &conforming)
	if err != nil {
		return nil, fmt.Errorf("read the dev session's client_encoding and standard_conforming_strings: %w", err)
	}
	if utf8EncodingName(encoding) && strings.EqualFold(conforming, "on") {
		return g, nil
	}
	return nil, &replay.SessionRefusal{Reason: fmt.Sprintf(
		"the dev session reads statements with client_encoding %q and standard_conforming_strings %q, and the %s "+
			"replay guard reads each statement as UTF-8 with standard-conforming strings: the server reads the "+
			"same bytes by other rules and can run what the guard does not see; connect with client_encoding "+
			"UTF8 and standard_conforming_strings on",
		encoding, conforming, g.dialect,
	)}
}
