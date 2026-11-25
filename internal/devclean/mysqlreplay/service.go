// Package mysqlreplay is the MySQL and MariaDB migration replay guard: it
// refuses a statement whose effects cannot be confined to the dev database,
// such as an executable comment, a stored body or a write to another
// database, and lifts what a server the run owns, a server named as a whole,
// or a run that trusts its SQL lets a replay change. internal/devclean asks it through the
// internal/devclean/replay contract.
package mysqlreplay

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/internal/devclean/replay"
	"ptah.run/internal/mysqlstored"
)

// Services returns the replay guards of MySQL and MariaDB, which read the
// same SQL the same way.
func Services() []replay.Service {
	return []replay.Service{
		{Dialect: platform.MySQL, Guard: guard{dialect: platform.MySQL}},
		{Dialect: platform.MariaDB, Guard: guard{dialect: platform.MariaDB}},
	}
}

type guard struct {
	dialect string
}

// Validate refuses a statement whose effects cannot be confined to scope.
//
// A migration can change the replay session's sql_mode, with an expression
// the guard does not evaluate, and some modes change where a string, a quoted
// name or a comment ends: NO_BACKSLASH_ESCAPES makes a backslash in a string a
// character, ANSI_QUOTES makes "..." a name in which a backslash is a
// character, and MariaDB's MSSQL mode makes [...] a name. So `'a\'` is one
// string by one rule and runs on past its end by another, and a definer, a
// target or a comment can stand on either side of the quote. A statement is
// checked under every combination of the modes whose quote it holds, and
// refused when any reading refuses it.
func (g guard) Validate(stmt string, scope replay.Scope) error {
	for _, reading := range readings(g.dialect, stmt) {
		if err := g.validate(stmt, scope, reading); err != nil {
			return err
		}
	}
	return nil
}

// readings are the ways a session can read stmt: the default rules, and each
// combination of the modes that change how a character stmt holds is read --
// NO_BACKSLASH_ESCAPES for a backslash, ANSI_QUOTES for a double quote, and
// MariaDB's MSSQL mode for an opening square bracket. The last is read on
// either dialect, since a mysql URL can reach a MariaDB server.
func readings(dialect, stmt string) []mysqlstored.Server {
	readings := []mysqlstored.Server{{Dialect: dialect}}
	variants := []struct {
		holds string
		set   func(*mysqlstored.Server)
	}{
		{holds: `\`, set: func(server *mysqlstored.Server) { server.NoBackslashEscapes = true }},
		{holds: `"`, set: func(server *mysqlstored.Server) { server.AnsiQuotes = true }},
		{holds: `[`, set: func(server *mysqlstored.Server) { server.BracketNames = true }},
	}
	for _, variant := range variants {
		if !strings.Contains(stmt, variant.holds) {
			continue
		}
		for _, reading := range slices.Clone(readings) {
			variant.set(&reading)
			readings = append(readings, reading)
		}
	}
	return readings
}

// ValidateSession refuses a session whose client character set is outside
// the UTF-8 family, as a dev URL's charset or collation parameter or the
// server's own default can make it. The guard reads every statement as UTF-8,
// and such a session reads the same bytes by other rules, so the server can
// run a statement the guard reads as harmless; see
// [mysqlstored.UTF8CharacterSet].
//
// A run that trusts its SQL is not refused, and the session is not read:
// trust already lifts a prepared statement, which can change the character
// set as well as SET NAMES can, so the refusal would protect nothing there.
// What trust gives up is that the guard reads the statements it still
// refuses under trust as the server reads them.
func (g guard) ValidateSession(ctx context.Context, session replay.Session, scope replay.Scope) error {
	if scope.TrustedSQL {
		return nil
	}
	var charset string
	if err := session.QueryRowContext(ctx, "SELECT @@character_set_client").Scan(&charset); err != nil {
		return fmt.Errorf("read the dev session's client character set: %w", err)
	}
	if mysqlstored.UTF8CharacterSet(charset) {
		return nil
	}
	return &replay.SessionRefusal{Reason: fmt.Sprintf(
		"the dev session's client character set is %q, and the %s replay guard reads each statement as UTF-8: "+
			"the server reads the same bytes as %s and can run what the guard does not see; "+
			"connect with a utf8mb4 client character set, such as charset=utf8mb4 in the dev URL",
		charset, g.dialect, charset,
	)}
}

// validate is [guard.Validate] for the statement read by reading's rules.
func (g guard) validate(stmt string, scope replay.Scope, reading mysqlstored.Server) error {
	if err := rejectMySQLExecutableComments(g.dialect, stmt, reading); err != nil {
		return err
	}
	tokens := replay.Tokens(stmt, mysqlstored.LexerOptions(reading))
	if err := rejectUnreadableDefinition(g.dialect, stmt, tokens); err != nil {
		return err
	}
	// A statement another runs is checked as its own, in the same realm and
	// reading, and the rest is checked without it: the variables a SET
	// STATEMENT sets, or the words of an ANALYZE.
	if start, ok := mysqlstored.Wrapped(tokens); ok {
		if err := g.validate(stmt[tokens[start].Start:], scope, reading); err != nil {
			return err
		}
		tokens = tokens[:start]
	}
	// A run that trusts its SQL lifts the same statements a server the run
	// owns lifts, and for the same reason: their only effect the guard cannot
	// confine is on the rest of the server. See [validateMySQLReplayStatement]
	// for what trust lifts beyond them and what it keeps.
	if (scope.Server || scope.TrustedSQL) && mysqlServerWideOperation(stmt, tokens) != "" {
		return nil
	}
	if scope.Databases && mysqlDatabaseOperation(tokens) {
		return nil
	}
	return validateMySQLReplayStatement(g.dialect, scope.Database, stmt, tokens, scope)
}
