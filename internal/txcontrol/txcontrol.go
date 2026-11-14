// Package txcontrol says whether one SQL statement controls transaction state:
// opens, ends or marks a transaction, or changes how the session ends one.
//
// The migrator refuses such a statement in a migration body whenever it owns
// the transaction or checkpoints each statement itself, and migration lint
// reports the same statements before an apply does. Both read this one
// predicate, so the two cannot disagree about which statements count. It sits
// beside internal/migratorext rather than inside it because the engine
// differences it reads, such as whether BEGIN opens a block, come from the
// services in internal/migratorext/builtin, which import migratorext.
package txcontrol

import (
	"slices"

	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
	"ptah.run/internal/migratorext"
	"ptah.run/internal/migratorext/builtin"
)

// Controls reports whether statement, one statement of dialect's SQL, controls
// transaction state. Comments and whitespace do not count, and an empty
// statement controls nothing. A dialect the registry does not know is read
// with the generic grammar: BEGIN and END are transaction control there.
//
// Where an engine's BEGIN and END delimit a block, as on SQL Server, only
// BEGIN TRAN, BEGIN TRANSACTION and BEGIN DISTRIBUTED open a transaction, and a
// BEGIN ... END body is not control. Where an engine has an autocommit
// switch, as MySQL and MariaDB do, SET autocommit is control too.
func Controls(statement, dialect string) bool {
	options := dialectlexer.Options(dialect)
	tokens := significantTokens(statement, options)
	if len(tokens) == 0 {
		return false
	}
	services := builtin.Lookup(dialect)
	return isPrimary(tokens, services) || isSet(statement, options, tokens, services)
}

// significantTokens tokenizes source and drops whitespace and comments.
func significantTokens(source string, options lexer.Options) []lexer.Token {
	lexr := lexer.NewLexerWithOptions(source, options)
	var tokens []lexer.Token
	for {
		token := lexr.NextToken()
		switch token.Type {
		case lexer.TokenEOF:
			return tokens
		case lexer.TokenWhitespace, lexer.TokenComment:
			continue
		default:
			tokens = append(tokens, token)
		}
	}
}

// isPrimary reports whether a statement opens, ends or marks a transaction.
func isPrimary(tokens []lexer.Token, services migratorext.Services) bool {
	first := tokens[0]
	if matchesAny(first, "COMMIT", "ROLLBACK", "SAVEPOINT", "ABORT") {
		return true
	}
	if first.MatchIdentifierValue("END") && !services.BlockBegin {
		return true
	}
	if first.MatchIdentifierValue("XA") {
		return true
	}
	if first.MatchIdentifierValue("SAVE") {
		return len(tokens) > 1 && matchesAny(tokens[1], "TRAN", "TRANSACTION")
	}
	if first.MatchIdentifierValue("PREPARE") {
		return len(tokens) > 1 && tokens[1].MatchIdentifierValue("TRANSACTION")
	}
	if first.MatchIdentifierValue("RELEASE") {
		return len(tokens) > 1 && tokens[1].MatchIdentifierValue("SAVEPOINT")
	}
	if first.MatchIdentifierValue("START") {
		return len(tokens) > 1 && tokens[1].MatchIdentifierValue("TRANSACTION")
	}
	if first.MatchIdentifierValue("BEGIN") {
		if !services.BlockBegin {
			return true
		}
		return len(tokens) > 1 && matchesAny(tokens[1], "TRAN", "TRANSACTION", "DISTRIBUTED")
	}
	return false
}

// isSet reports a SET statement that changes the transaction's characteristics
// or the session's autocommit mode.
func isSet(statement string, options lexer.Options, tokens []lexer.Token, services migratorext.Services) bool {
	if !tokens[0].MatchIdentifierValue("SET") {
		return false
	}
	if migratorext.ContainsIdentifierSequence(statement, options, "TRANSACTION") ||
		migratorext.ContainsIdentifierSequence(statement, options, "IMPLICIT_TRANSACTIONS") {
		return true
	}
	return services.AutocommitControl && migratorext.ContainsIdentifierSequence(statement, options, "AUTOCOMMIT")
}

func matchesAny(token lexer.Token, keywords ...string) bool {
	return slices.ContainsFunc(keywords, token.MatchIdentifierValue)
}
