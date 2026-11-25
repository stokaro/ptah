package mysqlreplay

import (
	"ptah.run/internal/devclean/replay"
	"ptah.run/internal/lexer"
)

// realmLift says which realm lifts the refusal of a [mysqlServerStatement].
type realmLift int

const (
	// liftedNowhere is refused in every realm and under trust: the statement
	// acts on the server or on the identity the run connects as, which the
	// run needs for the rest of the command whoever owns the server.
	liftedNowhere realmLift = iota
	// liftedOnOwnedServer is refused in the database realm and lifted on a
	// server the run owns, and under trust: its only effect the guard cannot
	// confine is on the rest of the server.
	liftedOnOwnedServer
)

// mysqlServerStatement is a statement, known by its first words, whose effect
// is on the server rather than on a database, and the realm that lifts its
// refusal.
type mysqlServerStatement struct {
	words     []string
	operation string
	lift      realmLift
}

// mysqlServerStatements are the server statements the replay guard refuses by
// their first words, each with the realm that lifts it. A replay has no
// reason to stop the server, end a session, change replication or block the
// server's writes, so those are refused in every realm.
var mysqlServerStatements = []mysqlServerStatement{
	// Every realm: stops the server, and every session on it, the run's own.
	{words: []string{"SHUTDOWN"}, operation: "SHUTDOWN", lift: liftedNowhere},
	{words: []string{"RESTART"}, operation: "RESTART", lift: liftedNowhere},
	// Every realm: ends another session or its statement, the run's own
	// included.
	{words: []string{"KILL"}, operation: "KILL", lift: liftedNowhere},
	// Every realm: replays binary log events, which can write any database
	// and bypass every check here.
	{words: []string{"BINLOG"}, operation: "BINLOG", lift: liftedNowhere},
	// Every realm: replication and the binary log, which outlive the run and
	// reach other servers. CHANGE begins only CHANGE REPLICATION SOURCE,
	// CHANGE MASTER and CHANGE REPLICATION FILTER.
	{words: []string{"PURGE"}, operation: "binary log or replication change", lift: liftedNowhere},
	{words: []string{"CHANGE"}, operation: "binary log or replication change", lift: liftedNowhere},
	{words: []string{"START", "REPLICA"}, operation: "binary log or replication change", lift: liftedNowhere},
	{words: []string{"START", "SLAVE"}, operation: "binary log or replication change", lift: liftedNowhere},
	{words: []string{"START", "ALL"}, operation: "binary log or replication change", lift: liftedNowhere},
	{words: []string{"START", "GROUP_REPLICATION"}, operation: "binary log or replication change", lift: liftedNowhere},
	{words: []string{"STOP", "REPLICA"}, operation: "binary log or replication change", lift: liftedNowhere},
	{words: []string{"STOP", "SLAVE"}, operation: "binary log or replication change", lift: liftedNowhere},
	{words: []string{"STOP", "ALL"}, operation: "binary log or replication change", lift: liftedNowhere},
	{words: []string{"STOP", "GROUP_REPLICATION"}, operation: "binary log or replication change", lift: liftedNowhere},
	// Every realm: copies a data directory, this server's or a donor's over
	// it.
	{words: []string{"CLONE"}, operation: "CLONE", lift: liftedNowhere},
	// Every realm: blocks DDL, or with MariaDB's BACKUP STAGE writes, on the
	// whole server until the session ends.
	{words: []string{"LOCK", "INSTANCE"}, operation: "server-wide backup lock", lift: liftedNowhere},
	{words: []string{"BACKUP"}, operation: "server-wide backup lock", lift: liftedNowhere},
	// Every realm: spellings of ALTER USER, which the guard keeps refused on
	// a server the run owns too because it can change the credentials or the
	// roles the run connects with.
	{words: []string{"SET", "PASSWORD"}, operation: "SET PASSWORD", lift: liftedNowhere},
	{words: []string{"SET", "DEFAULT", "ROLE"}, operation: "SET DEFAULT ROLE", lift: liftedNowhere},
	// Database realm only: assigns or preloads the key caches of the server,
	// which every database shares.
	{words: []string{"CACHE", "INDEX"}, operation: "key cache change", lift: liftedOnOwnedServer},
	{words: []string{"LOAD", "INDEX"}, operation: "key cache change", lift: liftedOnOwnedServer},
	// Database realm only: moves threads, other sessions' among them, to
	// another resource group.
	{words: []string{"SET", "RESOURCE", "GROUP"}, operation: "resource group change", lift: liftedOnOwnedServer},
}

// mysqlServerStatementOf returns the server statement tokens begin with.
func mysqlServerStatementOf(tokens []lexer.Token) (mysqlServerStatement, bool) {
	for _, statement := range mysqlServerStatements {
		if replay.TokenSequenceAt(tokens, 0, statement.words...) {
			return statement, true
		}
	}
	return mysqlServerStatement{}, false
}

// mysqlLeadingOperation names the refusal of a statement its first words
// decide: a server statement in every scope, and, unless the run trusts its
// SQL, a compound statement or an IMPORT TABLE. IMPORT TABLE reads table files
// from the server's file system and creates the tables they name, in
// whichever database they name.
func mysqlLeadingOperation(tokens []lexer.Token, scope replay.Scope) string {
	if statement, ok := mysqlServerStatementOf(tokens); ok {
		return statement.operation
	}
	switch {
	case scope.TrustedSQL:
		return ""
	case isMySQLCompoundStatement(tokens):
		return "compound statement"
	case replay.TokenSequenceAt(tokens, 0, "IMPORT", "TABLE"):
		return "IMPORT TABLE external data operation"
	default:
		return ""
	}
}

// isMySQLCompoundStatement reports a compound statement MariaDB runs outside
// a stored program: BEGIN NOT ATOMIC, IF, CASE, LOOP, REPEAT, WHILE or FOR.
// Its statements run as the server reads them, EXECUTE IMMEDIATE of a string
// among them, so the guard cannot read its effects before the server runs it.
// Measured on MariaDB 11.8.9, each of them deletes from other.t when its body
// does; a label in front of one is a syntax error there. MySQL 8.4.13 refuses
// each of them outside a stored program, so refusing them there costs
// nothing.
func isMySQLCompoundStatement(tokens []lexer.Token) bool {
	switch {
	case len(tokens) == 0:
		return false
	case isKeyword(tokens[0], "BEGIN"):
		return replay.TokenSequenceAt(tokens, 1, "NOT", "ATOMIC")
	default:
		return isKeyword(tokens[0], "IF", "CASE", "LOOP", "REPEAT", "WHILE", "FOR")
	}
}
