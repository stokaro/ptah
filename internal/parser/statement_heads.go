package parser

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/ptaherr"
)

// UnknownStatementError is the refusal of opening words no engine Ptah reads has. It
// is the one refusal this parser can say is about the text: past the opening words a
// grammar it does not hold looks like a mistake, so `CREATE TABLE b BOGUS` and
// ClickHouse's `CREATE TABLE t ON CLUSTER c` read alike, and a refusal there says only
// that the parser could not read the statement.
type UnknownStatementError struct {
	// Head is the opening words, as the keywords they are read as.
	Head string
	text string
}

func (e *UnknownStatementError) Error() string { return e.text }

// refuseHead is the refusal of a statement whose opening words this reading does not
// hold. head is the words before word, and written the format of the refusal, with
// word as its argument. A word after a statement that ended with no semicolon may be
// a clause the reading stopped short of, so it says nothing about the text.
func (p *Parser) refuseHead(start int, head, word, written string) error {
	refusal := fmt.Sprintf(written, word)
	known := slices.Contains(objectKinds, word)
	if head == "" {
		known = slices.Contains(statementKeywords, word)
	}
	if known || p.midStatement {
		return ptaherr.At(start, errors.New(refusal))
	}
	return ptaherr.At(start, &UnknownStatementError{Head: strings.TrimSpace(head + " " + word), text: refusal})
}

// statementKeywords are the words a statement opens with on any engine Ptah reads. A
// word outside them is no statement: an engine's parser refuses it as a syntax error.
//
// They do not say an engine runs the statement, or that this parser reads it, so a
// word that belongs here and is missing costs a statement the code SQL001 where
// SQL002 is due, and either way the statement is reported.
var statementKeywords = []string{
	"ABORT", "ADD", "ALTER", "ANALYSE", "ANALYZE", "ASSOCIATE", "ATTACH", "AUDIT", "BACKUP", "BEGIN", "BINLOG", "BULK",
	"CACHE", "CALL", "CANCEL", "CHANGE", "CHECK", "CHECKPOINT", "CHECKSUM", "CLONE", "CLOSE", "CLUSTER", "COMMENT",
	"COMMIT", "COPY", "CREATE", "DBCC", "DISASSOCIATE", "EXPERIMENTAL", "GRAPH", "SCRUB", "UNDROP",
	"DEALLOCATE", "DECLARE", "DELETE", "DELIMITER", "DENY", "DESC", "DESCRIBE", "DETACH", "DISCARD", "DISABLE", "DO",
	"DROP", "ENABLE", "END", "EXCHANGE", "EXEC", "EXECUTE", "EXISTS", "EXPLAIN", "EXPORT", "FETCH", "FLASHBACK",
	"FLUSH", "GET", "GO", "GOTO", "GRANT", "HANDLER", "HELP", "IF", "IMPORT", "INSERT", "INSTALL", "KILL", "LISTEN",
	"LOAD", "LOCK", "MERGE", "MOVE", "NOAUDIT", "NOTIFY", "OPEN", "OPTIMIZE", "PAUSE", "PIVOT", "PRAGMA", "PREPARE",
	"PRINT", "PURGE", "RAISE", "RAISERROR", "READ", "READTEXT", "REASSIGN", "RECONFIGURE", "REFRESH", "REINDEX",
	"RELEASE", "RENAME", "REPAIR", "REPLACE", "RESET", "RESIGNAL", "RESTART", "RESTORE", "RESUME", "RETURN", "REVERT",
	"REVOKE", "ROLLBACK", "SAVE", "SAVEPOINT", "SECURITY", "SELECT", "SET", "SETUSER", "SHOW", "SHUTDOWN", "SIGNAL",
	"START", "STOP", "SYSTEM", "TABLE", "THROW", "TRUNCATE", "UNINSTALL", "UNLISTEN", "UNLOCK", "UNPIVOT", "UPDATE",
	"UPDATETEXT", "UPSERT", "USE", "VACUUM", "VALUES", "WAITFOR", "WATCH", "WHILE", "WITH", "WRITETEXT", "XA",
}

// objectKinds are the words a CREATE, an ALTER or a DROP may name on any engine Ptah
// reads, or stand before such a word. [statementKeywords] says why a list like this
// costs little when it is short of a word.
var objectKinds = []string{
	"ACCESS", "ACCOUNT", "AGGREGATE", "ALGORITHM", "ANALYTIC", "APPLICATION", "ASSEMBLY", "ASYMMETRIC", "ATTRIBUTE",
	"AUDIT", "AUTHORIZATION", "AVAILABILITY", "BACKUP", "BITMAP", "BROKER", "CONTEXT", "EDITION", "EDITIONABLE", "GRAPH", "JSON",
	"LARGE", "NONEDITIONABLE", "PFILE", "PLUGGABLE", "PREPARE", "PROPERTY", "REPLICA", "SENSITIVITY", "CAST", "CATALOG", "CERTIFICATE", "CHANGE", "CHANGEFEED", "CLUSTER", "CLUSTERED",
	"COLLATION", "COLUMN", "COLUMNSTORE", "CONSTRAINT", "CONTRACT", "CONVERSION", "CREDENTIAL", "CRYPTOGRAPHIC",
	"DATABASE", "DEFAULT", "DEFINER", "DICTIONARY", "DIMENSION", "DIRECTORY", "DOMAIN", "ENDPOINT", "EVENT", "EXTENSION",
	"EXTERNAL", "FEDERATED", "FOREIGN", "FULLTEXT", "FUNCTION", "GLOBAL", "GROUP", "INDEX", "INDEXTYPE", "INSTANCE",
	"JAVA", "LANGUAGE", "LIBRARY", "LIVE", "LOCAL", "LOCALITY", "LOGFILE", "LOGIN", "MASKING", "MASTER", "MATERIALIZED",
	"MESSAGE", "MODEL", "NAMED", "NONCLUSTERED", "OPERATOR", "OR", "OUTLINE", "OWNED", "PACKAGE", "PARTITION",
	"PLACEMENT", "POLICY", "PRIMARY", "PROC", "PROCEDURE", "PROFILE", "PROTO", "PUBLICATION", "QUERY", "QUEUE", "QUOTA",
	"RECURSIVE", "REMOTE", "REPLICATION", "RESOURCE", "RESTRICTED", "ROLE", "ROUTE", "ROUTINE", "ROW", "RULE", "SCHEMA",
	"SEARCH", "SECURITY", "SELECTIVE", "SEQUENCE", "SERVER", "SERVICE", "SESSION", "SETTINGS", "SIGNATURE", "SPATIAL",
	"SPFILE", "SQL", "STATISTICS", "STREAM", "SUBSCRIPTION", "SYMMETRIC", "SYNONYM", "SYSTEM", "TABLE", "TABLEGROUP",
	"TABLESPACE", "TEMP", "TEMPORARY", "TEXT", "TOPIC", "TRANSFORM", "TRIGGER", "TYPE", "UNDO", "UNIQUE", "UNLOGGED",
	"USER", "VECTOR", "VIEW", "VIRTUAL", "WINDOW", "WORKLOAD", "XML",
}
