package sqlserverreplay

import (
	"fmt"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/internal/devclean/replay"
	"ptah.run/internal/lexer"
)

func validateSQLServerReplayStatementBase(tokens []lexer.Token, scope replay.Scope) error {
	if len(tokens) == 0 {
		return nil
	}
	first := replay.NormalizedIdentifier(tokens[0])
	if first == "USE" {
		return replay.Refuse(platform.SQLServer, first)
	}
	if first == "EXEC" || first == "EXECUTE" {
		if sqlServerRemoteExecute(tokens) && !scope.TrustedSQL {
			return replay.Refuse(platform.SQLServer, "remote EXECUTE")
		}
	}
	if isGlobalSQLServerDDL(tokens) && (!scope.TrustedSQL || !sqlServerUnconfinedServerObject(tokens)) {
		return replay.Refuse(platform.SQLServer, replay.DescribeStatementObject(tokens))
	}
	if replay.UsesTemporaryObject(tokens) || containsTemporaryTarget(tokens) {
		return replay.Refuse(platform.SQLServer, "temporary object")
	}
	if scope.TrustedSQL {
		return nil
	}
	if reader, ok := sqlServerOutsideReader(tokens); ok {
		return &replay.Refusal{Dialect: platform.SQLServer, Operation: reader.name, Reason: reader.reason}
	}
	if name := sqlServerOtherDatabaseName(tokens, scope.Database); name != "" {
		return &replay.Refusal{
			Dialect:   platform.SQLServer,
			Operation: fmt.Sprintf("cross-database reference %q", name),
			Reason:    "a statement that reads or writes another database runs only when the replayed SQL is trusted",
		}
	}
	if first == "DBCC" && sqlServerDatabaseLocalDBCC(tokens) {
		// Every other DBCC acts on the server, and is refused under trust too;
		// see [sqlServerExternalOrServerOperation].
		return &replay.Refusal{
			Dialect:   platform.SQLServer,
			Operation: "DBCC " + replay.NormalizedIdentifier(tokens[1]),
			Reason: "DBCC names the table it checks or reseeds in a string, which can name another database, " +
				"so it runs only when the replayed SQL is trusted",
		}
	}
	if sqlServerKeywordSequenceAt(tokens, 0, "ADD", "SIGNATURE") || sqlServerKeywordSequenceAt(tokens, 0, "ADD", "COUNTER", "SIGNATURE") {
		return replay.Refuse(platform.SQLServer, "ADD SIGNATURE")
	}
	return nil
}

// sqlServerOtherDatabaseName returns the first name in tokens that can name a
// database other than database, the one the dev session selected, or "" when
// every name is local. It reads every name in the statement, whatever its
// place: the target of a write, a table a query reads, a view's or a
// function's body, NEXT VALUE FOR, the table of CREATE STATISTICS, the column
// of WRITETEXT, REFERENCES, the procedure of an EXEC. See [sqlServerLocalName].
//
// It reads a name from every token, so it reads the parts at the end of a
// longer name again; a name ending another is local when the longer one is,
// and the first name it refuses is the whole one.
func sqlServerOtherDatabaseName(tokens []lexer.Token, database string) string {
	for index := range tokens {
		if parts, _ := replay.QualifiedIdentifierAt(tokens, index); !sqlServerLocalName(parts, database) {
			return strings.Join(parts, ".")
		}
	}
	return ""
}

// sqlServerLocalName reports whether a name of parts names nothing outside
// database. A name of one or two parts is an object, or a schema and an
// object. The part three from the end can name a database: db.schema.object,
// or db..object with the schema left out. A part before it can name a server,
// as in server.db.schema.object, or a database, as in the
// db.schema.table.column of WRITETEXT.
//
// So a name is local when every part before the third from the end is empty
// and that part is empty, which means the current database, or spelled
// exactly as database. The same rule refuses a three-part column name such as
// schema.table.column, which a guard that does not resolve names cannot tell
// from db.schema.object. An empty database, from a guard that read no session,
// makes every name with a database part one that reaches outside.
//
// The comparison is exact. The server compares database names under its own
// collation, and no fold the guard could apply matches every collation: under
// Turkish_CI_AS, SQL Server 2025 takes neither [mI] nor [MI] for a database
// named mi, which strings.EqualFold would. An exact spelling names the dev
// database under every collation, so another spelling is refused, and trust
// lifts the refusal.
func sqlServerLocalName(parts []string, database string) bool {
	if len(parts) < 3 {
		return true
	}
	for _, part := range parts[:len(parts)-3] {
		if part != "" {
			return false
		}
	}
	named := parts[len(parts)-3]
	return named == "" || named == database
}

// sqlServerRemoteExecute reports the AT of EXECUTE ... AT linked_server: the
// word AT, unquoted, that is not part of a name or a variable. A string or a
// bracketed name that spells at, such as the new name N'at', is not it.
func sqlServerRemoteExecute(tokens []lexer.Token) bool {
	for index, token := range tokens {
		previous := replay.TokenAt(tokens, index-1)
		if token.MatchIdentifierValue("AT") && !previous.MatchOperatorValue("@") && !previous.MatchOperatorValue(".") {
			return true
		}
	}
	return false
}

// sqlServerOutsideRead is a function that reads outside the dev database:
// a file on the server, or a remote source. A run that trusts its SQL may call
// one; without trust it is refused wherever it stands, as a source, in a
// view's body or as the target of a write, as INSERT OPENQUERY(...) VALUES (1)
// is.
type sqlServerOutsideRead struct {
	// name is the function, compared without case.
	name string
	// reason says what the function reads.
	reason string
	// keyword is true for a rowset function the server reads as a keyword.
	// Only the unquoted word calls it: SQL Server 2025 refuses
	// [OPENROWSET](BULK ...) with Msg 156, so [OPENQUERY] is a table name.
	// The other functions are system objects, which the server finds by any
	// spelling of their name: sys.fn_trace_gettable, [sys].[fn_trace_gettable],
	// fn_trace_gettable alone and ::fn_trace_gettable.
	keyword bool
}

// sqlServerOutsideReads are the functions [sqlServerOutsideRead] describes,
// measured on SQL Server 2025. OPENROWSET(BULK '/etc/hostname', SINGLE_CLOB)
// returned the file. sys.dm_os_enumerate_filesystem listed /etc, and
// sys.dm_os_file_exists found /etc/hostname. sys.fn_trace_gettable returned
// the rows of /var/opt/mssql/log/log.trc. The two audit readers and the
// extended event reader took a path under /var/opt/mssql/log, and
// sys.fn_dump_dblog opened /etc/hostname as a backup device. The procedures
// that read the file system, such as xp_dirtree, xp_fileexist and xp_subdirs,
// are refused as every EXEC is.
var sqlServerOutsideReads = []sqlServerOutsideRead{
	{name: "OPENROWSET", reason: "it reads a file on the server or a remote source", keyword: true},
	{name: "OPENDATASOURCE", reason: "it reads a remote source", keyword: true},
	{name: "OPENQUERY", reason: "it runs a query on a linked server", keyword: true},
	{name: "dm_os_enumerate_filesystem", reason: "it lists the server's file system"},
	{name: "dm_os_file_exists", reason: "it tells whether a path exists on the server"},
	{name: "fn_get_audit_file", reason: "it reads audit files on the server"},
	{name: "fn_get_audit_file_v2", reason: "it reads audit files on the server"},
	{name: "fn_xe_file_target_read_file", reason: "it reads extended event files on the server"},
	{name: "fn_trace_gettable", reason: "it reads trace files on the server"},
	{name: "fn_dump_dblog", reason: "it reads a backup file on the server"},
}

// sqlServerOutsideReader returns the first call in tokens of a function
// [sqlServerOutsideReads] holds: its name followed by an opening parenthesis.
func sqlServerOutsideReader(tokens []lexer.Token) (sqlServerOutsideRead, bool) {
	for index := range tokens {
		next := replay.TokenAt(tokens, index+1)
		if !next.MatchOperatorValue("(") {
			continue
		}
		for _, read := range sqlServerOutsideReads {
			if read.keyword && tokens[index].MatchIdentifierValue(read.name) ||
				!read.keyword && strings.EqualFold(replay.IdentifierValue(tokens[index]), read.name) {
				return read, true
			}
		}
	}
	return sqlServerOutsideRead{}, false
}

func isGlobalSQLServerDDL(tokens []lexer.Token) bool {
	if !replay.IsDDLAction(tokens) {
		return false
	}
	kindIndex := replay.StatementObjectKindIndex(tokens)
	if kindIndex == replay.MutationTargetNotFound {
		return false
	}
	switch replay.NormalizedIdentifier(tokens[kindIndex]) {
	case "DATABASE", "LOGIN", "CREDENTIAL", "ENDPOINT", "AVAILABILITY":
		return true
	case "SERVER":
		return true
	case "SYNONYM":
		return replay.NormalizedIdentifier(tokens[0]) != "DROP"
	case "EXTERNAL":
		return !replay.TokenSequenceAt(tokens, kindIndex, "EXTERNAL", "TABLE")
	default:
		return false
	}
}

// sqlServerUnconfinedServerObject reports a statement [isGlobalSQLServerDDL]
// names whose only reason to be refused is that its object is not the dev
// database's: CREATE or DROP of a database or a login, a server credential in
// any form, and CREATE or ALTER of a synonym, which the realm cleanup removes
// and which can name an object outside the dev database.
//
// ALTER DATABASE and a database's own scoped objects -- DATABASE SCOPED
// CREDENTIAL or CONFIGURATION, DATABASE AUDIT SPECIFICATION, DATABASE
// ENCRYPTION KEY -- are not: they configure the dev database itself, or leave
// in it what the cleanup does not remove. Nor is ALTER LOGIN, which can change
// the credentials the run connects with, nor an ENDPOINT, an AVAILABILITY
// GROUP, a SERVER object or an EXTERNAL object other than a table, which
// configure the server or stay in the dev database after the cleanup.
func sqlServerUnconfinedServerObject(tokens []lexer.Token) bool {
	kindIndex := replay.StatementObjectKindIndex(tokens)
	if kindIndex == replay.MutationTargetNotFound {
		return false
	}
	first := replay.NormalizedIdentifier(tokens[0])
	switch replay.NormalizedIdentifier(tokens[kindIndex]) {
	case "DATABASE":
		switch replay.NormalizedIdentifier(replay.TokenAt(tokens, kindIndex+1)) {
		case "SCOPED", "AUDIT", "ENCRYPTION":
			return false
		}
		return first == "CREATE" || first == "DROP"
	case "LOGIN":
		return first == "CREATE" || first == "DROP"
	case "CREDENTIAL", "SYNONYM":
		return true
	default:
		return false
	}
}

func containsTemporaryTarget(tokens []lexer.Token) bool {
	for _, target := range replay.MutationTargets(tokens) {
		for _, part := range target {
			if strings.HasPrefix(part, "#") {
				return true
			}
		}
	}
	return false
}
