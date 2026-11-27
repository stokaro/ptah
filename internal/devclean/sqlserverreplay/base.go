package sqlserverreplay

import (
	"fmt"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/internal/devclean/replay"
	"ptah.run/internal/lexer"
	"ptah.run/internal/sqlserverexec"
)

func (g guard) validateReplayStatementBase(tokens []lexer.Token, scope replay.Scope) error {
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
	name, err := g.otherDatabaseName(tokens, scope.Database)
	if err != nil {
		return err
	}
	if name != "" {
		return &replay.Refusal{
			Dialect:   platform.SQLServer,
			Operation: fmt.Sprintf("cross-database reference %q", name),
			Reason:    "a statement that reads or writes another database runs only when the replayed SQL is trusted",
		}
	}
	if first == "DBCC" && sqlServerDatabaseLocalDBCC(tokens) {
		// Every other DBCC acts on the server, and is refused under trust too;
		// see [sqlServerExternalOrServerOperation].
		return g.validateDBCCTable(tokens, scope.Database)
	}
	if sqlServerKeywordSequenceAt(tokens, 0, "ADD", "SIGNATURE") || sqlServerKeywordSequenceAt(tokens, 0, "ADD", "COUNTER", "SIGNATURE") {
		return replay.Refuse(platform.SQLServer, "ADD SIGNATURE")
	}
	return nil
}

// otherDatabaseName returns the first name in tokens that can name a
// database other than database, the one the dev session selected, or "" when
// every name is local. It reads each name the statement writes as
// identifiers: the target of a write, a table a query reads, a view's or a
// function's body, NEXT VALUE FOR, the table of CREATE STATISTICS, the column
// of WRITETEXT, REFERENCES, the procedure of an EXEC. See [guard.localName].
// A name the statement holds in a string, such as OBJECT_ID(N'db.dbo.t'), is
// not a name here; the DBCC commands that read one are judged by
// [guard.validateDBCCTable].
//
// It reads a name from every token, so it reads the parts at the end of a
// longer name again; a name ending another is local when the longer one is,
// and the first name it refuses is the whole one.
func (g guard) otherDatabaseName(tokens []lexer.Token, database string) (string, error) {
	for index := range tokens {
		parts, _ := replay.QualifiedIdentifierAt(tokens, index)
		local, err := g.localName(parts, database)
		if err != nil {
			return "", err
		}
		if !local {
			return strings.Join(parts, "."), nil
		}
	}
	return "", nil
}

// localName reports whether a name of parts names nothing outside
// database. A name of one or two parts is an object, or a schema and an
// object. The part three from the end can name a database: db.schema.object,
// or db..object with the schema left out. A part before it names a server,
// as in server.db.schema.object, or a database, as in the
// db.schema.table.column of WRITETEXT, and such a name is never local.
//
// A database part that is empty, which means the current database, or
// spelled exactly as database is local. Any other database part is asked of
// the dev session (see [reachedDatabases]): a database the session reaches is
// another database, and the name is refused. A part that names no database
// the session reaches is not a database to the server either, so the name is
// a column or a method on one, as in alias.column.Method(), table.column.Method()
// and schema.table.column, and it is local. So master.sys.databases and
// tempdb.sys.objects are refused, since the guest user, which cannot be
// disabled there, lets every login reach both. The guard
// does not bind an alias or read a parenthesis instead: SELECT o.dbo.f() FROM
// dbo.x AS o runs database o's function, and INSERT INTO o.dbo.value (id) ...
// FROM dbo.x AS o writes into database o.
//
// A guard that read no session asks nothing, and takes every database part
// that is not empty for another database.
//
// The comparison with database is exact. The server compares database names
// under its own collation, and no fold the guard could apply matches every
// collation: under Turkish_CI_AS, SQL Server 2025 takes neither [mI] nor
// [MI] for a database named mi, which strings.EqualFold would. Another
// spelling of the dev database is asked, the session reaches it, and the name
// is refused; trust lifts the refusal.
func (g guard) localName(parts []string, database string) (bool, error) {
	if len(parts) < 3 {
		return true, nil
	}
	for _, part := range parts[:len(parts)-3] {
		if part != "" {
			return false, nil
		}
	}
	named := parts[len(parts)-3]
	if named == "" || named == database {
		return true, nil
	}
	if g.databases == nil {
		return false, nil
	}
	reached, err := g.databases.reaches(named)
	return !reached, err
}

// validateDBCCTable refuses DBCC CHECKIDENT, CHECKTABLE or CHECKCONSTRAINTS
// whose table the guard cannot read as a name of the dev database. The server
// reads the first argument as a name, whatever its quoting: 'o.dbo.t',
// N'o.dbo.t', "o.dbo.t" and [o.dbo.t] each name the table t in database o
// (measured on SQL Server 2025), so the argument is split with
// [sqlserverexec.SplitName] and judged by [guard.localName]. A variable, a
// number or any other argument names a table the guard cannot see, and is
// refused. CHECKCONSTRAINTS with no argument checks the current database,
// and its argument names only an object there (Msg 2506 for a name of
// another database), so it runs.
func (g guard) validateDBCCTable(tokens []lexer.Token, database string) error {
	command := replay.NormalizedIdentifier(tokens[1])
	if command == "CHECKCONSTRAINTS" && (len(tokens) == 2 || tokens[2].MatchIdentifierValue("WITH")) {
		return nil
	}
	parts, ok := dbccTableName(tokens)
	if !ok {
		return &replay.Refusal{
			Dialect:   platform.SQLServer,
			Operation: "DBCC " + command,
			Reason: "its table is not a constant name, so the guard cannot tell which database it names; " +
				"it runs only when the replayed SQL is trusted",
		}
	}
	local, err := g.localName(parts, database)
	if err != nil || local {
		return err
	}
	return &replay.Refusal{
		Dialect:   platform.SQLServer,
		Operation: fmt.Sprintf("DBCC %s of %q", command, strings.Join(parts, ".")),
		Reason:    "the table it names is in another database, which a statement reaches only when the replayed SQL is trusted",
	}
}

// dbccTableName reads the first argument of a DBCC that names a table, as in
// DBCC CHECKIDENT ('dbo.items', RESEED, 1): a string, with or without N, or a
// name, followed by a comma or the closing parenthesis. It reports false for
// any other argument.
func dbccTableName(tokens []lexer.Token) ([]string, bool) {
	open, argument, literal := replay.TokenAt(tokens, 2), replay.TokenAt(tokens, 3), replay.TokenAt(tokens, 4)
	if !open.MatchOperatorValue("(") {
		return nil, false
	}
	next := 4
	if argument.MatchIdentifierValue("N") && argument.End == literal.Start && strings.HasPrefix(literal.Value, "'") {
		argument = literal
		next = 5
	}
	end := replay.TokenAt(tokens, next)
	if !end.MatchOperatorValue(",") && !end.MatchOperatorValue(")") {
		return nil, false
	}
	text, ok := dbccNameText(argument)
	if !ok {
		return nil, false
	}
	return sqlserverexec.SplitName(text)
}

// dbccNameText returns the text the server reads a name from in a DBCC
// argument: a string's value, a bracketed name without its brackets, and an
// unquoted name as it is, which [sqlserverexec.SplitName] then reads. A
// variable, a number and an operator have none.
func dbccNameText(argument lexer.Token) (string, bool) {
	switch {
	case argument.Type == lexer.TokenString:
		return lexer.StringValue(argument.Value, lexerOptions())
	case argument.Type != lexer.TokenIdentifier:
		return "", false
	case strings.HasPrefix(argument.Value, "["):
		value := argument.Value[1:]
		quoted, closed := strings.CutSuffix(value, "]")
		return strings.ReplaceAll(quoted, "]]", "]"), closed
	default:
		return argument.Value, true
	}
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
