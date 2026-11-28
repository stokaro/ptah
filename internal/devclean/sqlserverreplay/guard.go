package sqlserverreplay

import (
	"fmt"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/internal/devclean/replay"
	"ptah.run/internal/lexer"
)

// validateReplayStatement rejects SQL Server mutations whose
// effects cannot be removed by the database-realm cleanup.
//
// Under [replay.Scope.TrustedSQL] it lifts the refusals whose only
// reason is that the effect reaches past the dev database: EXEC and EXECUTE,
// which run SQL the guard cannot read, as a DO block does on PostgreSQL, and
// EXECUTE AS among them; SETUSER in every form, which changes the user the
// session runs as, or goes back to the login when it names none; a
// function, procedure or DML trigger, whose body is opaque and which the realm
// cleanup removes; a database, a login or a server credential created or
// dropped, and a synonym (see [sqlServerUnconfinedServerObject]); a name of
// another database the dev session reaches, a read as well as a write (see
// [guard.otherDatabaseName]); DBCC CHECKIDENT, CHECKTABLE and
// CHECKCONSTRAINTS on a table of another database, or on one they do not name
// as a constant (see [guard.validateDBCCTable]); BACKUP, RESTORE, BULK
// INSERT, a remote EXECUTE and the functions that read a file on the server
// or a remote source (see [sqlServerOutsideReads]), which move data across
// the server's edge; and ADD SIGNATURE or ADD COUNTER SIGNATURE, which gives a
// module the permissions of a certificate's user or login.
//
// It keeps every refusal that protects the run: USE and a temporary object,
// which change or live in the replay's session; GRANT, DENY and REVOKE, and a
// database principal, whose state stays in the dev database after the cleanup;
// a DDL trigger on the database and anything ON SERVER, which fire on the run's
// own statements; every other DBCC, KILL, RECONFIGURE and SHUTDOWN,
// which act on the server the run uses; an assembly, which loads code into the
// server; and the objects the realm cleanup does not remove: an external table
// or data source, a ledger table, a partition function or scheme, full-text,
// key and certificate objects, and Service Broker and resource governor
// objects.
func (g guard) validateReplayStatement(tokens []lexer.Token, scope replay.Scope) error {
	if err := g.validateReplayStatementBase(tokens, scope); err != nil {
		return err
	}
	if len(tokens) == 0 {
		return nil
	}
	tokens = sqlServerExecutableTokens(tokens)

	first := replay.NormalizedIdentifier(tokens[0])
	if first == "EXEC" || first == "EXECUTE" {
		if scope.TrustedSQL {
			return nil
		}
		return validateSQLServerExecute(tokens)
	}
	if first == "SETUSER" && !scope.TrustedSQL {
		return &replay.Refusal{
			Dialect:   platform.SQLServer,
			Operation: first,
			Reason:    "it changes the user the session runs as, and with it the databases the session reaches; it runs only when the replayed SQL is trusted",
		}
	}
	if operation := sqlServerExternalOrServerOperation(tokens, scope); operation != "" {
		return replay.Refuse(platform.SQLServer, operation)
	}
	if err := validateSQLServerStatementScope(tokens, first); err != nil {
		return err
	}
	if err := validateSQLServerStatementTargets(tokens, first, scope); err != nil {
		return err
	}
	return validateSQLServerStoredObjects(tokens, first, scope)
}

// validateSQLServerStatementScope refuses a permission change and a statement
// whose scope is the server or a DDL trigger on the database. Trust lifts none
// of them; see [guard.validateReplayStatement].
func validateSQLServerStatementScope(tokens []lexer.Token, first string) error {
	if first == "GRANT" || first == "DENY" || first == "REVOKE" {
		return replay.Refuse(platform.SQLServer, "permission mutation")
	}
	if sqlServerHasGlobalScope(tokens) {
		return replay.Refuse(platform.SQLServer, "ON SERVER or ON ALL SERVER")
	}
	if sqlServerHasDatabaseTriggerScope(tokens) {
		return replay.Refuse(
			platform.SQLServer,
			first+" DATABASE DDL TRIGGER",
		)
	}
	return nil
}

// validateSQLServerStatementTargets refuses an executable stored body and a
// temporary SELECT INTO target. Under [replay.Scope.TrustedSQL] only the
// temporary target stays refused, since it lives in the replay's session. A
// target in another database is refused, without trust, as every name of
// another database is; see [guard.otherDatabaseName].
func validateSQLServerStatementTargets(tokens []lexer.Token, first string, scope replay.Scope) error {
	if sqlServerDefinesExecutableBody(tokens) && !scope.TrustedSQL {
		return replay.Refuse(platform.SQLServer, first+" executable stored body")
	}
	if sqlServerSelectIntoTemporaryObject(tokens) {
		return replay.Refuse(platform.SQLServer, "temporary SELECT INTO target")
	}
	return nil
}

// validateSQLServerStoredObjects refuses the objects the realm cleanup does not
// remove. Trust lifts none of them. A database role is one only where the
// cleanup does not drop the roles the run creates; see
// [sqlServerConfinedDatabaseRole].
func validateSQLServerStoredObjects(tokens []lexer.Token, first string, scope replay.Scope) error {
	if sqlServerExternalTableWritesData(tokens) {
		return replay.Refuse(
			platform.SQLServer,
			"CREATE EXTERNAL TABLE AS SELECT",
		)
	}
	if sqlServerRejectsExternalTableDDL(tokens) {
		return replay.Refuse(platform.SQLServer, first+" EXTERNAL TABLE")
	}
	if sqlServerEnablesLedger(tokens) {
		return replay.Refuse(platform.SQLServer, "LEDGER table")
	}
	if confined, err := sqlServerDatabaseRole(tokens, scope.DatabaseRoles); confined || err != nil {
		return err
	}
	if operation := sqlServerUnsupportedRealmOperation(tokens); operation != "" {
		return replay.Refuse(platform.SQLServer, operation)
	}
	return nil
}

// sqlServerDatabaseRole decides a CREATE ROLE or DROP ROLE on a dev database
// whose every cleanup drops the database roles the run creates. A database
// role lives in the database that holds it, so such a cleanup confines it to
// the dev database: CREATE ROLE is confined, and so is DROP ROLE of a role the
// claim did not find. The drop of one it found is refused, since the role
// would stay gone after the run. confined is false for every other statement,
// which the caller judges as before: ALTER ROLE stays refused, because it can
// add a member the claim found to a fixed role and that membership outlives
// the run, and so do an application role, which carries a password, and a
// user, which can map a server login.
func sqlServerDatabaseRole(tokens []lexer.Token, roles replay.DatabaseRoleScope) (confined bool, err error) {
	if !roles.Dropped || !replay.IsDDLAction(tokens) || !sqlServerKeywordSequenceAt(tokens, 1, "ROLE") {
		return false, nil
	}
	switch replay.NormalizedIdentifier(tokens[0]) {
	case "CREATE":
		return true, nil
	case "DROP":
		index := 2
		if sqlServerKeywordSequenceAt(tokens, index, "IF", "EXISTS") {
			index += 2
		}
		if index >= len(tokens) {
			return false, nil
		}
		name := replay.NormalizedIdentifier(tokens[index])
		if name == "" {
			return false, nil
		}
		for _, kept := range roles.Kept {
			if strings.EqualFold(kept, name) {
				return false, replay.Refuse(platform.SQLServer, fmt.Sprintf("DROP DATABASE PRINCIPAL of the dev database's own role %q", kept))
			}
		}
		return true, nil
	}
	return false, nil
}

func sqlServerRejectsExternalTableDDL(tokens []lexer.Token) bool {
	return replay.NormalizedIdentifier(tokens[0]) != "DROP" &&
		sqlServerDDLStartsWith(tokens, "EXTERNAL", "TABLE")
}

// sqlServerExternalOrServerOperation names a statement that reaches external
// storage or acts on the server. Under [replay.Scope.TrustedSQL] the storage
// operations are not named: they move data across the server's edge and change
// nothing in it the cleanup does not remove. The server operations are named
// either way.
func sqlServerExternalOrServerOperation(tokens []lexer.Token, scope replay.Scope) string {
	first := replay.NormalizedIdentifier(tokens[0])
	switch first {
	case "BACKUP", "RESTORE":
		if !scope.TrustedSQL {
			return first + " external storage operation"
		}
	case "BULK":
		if sqlServerKeywordSequenceAt(tokens, 1, "INSERT") && !scope.TrustedSQL {
			return "BULK INSERT external data operation"
		}
	case "DBCC":
		if !sqlServerDatabaseLocalDBCC(tokens) {
			return first + " server operation"
		}
	case "KILL", "RECONFIGURE", "SHUTDOWN":
		return first + " server operation"
	}
	return ""
}

func sqlServerDefinesExecutableBody(tokens []lexer.Token) bool {
	if replay.NormalizedIdentifier(tokens[0]) == "DROP" &&
		sqlServerDDLStartsWith(tokens, "FUNCTION") {
		return false
	}
	return sqlServerDDLStartsWith(tokens, "FUNCTION") ||
		sqlServerDDLStartsWith(tokens, "PROCEDURE") ||
		sqlServerDDLStartsWith(tokens, "PROC") ||
		sqlServerDDLStartsWith(tokens, "TRIGGER")
}

func sqlServerDatabaseLocalDBCC(tokens []lexer.Token) bool {
	return sqlServerKeywordSequenceAt(tokens, 1, "CHECKIDENT") ||
		sqlServerKeywordSequenceAt(tokens, 1, "CHECKTABLE") ||
		sqlServerKeywordSequenceAt(tokens, 1, "CHECKCONSTRAINTS")
}

func sqlServerHasGlobalScope(tokens []lexer.Token) bool {
	if !sqlServerTriggerStatement(tokens) &&
		!sqlServerDDLStartsWith(tokens, "EVENT", "SESSION") &&
		!sqlServerDDLStartsWith(tokens, "EVENT", "NOTIFICATION") {
		return false
	}
	return sqlServerContainsKeywordSequence(tokens, "ON", "SERVER") ||
		sqlServerContainsKeywordSequence(tokens, "ON", "ALL", "SERVER")
}

func sqlServerHasDatabaseTriggerScope(tokens []lexer.Token) bool {
	return sqlServerTriggerStatement(tokens) &&
		sqlServerContainsKeywordSequence(tokens, "ON", "DATABASE")
}

func sqlServerTriggerStatement(tokens []lexer.Token) bool {
	if sqlServerDDLStartsWith(tokens, "TRIGGER") {
		return true
	}
	if len(tokens) < 2 {
		return false
	}
	first := replay.NormalizedIdentifier(tokens[0])
	return (first == "ENABLE" || first == "DISABLE") &&
		sqlServerKeywordSequenceAt(tokens, 1, "TRIGGER")
}

// validateSQLServerExecute refuses an EXEC, which can run anything, unless it is a
// call the cleanup takes back.
func validateSQLServerExecute(tokens []lexer.Token) error {
	if sqlServerConfinedExtendedProperty(tokens) {
		return nil
	}
	return replay.Refuse(platform.SQLServer, "EXEC/EXECUTE sublanguage")
}

// sqlServerConfinedExtendedProperty reports a call that sets, changes or drops an
// extended property the realm cleanup takes back: one on an object inside a schema,
// which goes with the object, or a schema's MS_Description, which goes with the
// schema, or with the cleanup where the schema is the one it keeps. A property on
// the database, or another property on a schema, outlives the cleanup.
func sqlServerConfinedExtendedProperty(tokens []lexer.Token) bool {
	index := 1
	dot := replay.TokenAt(tokens, index+1)
	if replay.NormalizedIdentifier(replay.TokenAt(tokens, index)) == "SYS" && dot.MatchOperatorValue(".") {
		index += 2
	}
	switch replay.NormalizedIdentifier(replay.TokenAt(tokens, index)) {
	case "SP_ADDEXTENDEDPROPERTY", "SP_UPDATEEXTENDEDPROPERTY", "SP_DROPEXTENDEDPROPERTY":
	default:
		return false
	}
	arguments := make(map[string]string, 8)
	for index++; index < len(tokens); index++ {
		if !tokens[index].MatchOperatorValue("@") {
			continue
		}
		name := replay.NormalizedIdentifier(replay.TokenAt(tokens, index+1))
		value := replay.TokenAt(tokens, index+3)
		if value.MatchIdentifierValue("N") {
			value = replay.TokenAt(tokens, index+4)
		}
		arguments[name] = replay.NormalizedIdentifier(value)
	}
	if arguments["LEVEL0TYPE"] != "SCHEMA" {
		return false
	}
	return arguments["LEVEL1TYPE"] != "" || arguments["NAME"] == "MS_DESCRIPTION"
}

func sqlServerExecutableTokens(tokens []lexer.Token) []lexer.Token {
	if len(tokens) == 0 || !tokens[0].MatchIdentifierValue("WITH") {
		return tokens
	}
	depth := 0
	closedDefinition := false
	for index := 1; index < len(tokens); index++ {
		switch {
		case tokens[index].MatchOperatorValue("("):
			depth++
		case tokens[index].MatchOperatorValue(")"):
			depth--
			closedDefinition = depth == 0
		case depth == 0 && closedDefinition &&
			sqlServerDMLStatementAt(tokens, index):
			return tokens[index:]
		}
	}
	return tokens
}

func sqlServerDMLStatementAt(tokens []lexer.Token, index int) bool {
	return sqlServerKeywordSequenceAt(tokens, index, "SELECT") ||
		sqlServerKeywordSequenceAt(tokens, index, "INSERT") ||
		sqlServerKeywordSequenceAt(tokens, index, "UPDATE") ||
		sqlServerKeywordSequenceAt(tokens, index, "DELETE") ||
		sqlServerKeywordSequenceAt(tokens, index, "MERGE")
}

func sqlServerSelectIntoTemporaryObject(tokens []lexer.Token) bool {
	targetIndex := sqlServerSelectIntoTargetIndex(tokens)
	if targetIndex < 0 || targetIndex >= len(tokens) {
		return false
	}
	target := replay.NormalizedIdentifier(tokens[targetIndex])
	return len(target) > 0 && target[0] == '#'
}

func sqlServerSelectIntoTargetIndex(tokens []lexer.Token) int {
	if len(tokens) == 0 {
		return replay.MutationTargetNotFound
	}
	first := replay.NormalizedIdentifier(tokens[0])
	if first != "SELECT" && first != "WITH" {
		return replay.MutationTargetNotFound
	}
	intoIndex := sqlServerFindKeyword(tokens, "INTO", 1)
	if intoIndex == replay.MutationTargetNotFound {
		return replay.MutationTargetNotFound
	}
	return intoIndex + 1
}

func sqlServerFindKeyword(tokens []lexer.Token, value string, start int) int {
	for index := max(start, 0); index < len(tokens); index++ {
		if tokens[index].MatchIdentifierValue(value) {
			return index
		}
	}
	return replay.MutationTargetNotFound
}

func sqlServerContainsKeywordSequence(tokens []lexer.Token, values ...string) bool {
	for index := range len(tokens) {
		if sqlServerKeywordSequenceAt(tokens, index, values...) {
			return true
		}
	}
	return false
}

func sqlServerKeywordSequenceAt(tokens []lexer.Token, index int, values ...string) bool {
	if index < 0 || index+len(values) > len(tokens) {
		return false
	}
	for offset, value := range values {
		if !tokens[index+offset].MatchIdentifierValue(value) {
			return false
		}
	}
	return true
}

func sqlServerDDLStartsWith(tokens []lexer.Token, values ...string) bool {
	if !replay.IsDDLAction(tokens) {
		return false
	}
	index := 1
	if sqlServerKeywordSequenceAt(tokens, index, "OR", "ALTER") {
		index += 2
	}
	return sqlServerKeywordSequenceAt(tokens, index, values...)
}

func sqlServerExternalTableWritesData(tokens []lexer.Token) bool {
	if !tokens[0].MatchIdentifierValue("CREATE") ||
		!sqlServerDDLStartsWith(tokens, "EXTERNAL", "TABLE") {
		return false
	}
	depth := 0
	for index := 3; index < len(tokens); index++ {
		switch {
		case tokens[index].MatchOperatorValue("("):
			depth++
		case tokens[index].MatchOperatorValue(")"):
			depth--
		case depth == 0 && tokens[index].MatchIdentifierValue("AS"):
			return true
		}
	}
	return false
}

func sqlServerEnablesLedger(tokens []lexer.Token) bool {
	if !sqlServerDDLStartsWith(tokens, "TABLE") {
		return false
	}
	for index := 0; index+2 < len(tokens); index++ {
		if tokens[index].MatchIdentifierValue("LEDGER") &&
			tokens[index+1].MatchOperatorValue("=") &&
			tokens[index+2].MatchIdentifierValue("ON") {
			return true
		}
	}
	return false
}

func sqlServerUnsupportedRealmOperation(tokens []lexer.Token) string {
	if !replay.IsDDLAction(tokens) {
		return ""
	}
	first := replay.NormalizedIdentifier(tokens[0])
	if sqlServerDDLStartsWith(tokens, "USER") ||
		sqlServerDDLStartsWith(tokens, "ROLE") ||
		sqlServerDDLStartsWith(tokens, "APPLICATION", "ROLE") {
		return first + " DATABASE PRINCIPAL"
	}

	patterns := [...]struct {
		sequence  []string
		operation string
	}{
		{sequence: []string{"AUTHORIZATION"}, operation: first + " AUTHORIZATION"},
		{sequence: []string{"ASSEMBLY"}, operation: first + " ASSEMBLY"},
		{sequence: []string{"PARTITION", "FUNCTION"}, operation: first + " PARTITION FUNCTION"},
		{sequence: []string{"PARTITION", "SCHEME"}, operation: first + " PARTITION SCHEME"},
		{sequence: []string{"FULLTEXT", "CATALOG"}, operation: first + " FULLTEXT CATALOG"},
		{sequence: []string{"FULLTEXT", "STOPLIST"}, operation: first + " FULLTEXT STOPLIST"},
		{sequence: []string{"SEARCH", "PROPERTY", "LIST"}, operation: first + " SEARCH PROPERTY LIST"},
		{sequence: []string{"CERTIFICATE"}, operation: first + " CERTIFICATE"},
		{sequence: []string{"COLUMN", "MASTER", "KEY"}, operation: first + " COLUMN MASTER KEY"},
		{sequence: []string{"COLUMN", "ENCRYPTION", "KEY"}, operation: first + " COLUMN ENCRYPTION KEY"},
		{sequence: []string{"MASTER", "KEY"}, operation: first + " MASTER KEY"},
		{sequence: []string{"SYMMETRIC", "KEY"}, operation: first + " SYMMETRIC KEY"},
		{sequence: []string{"ASYMMETRIC", "KEY"}, operation: first + " ASYMMETRIC KEY"},
		{sequence: []string{"EVENT", "NOTIFICATION"}, operation: first + " EVENT NOTIFICATION"},
		{sequence: []string{"EVENT", "SESSION"}, operation: first + " EVENT SESSION"},
		{sequence: []string{"MESSAGE", "TYPE"}, operation: first + " MESSAGE TYPE"},
		{sequence: []string{"REMOTE", "SERVICE", "BINDING"}, operation: first + " REMOTE SERVICE BINDING"},
		{sequence: []string{"BROKER", "PRIORITY"}, operation: first + " BROKER PRIORITY"},
		{sequence: []string{"CONTRACT"}, operation: first + " CONTRACT"},
		{sequence: []string{"QUEUE"}, operation: first + " QUEUE"},
		{sequence: []string{"SERVICE"}, operation: first + " SERVICE"},
		{sequence: []string{"ROUTE"}, operation: first + " ROUTE"},
		{sequence: []string{"RESOURCE", "POOL"}, operation: first + " RESOURCE POOL"},
		{sequence: []string{"RESOURCE", "GOVERNOR"}, operation: first + " RESOURCE GOVERNOR"},
		{sequence: []string{"WORKLOAD", "GROUP"}, operation: first + " WORKLOAD GROUP"},
		{sequence: []string{"CRYPTOGRAPHIC", "PROVIDER"}, operation: first + " CRYPTOGRAPHIC PROVIDER"},
	}
	for _, pattern := range patterns {
		if sqlServerDDLStartsWith(tokens, pattern.sequence...) {
			return pattern.operation
		}
	}
	return ""
}
