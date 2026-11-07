package builtindialects

import "ptah.run/core/platform/descriptor"

// The statements each engine's server takes, as the shared parser reads them, beside
// the DDL it reads on every engine. Each form and each DROP clause is what the release
// CI runs accepted, on a schema holding the objects it names, and what is left out of
// one is what that server answered with a syntax error, or refused as unimplemented
// whichever objects exist. Measured on PostgreSQL 18, YugabyteDB 2026.1, CockroachDB
// 26.3, MySQL 26.10, MariaDB 12.3, ClickHouse 26.10, SQL Server 2025 and SQLite 3.51.
// Oracle is not measured and states none.

// The forms more than one engine writes alike.
const (
	postgresTruncate = "TRUNCATE [TABLE] [ONLY] <names> [RESTART IDENTITY|CONTINUE IDENTITY] [CASCADE|RESTRICT]"
	postgresLock     = "LOCK [TABLE] [ONLY] <names> " +
		"[IN {ACCESS {SHARE|EXCLUSIVE}|ROW {SHARE|EXCLUSIVE}|SHARE [UPDATE EXCLUSIVE|ROW EXCLUSIVE]|EXCLUSIVE} MODE] [NOWAIT]"
	savepointForm    = "SAVEPOINT <name>"
	releaseForm      = "RELEASE [SAVEPOINT] <name>"
	mysqlReleaseForm = "RELEASE SAVEPOINT <name>"
)

// postgresStatementForms is PostgreSQL's.
func postgresStatementForms() descriptor.StatementForms {
	return descriptor.StatementForms{
		Drops: []descriptor.Drop{
			{Kind: descriptor.DropKindView, IfExists: true, List: true, Behavior: true},
			{Kind: descriptor.DropKindMaterializedView, IfExists: true, List: true, Behavior: true},
			{Kind: descriptor.DropKindTrigger, IfExists: true, OnTable: true, Behavior: true},
			{Kind: descriptor.DropKindFunction, IfExists: true, List: true, Behavior: true, Arguments: true},
			{Kind: descriptor.DropKindProcedure, IfExists: true, List: true, Behavior: true, Arguments: true},
			{Kind: descriptor.DropKindSequence, IfExists: true, List: true, Behavior: true},
			{Kind: descriptor.DropKindType, IfExists: true, List: true, Behavior: true},
		},
		Operations: []string{postgresTruncate, savepointForm, releaseForm, postgresLock},
	}
}

// yugabyteStatementForms is PostgreSQL's, without a list of views, materialized views
// or sequences: YugabyteDB 2026.1 answers `DROP multiple objects not supported yet`.
func yugabyteStatementForms() descriptor.StatementForms {
	return descriptor.StatementForms{
		Drops: []descriptor.Drop{
			{Kind: descriptor.DropKindView, IfExists: true, Behavior: true},
			{Kind: descriptor.DropKindMaterializedView, IfExists: true, Behavior: true},
			{Kind: descriptor.DropKindTrigger, IfExists: true, OnTable: true, Behavior: true},
			{Kind: descriptor.DropKindFunction, IfExists: true, List: true, Behavior: true, Arguments: true},
			{Kind: descriptor.DropKindProcedure, IfExists: true, List: true, Behavior: true, Arguments: true},
			{Kind: descriptor.DropKindSequence, IfExists: true, Behavior: true},
			{Kind: descriptor.DropKindType, IfExists: true, List: true, Behavior: true},
		},
		Operations: []string{postgresTruncate, savepointForm, releaseForm, postgresLock},
	}
}

// cockroachStatementForms is PostgreSQL's less what CockroachDB 26.3 refuses:
// CASCADE on a routine or a type answers `unimplemented`, TRUNCATE takes no IDENTITY
// clause, and LOCK TABLE answers `is not supported`.
func cockroachStatementForms() descriptor.StatementForms {
	return descriptor.StatementForms{
		Drops: []descriptor.Drop{
			{Kind: descriptor.DropKindView, IfExists: true, List: true, Behavior: true},
			{Kind: descriptor.DropKindMaterializedView, IfExists: true, List: true, Behavior: true},
			{Kind: descriptor.DropKindTrigger, IfExists: true, OnTable: true, Behavior: true},
			{Kind: descriptor.DropKindFunction, IfExists: true, List: true, Arguments: true},
			{Kind: descriptor.DropKindProcedure, IfExists: true, List: true, Arguments: true},
			{Kind: descriptor.DropKindSequence, IfExists: true, List: true, Behavior: true},
			{Kind: descriptor.DropKindType, IfExists: true, List: true},
		},
		Operations:       []string{"TRUNCATE [TABLE] [ONLY] <names> [CASCADE|RESTRICT]", savepointForm, releaseForm},
		UnreadStatements: []string{"UPSERT"},
	}
}

// mysqlOperations are what MySQL and MariaDB share, with the form of LOCK that tells
// them apart. RELEASE needs its SAVEPOINT there.
func mysqlOperations(lock string) []string {
	return []string{
		"TRUNCATE [TABLE] <name>",
		savepointForm,
		mysqlReleaseForm,
		lock,
		"UNLOCK {TABLE|TABLES}",
		"OPTIMIZE [NO_WRITE_TO_BINLOG|LOCAL] {TABLE|TABLES} <names>",
	}
}

// mysqlStatementForms is MySQL's. CASCADE and RESTRICT on a view are parsed and
// ignored there, and a routine takes neither an argument list nor a second name.
func mysqlStatementForms() descriptor.StatementForms {
	return descriptor.StatementForms{
		RenameTable: true,
		Drops: []descriptor.Drop{
			{Kind: descriptor.DropKindView, IfExists: true, List: true, Behavior: true},
			{Kind: descriptor.DropKindTrigger, IfExists: true},
			{Kind: descriptor.DropKindFunction, IfExists: true},
			{Kind: descriptor.DropKindProcedure, IfExists: true},
		},
		Operations:       mysqlOperations("LOCK {TABLE|TABLES} (<name> [AS <name>] {READ [LOCAL]|WRITE}),..."),
		UnreadStatements: []string{"REPLACE"},
	}
}

// mariaDBStatementForms is MySQL's with a sequence, and with the LOW_PRIORITY MySQL
// 26.10 answers with a syntax error.
func mariaDBStatementForms() descriptor.StatementForms {
	grammar := mysqlStatementForms()
	grammar.Drops = append(grammar.Drops, descriptor.Drop{Kind: descriptor.DropKindSequence, IfExists: true, List: true})
	grammar.Operations = mysqlOperations("LOCK {TABLE|TABLES} (<name> [AS <name>] {READ [LOCAL]|WRITE|LOW_PRIORITY WRITE}),...")
	return grammar
}

// clickHouseStatementForms is ClickHouse's. It has no DROP MATERIALIZED VIEW: DROP
// VIEW removes one.
func clickHouseStatementForms() descriptor.StatementForms {
	return descriptor.StatementForms{
		RenameTable: true,
		Drops: []descriptor.Drop{
			{Kind: descriptor.DropKindTable, Sync: true},
			{Kind: descriptor.DropKindView, IfExists: true, List: true, Sync: true},
			{Kind: descriptor.DropKindFunction, IfExists: true},
		},
		Operations: []string{
			"TRUNCATE [TABLE] [IF EXISTS] <name> [SYNC]",
			"OPTIMIZE TABLE <name> [FINAL] [DEDUPLICATE]",
		},
	}
}

// sqlServerStatementForms is T-SQL's. TRUNCATE needs its TABLE there, a savepoint
// has no RELEASE, and no DROP takes CASCADE.
func sqlServerStatementForms() descriptor.StatementForms {
	return descriptor.StatementForms{
		Drops: []descriptor.Drop{
			{Kind: descriptor.DropKindView, IfExists: true, List: true},
			{Kind: descriptor.DropKindTrigger, IfExists: true, List: true},
			{Kind: descriptor.DropKindFunction, IfExists: true, List: true},
			{Kind: descriptor.DropKindProcedure, IfExists: true, List: true},
			{Kind: descriptor.DropKindProc, IfExists: true, List: true},
			{Kind: descriptor.DropKindSequence, IfExists: true, List: true},
			{Kind: descriptor.DropKindType, IfExists: true},
		},
		Operations: []string{"TRUNCATE TABLE <name>", "SAVE {TRANSACTION|TRAN} <name>"},
	}
}

// sqliteStatementForms is SQLite's, whose DROP names one object.
func sqliteStatementForms() descriptor.StatementForms {
	return descriptor.StatementForms{
		Drops: []descriptor.Drop{
			{Kind: descriptor.DropKindView, IfExists: true},
			{Kind: descriptor.DropKindTrigger, IfExists: true},
		},
		Operations:       []string{savepointForm, releaseForm},
		UnreadStatements: []string{"REPLACE"},
	}
}
