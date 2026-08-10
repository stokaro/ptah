//go:build integration

package migrator_test

import "testing"

func TestRevisionCompletionFailure_PostgresTransactionalLive(t *testing.T) {
	runRevisionCompletionScenario(t, postgresRevisionCompletionTarget(), retryAfterFixingTheRevisionWrite())
}

func TestRevisionCompletionFailure_MySQLImplicitCommitLive(t *testing.T) {
	runRevisionCompletionScenario(t, mySQLRevisionCompletionTarget(), retryAfterFixingTheRevisionWrite())
}

func TestRevisionCompletionFailure_MariaDBImplicitCommitLive(t *testing.T) {
	runRevisionCompletionScenario(t, mariaDBRevisionCompletionTarget(), retryAfterFixingTheRevisionWrite())
}

func TestRevisionCompletionFailure_ClickHouseNoTransactionLive(t *testing.T) {
	runRevisionCompletionScenario(t, clickHouseRevisionCompletionTarget(), markAppliedAfterTheBodyCommitted())
}

func TestRevisionCompletionRepair_MySQLImplicitCommitLive(t *testing.T) {
	runRevisionCompletionScenario(t, mySQLRevisionCompletionTarget(), markAppliedAfterTheBodyCommitted())
}

func TestRevisionCompletionRepair_MariaDBImplicitCommitLive(t *testing.T) {
	runRevisionCompletionScenario(t, mariaDBRevisionCompletionTarget(), markAppliedAfterTheBodyCommitted())
}

func TestRevisionCompletionFailure_MySQLKeepsDDLAndTrailingDMLLive(t *testing.T) {
	runRevisionCompletionAfterDDL(t, mySQLRevisionCompletionTarget())
}

func TestRevisionCompletionFailure_MariaDBKeepsDDLAndTrailingDMLLive(t *testing.T) {
	runRevisionCompletionAfterDDL(t, mariaDBRevisionCompletionTarget())
}

func TestRevisionCompletionFailure_MySQLRollsBackADMLOnlyBodyLive(t *testing.T) {
	runRevisionCompletionDMLOnly(t, mySQLRevisionCompletionTarget())
}

func TestRevisionCompletionFailure_MariaDBRollsBackADMLOnlyBodyLive(t *testing.T) {
	runRevisionCompletionDMLOnly(t, mariaDBRevisionCompletionTarget())
}
