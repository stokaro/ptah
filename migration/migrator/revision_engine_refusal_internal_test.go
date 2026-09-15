package migrator

// White-box testing required: revisionEngineRefusal is package-local, and what
// it protects is a statement that has not run yet, so no exported call can
// observe it without a live database of each dialect.

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
)

// TestRevisionEngineRefusal_TurnsDownAnEngineTheTableCannotBe pins the refusal
// that has to happen before the CREATE rather than after it.
//
// On the MySQL family the server takes the statement and Ptah then refuses the
// table: requireTransactionalMetadataEngine reads the engine back and insists on
// InnoDB, because the revision table is the witness that a migration was applied
// and MyISAM has no transaction to roll a failed one back with. MySQL DDL
// commits and the create is `CREATE TABLE IF NOT EXISTS`, so a refusal after the
// fact leaves a table Ptah will not use and will not recreate -- every later
// verb fails until somebody drops it by hand.
//
// SQL Server is the opposite: the statement there has no engine clause at all,
// so a named engine is dropped in silence (stokaro/ptah#2234).
func TestRevisionEngineRefusal_TurnsDownAnEngineTheTableCannotBe(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		engine  string
		want    string
	}{
		{
			name:    "mysql refuses an engine that is not InnoDB",
			dialect: platform.MySQL,
			engine:  "MyISAM",
			want:    `.*"MyISAM".*must be InnoDB.*--migrations-engine.*PTAH_MIGRATIONS_ENGINE.*`,
		},
		{
			name:    "mariadb refuses it too",
			dialect: platform.MariaDB,
			engine:  "Aria",
			want:    `.*"Aria".*must be InnoDB.*`,
		},
		{
			name:    "sqlserver refuses any engine at all",
			dialect: platform.SQLServer,
			engine:  "MergeTree",
			want:    `.*"MergeTree".*no engine clause.*`,
		},
		{
			// Oracle commits DDL implicitly as the MySQL family does, and that
			// is all the two share: read as that family, the refusal tells an
			// Oracle operator the engine must be InnoDB (stokaro/ptah#3298).
			name:    "oracle refuses any engine at all",
			dialect: platform.Oracle,
			engine:  "MergeTree",
			want:    `migrations engine "MergeTree" cannot be named on oracle: the revision table there has no engine clause .*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(revisionEngineRefusal(test.dialect, test.engine), qt.ErrorMatches, test.want)
		})
	}
}

// TestRevisionEngineRefusal_AcceptsWhatTheTableCanBe is the control.
//
// A refusal that turned down more than it should would make the flag useless on
// the one dialect it exists for, and would break every MySQL deployment that
// names the engine it already has.
func TestRevisionEngineRefusal_AcceptsWhatTheTableCanBe(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		engine  string
	}{
		{name: "clickhouse is the server's judgment, not this one", dialect: platform.ClickHouse, engine: "ReplicatedMergeTree('/x', '{replica}')"},
		{name: "clickhouse may also be given an engine it will refuse", dialect: platform.ClickHouse, engine: "Log"},
		{name: "mysql accepts the engine it must have", dialect: platform.MySQL, engine: "InnoDB"},
		{name: "mysql accepts it however it is spelled", dialect: platform.MySQL, engine: "innodb"},
		{name: "an unset engine is what every other target uses", dialect: platform.MySQL, engine: ""},
		{name: "sqlserver with no engine is untouched", dialect: platform.SQLServer, engine: ""},
		{name: "oracle with no engine is untouched", dialect: platform.Oracle, engine: ""},
		{name: "postgres names none and is asked for none", dialect: platform.Postgres, engine: ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(revisionEngineRefusal(test.dialect, test.engine), qt.IsNil)
		})
	}
}

// TestRevisionTableHasNoEngineClause_AgreesWithBothBuilders holds the refusal
// and the statements it speaks for to one answer.
//
// The native builder is compared over every dialect with a capability preset,
// in both directions: a target the predicate lists must render no engine, and
// a target it does not list must render the one it was given. The Atlas
// builder has no engine clause on the PostgreSQL family either, so it is held
// only for the targets the predicate lists.
func TestRevisionTableHasNoEngineClause_AgreesWithBothBuilders(t *testing.T) {
	const engine = "ProbeEngine3298"

	for _, dialect := range capability.DefaultDialects() {
		t.Run("native "+dialect, func(t *testing.T) {
			c := qt.New(t)

			ddl := ptahRevisionsTableDDL(dialect, `"schema_migrations"`, "N'schema_migrations'", engine)

			c.Assert(strings.Contains(ddl, engine), qt.Equals, !revisionTableHasNoEngineClause(dialect))
		})
	}
	for _, dialect := range []string{platform.SQLServer, platform.Oracle} {
		t.Run("atlas "+dialect, func(t *testing.T) {
			c := qt.New(t)

			ddl := atlasRevisionsTableDDL(dialect, `"atlas_schema_revisions"`, "N'atlas_schema_revisions'", engine)

			c.Assert(revisionTableHasNoEngineClause(dialect), qt.IsTrue)
			c.Assert(ddl, qt.Not(qt.Contains), engine)
		})
	}
}
