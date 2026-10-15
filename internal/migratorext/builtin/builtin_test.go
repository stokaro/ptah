package builtin_test

import (
	"slices"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/builtindialects"
	"ptah.run/internal/migratorext"
	"ptah.run/internal/migratorext/builtin"
	"ptah.run/migration/migrationfile"
)

// Every built-in dialect has services, and each is served by exactly the
// services that name it, so no engine falls back to the generic behavior by
// accident.
func TestLookup_ServesEveryBuiltinDialect(t *testing.T) {
	for _, described := range builtindialects.Descriptors() {
		t.Run(described.Name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(builtin.Lookup(described.Name).Dialects, qt.Contains, described.Name)
		})
	}
}

func TestLookup_FoldsSpellings(t *testing.T) {
	c := qt.New(t)
	c.Assert(builtin.Lookup(" PGX ").Dialects, qt.DeepEquals, builtin.Lookup(platform.Postgres).Dialects)
	c.Assert(builtin.Lookup("mssql").Dialects, qt.DeepEquals, []string{platform.SQLServer})
}

func TestLookup_UnknownDialectTakesTheGenericBehavior(t *testing.T) {
	c := qt.New(t)
	services := builtin.Lookup("nonesuch")
	c.Assert(services.Dialects, qt.IsNil)
	c.Assert(services.Catalog, qt.IsNil)
	c.Assert(services.Timeouts, qt.IsNil)
	c.Assert(services.Units, qt.IsNil)
}

func TestDialects_AreCanonicalNames(t *testing.T) {
	c := qt.New(t)
	names := builtin.Dialects()
	slices.Sort(names)
	var want []string
	for _, described := range builtindialects.Descriptors() {
		want = append(want, described.Name)
	}
	slices.Sort(want)
	c.Assert(names, qt.DeepEquals, want)
}

func TestTimeouts_Statements(t *testing.T) {
	timeouts := migrationfile.Timeouts{
		LockTimeout: 1500 * time.Millisecond, HasLockTimeout: true,
		StatementTimeout: 2500 * time.Millisecond, HasStatementTimeout: true,
	}
	tests := []struct {
		name        string
		dialect     string
		scope       migratorext.Scope
		wantSetup   []string
		wantRestore []string
	}{
		{
			name: "postgres in a transaction", dialect: platform.Postgres,
			wantSetup: []string{"SET LOCAL lock_timeout = '1500ms'", "SET LOCAL statement_timeout = '2500ms'"},
		},
		{
			name: "cockroachdb on a session", dialect: platform.CockroachDB, scope: migratorext.ScopeSession,
			wantSetup:   []string{"SET lock_timeout = '1500ms'", "SET statement_timeout = '2500ms'"},
			wantRestore: []string{"RESET statement_timeout", "RESET lock_timeout"},
		},
		{
			name: "mysql bounds a statement in milliseconds", dialect: platform.MySQL,
			wantSetup: []string{
				"SET @ptah_prev_innodb_lock_wait_timeout = @@SESSION.innodb_lock_wait_timeout",
				"SET SESSION innodb_lock_wait_timeout = 2",
				"SET @ptah_prev_lock_wait_timeout = @@SESSION.lock_wait_timeout",
				"SET SESSION lock_wait_timeout = 2",
				"SET @ptah_prev_max_execution_time = @@SESSION.max_execution_time",
				"SET SESSION max_execution_time = 2500",
			},
			wantRestore: []string{
				"SET SESSION max_execution_time = @ptah_prev_max_execution_time",
				"SET SESSION lock_wait_timeout = @ptah_prev_lock_wait_timeout",
				"SET SESSION innodb_lock_wait_timeout = @ptah_prev_innodb_lock_wait_timeout",
			},
		},
		{
			name: "mariadb bounds a statement in seconds", dialect: platform.MariaDB,
			wantSetup: []string{
				"SET @ptah_prev_innodb_lock_wait_timeout = @@SESSION.innodb_lock_wait_timeout",
				"SET SESSION innodb_lock_wait_timeout = 2",
				"SET @ptah_prev_lock_wait_timeout = @@SESSION.lock_wait_timeout",
				"SET SESSION lock_wait_timeout = 2",
				"SET @ptah_prev_max_statement_time = @@SESSION.max_statement_time",
				"SET SESSION max_statement_time = 2.5",
			},
			wantRestore: []string{
				"SET SESSION max_statement_time = @ptah_prev_max_statement_time",
				"SET SESSION lock_wait_timeout = @ptah_prev_lock_wait_timeout",
				"SET SESSION innodb_lock_wait_timeout = @ptah_prev_innodb_lock_wait_timeout",
			},
		},
		{name: "ydb sends nothing", dialect: platform.YDB},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			setup, restore, err := builtin.Lookup(test.dialect).Timeouts.Statements(timeouts, test.scope)
			c.Assert(err, qt.IsNil)
			c.Assert(setup, qt.DeepEquals, test.wantSetup)
			c.Assert(restore, qt.DeepEquals, test.wantRestore)
		})
	}
}

func TestCatalog_PresenceQueryBindsItsNames(t *testing.T) {
	table := migratorext.Table{ConnectionSchema: "conn", Name: "schema_migrations", Quote: func(s string) string { return "<" + s + ">" }}
	tests := []struct {
		dialect   string
		wantQuery string
		wantArgs  []any
	}{
		{
			dialect: platform.Postgres,
			wantQuery: "SELECT COUNT(*)\nFROM information_schema.tables\n" +
				"WHERE table_schema = current_schema() AND table_name = ? AND table_type = 'BASE TABLE'",
			wantArgs: []any{"schema_migrations"},
		},
		{
			dialect: platform.Spanner,
			wantQuery: "SELECT COUNT(*)\nFROM information_schema.tables\n" +
				"WHERE table_schema = ? AND table_name = ? AND table_type = 'BASE TABLE'",
			wantArgs: []any{"conn", "schema_migrations"},
		},
		{
			dialect:   platform.ClickHouse,
			wantQuery: "SELECT count()\nFROM system.tables\nWHERE database = ? AND name = ? AND is_temporary = 0",
			wantArgs:  []any{"conn", "schema_migrations"},
		},
		{
			dialect:   platform.SQLite,
			wantQuery: "SELECT COUNT(*) FROM <conn>.sqlite_schema WHERE type = 'table' AND name = ? COLLATE NOCASE",
			wantArgs:  []any{"schema_migrations"},
		},
	}
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			query, args, err := builtin.Lookup(test.dialect).Catalog.PresenceQuery(table)
			c.Assert(err, qt.IsNil)
			c.Assert(query, qt.Equals, test.wantQuery)
			c.Assert(args, qt.DeepEquals, test.wantArgs)
		})
	}
}

func TestUnits_SplitYQL(t *testing.T) {
	c := qt.New(t)
	units := builtin.Lookup(platform.YDB).Units

	split, err := units.Split("CREATE TABLE a (id Int64, PRIMARY KEY (id));\nUPSERT INTO a (id) VALUES (1l);\n")

	c.Assert(err, qt.IsNil)
	c.Assert(split, qt.HasLen, 2)
	c.Assert(units.IsData(split[0].Text), qt.IsFalse)
	c.Assert(units.IsData(split[1].Text), qt.IsTrue)
}

func TestChecks_ValidateRefusesASequenceAdvance(t *testing.T) {
	tests := []struct {
		dialect   string
		assertion string
		wantErr   string
	}{
		{dialect: platform.SQLServer, assertion: "SELECT NEXT VALUE FOR s",
			wantErr: "check assertion must not advance a SQL Server sequence with NEXT VALUE FOR"},
		{dialect: platform.Oracle, assertion: "SELECT s.NEXTVAL FROM dual",
			wantErr: "check assertion must not advance an Oracle sequence with NEXTVAL"},
	}
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(builtin.Lookup(test.dialect).Checks.Validate(test.assertion), qt.ErrorMatches, test.wantErr)
			c.Assert(builtin.Lookup(test.dialect).Checks.Validate("SELECT 1"), qt.IsNil)
		})
	}
}
