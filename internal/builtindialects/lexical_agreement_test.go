package builtindialects_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/google/go-cmp/cmp/cmpopts"

	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/builtindialects"
)

// TestDescriptors_LexicalFactsAgreeWithTheSwitchesTheyReplace pins every
// built-in descriptor's Lexical section against the switches it replaces,
// written out here as they stood:
//
//   - internal/dialectlexer gave MySQL, MariaDB and ClickHouse backslash
//     escapes; SQL Server bracketed names and no # comments; PostgreSQL,
//     CockroachDB, YugabyteDB and Spanner the E'...' prefix, and PostgreSQL
//     alone its continued and U&'...' strings; MySQL and MariaDB the --
//     rule and their own executable comments. YDB was read by the YQL lexer.
//   - internal/deporder read ClickHouse's # and // comments and its
//     backslash escapes in strings and quoted names, and YDB by the YQL
//     lexer.
//   - internal/sqlreach refused Oracle's alternative-quoted literals.
//
// Where a descriptor differs from the switches, it does so deliberately,
// with a measurement in its comment. MySQL and MariaDB state their # comments
// and their string escapes, so the body scanner of internal/deporder reads
// both on these two, where its switch read neither; the shared lexer read
// both there already, and
// TestViewLikesForCreateForDialect_MySQLFamilyStringsAndComments in
// internal/deporder is the effect. SQLite states bracketed names, which no
// switch gave it, so a name such as [a;b] is one token wherever SQLite text
// is lexed.
func TestDescriptors_LexicalFactsAgreeWithTheSwitchesTheyReplace(t *testing.T) {
	postgresFamily := descriptor.Lexical{EscapeStrings: true}
	want := map[string]descriptor.Lexical{
		platform.Postgres:    {EscapeStrings: true, ContinuedStrings: true, UnicodeStrings: true},
		platform.CockroachDB: postgresFamily,
		platform.YugabyteDB:  postgresFamily,
		platform.Spanner:     postgresFamily,
		platform.MySQL: {
			LineComments: []string{"#"}, BackslashEscapes: true,
			DashCommentNeedsSpace: true, ExecutableComments: descriptor.ExecutableCommentsBang,
		},
		platform.MariaDB: {
			LineComments: []string{"#"}, BackslashEscapes: true,
			DashCommentNeedsSpace: true, ExecutableComments: descriptor.ExecutableCommentsMBang,
		},
		platform.ClickHouse: {
			LineComments: []string{"#", "//"}, BackslashEscapes: true, IdentifierBackslashEscapes: true,
		},
		platform.SQLite:    {BracketIdentifiers: true},
		platform.SQLServer: {BracketIdentifiers: true, HashNames: true},
		platform.Oracle:    {AlternativeQuoting: true},
		platform.YDB:       {Lexer: descriptor.LexerYQL},
	}
	described := builtindialects.Descriptors()
	c := qt.New(t)
	c.Assert(described, qt.HasLen, len(want))
	for _, dialect := range described {
		t.Run(dialect.Name, func(t *testing.T) {
			c := qt.New(t)
			expected, found := want[dialect.Name]
			c.Assert(found, qt.IsTrue)
			c.Assert(dialect.Lexical, qt.CmpEquals(cmpopts.EquateEmpty()), expected)
		})
	}
}

// TestDescriptors_BodiesAgreeWithTheSwitchTheyReplace pins Scripts.Bodies
// against the switch of internal/sqlcompound, which read a T-SQL body on SQL
// Server, a PL/SQL body on Oracle and YQL's bodies on YDB, and the
// dialect-blind BEGIN ... END everywhere else.
func TestDescriptors_BodiesAgreeWithTheSwitchTheyReplace(t *testing.T) {
	want := map[string]descriptor.Bodies{
		platform.SQLServer: descriptor.BodiesTransactSQL,
		platform.Oracle:    descriptor.BodiesPLSQL,
		platform.YDB:       descriptor.BodiesYQL,
	}
	for _, dialect := range builtindialects.Descriptors() {
		t.Run(dialect.Name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(dialect.Scripts.Bodies, qt.Equals, want[dialect.Name])
		})
	}
}

// TestDescriptors_ReachAgreesWithTheRulesItReplaces pins every built-in
// descriptor's Reach section against the rules of internal/sqlreach that
// named their dialects: the ClickHouse remote table functions, the Oracle
// network and server control packages, and the PostgreSQL server control
// functions refused on PostgreSQL, CockroachDB, YugabyteDB and Spanner. The
// names and the words of each refusal are written out as the rules held them.
func TestDescriptors_ReachAgreesWithTheRulesItReplaces(t *testing.T) {
	const (
		postgresConstruct = "PostgreSQL server control function"
		postgresReach     = "controls the server or its replication state rather than reading from it, " +
			"and no transaction undoes that"
	)
	postgresFamily := descriptor.Reach{
		ControlFunctions: descriptor.ReachCalls{
			Construct: postgresConstruct, Reach: postgresReach, Names: oldPostgresControlFunctions(),
		},
		ReplicationFunctions: descriptor.ReachCalls{
			Construct: postgresConstruct, Reach: postgresReach, Names: oldPostgresReplicationFunctions(),
		},
	}
	want := map[string]descriptor.Reach{
		platform.Postgres:    postgresFamily,
		platform.CockroachDB: postgresFamily,
		platform.YugabyteDB:  postgresFamily,
		platform.Spanner:     postgresFamily,
		platform.ClickHouse: {TableFunctions: descriptor.ReachCalls{
			Construct: "ClickHouse remote table function",
			Reach:     "reads over the network or from the host filesystem inside a query",
			Names:     oldClickHouseRemoteTableFunctions(),
		}},
		platform.Oracle: {
			NetworkPackages: descriptor.ReachCalls{
				Construct: "Oracle network package",
				Reach:     "sends a request from the database server, which no transaction retracts",
				Names: []string{
					"UTL_HTTP", "UTL_TCP", "UTL_SMTP", "UTL_MAIL",
					"UTL_INADDR", "UTL_URL", "UTL_FILE",
					"DBMS_LDAP", "HTTPURITYPE", "DBMS_NETWORK_ACL_ADMIN",
				},
			},
			ControlPackages: descriptor.ReachCalls{
				Construct: "Oracle server control package",
				Reach: "changes instance state that outlives the statement, " +
					"which no transaction undoes",
				Names: []string{"DBMS_PIPE", "DBMS_SCHEDULER", "DBMS_JOB"},
			},
		},
	}
	for _, dialect := range builtindialects.Descriptors() {
		t.Run(dialect.Name, func(t *testing.T) {
			c := qt.New(t)
			got := dialect.Reach
			got.Everywhere = nil
			c.Assert(got, qt.CmpEquals(cmpopts.EquateEmpty()), want[dialect.Name])
		})
	}
}

// oldPostgresControlFunctions is sqlreach.PostgresControlFunctions as it
// stood.
func oldPostgresControlFunctions() []string {
	return []string{
		"PG_CANCEL_BACKEND", "PG_TERMINATE_BACKEND",
		"PG_RELOAD_CONF", "PG_ROTATE_LOGFILE",
		"PG_PROMOTE", "PG_CREATE_RESTORE_POINT",
		"PG_SWITCH_WAL", "PG_WAL_REPLAY_PAUSE", "PG_WAL_REPLAY_RESUME",
		"PG_NOTIFY", "SET_CONFIG",
		"PG_STAT_RESET", "PG_STAT_RESET_SHARED", "PG_STAT_RESET_SLRU",
		"PG_STAT_RESET_SINGLE_TABLE_COUNTERS", "PG_STAT_RESET_SINGLE_FUNCTION_COUNTERS",
		"PG_STAT_RESET_REPLICATION_SLOT", "PG_STAT_RESET_SUBSCRIPTION_STATS",
		"PG_LOG_BACKEND_MEMORY_CONTEXTS",
		"PG_LOGICAL_EMIT_MESSAGE",
		"PG_BACKUP_START", "PG_BACKUP_STOP",
		"PG_START_BACKUP", "PG_STOP_BACKUP",
	}
}

// oldPostgresReplicationFunctions is sqlreach.PostgresReplicationFunctions as
// it stood.
func oldPostgresReplicationFunctions() []string {
	return []string{
		"PG_COPY_LOGICAL_REPLICATION_SLOT", "PG_COPY_PHYSICAL_REPLICATION_SLOT",
		"PG_CREATE_LOGICAL_REPLICATION_SLOT", "PG_CREATE_PHYSICAL_REPLICATION_SLOT",
		"PG_DROP_REPLICATION_SLOT", "PG_REPLICATION_SLOT_ADVANCE",
		"PG_REPLICATION_ORIGIN_ADVANCE", "PG_REPLICATION_ORIGIN_CREATE",
		"PG_REPLICATION_ORIGIN_DROP",
		"PG_REPLICATION_ORIGIN_SESSION_RESET", "PG_REPLICATION_ORIGIN_SESSION_SETUP",
		"PG_REPLICATION_ORIGIN_XACT_RESET", "PG_REPLICATION_ORIGIN_XACT_SETUP",
	}
}

// oldClickHouseRemoteTableFunctions is the sqlreach list as it stood.
func oldClickHouseRemoteTableFunctions() []string {
	bases := []string{
		"URL", "REMOTE", "REMOTESECURE", "CLUSTER", "CLUSTERALLREPLICAS",
		"S3", "GCS", "HDFS", "AZUREBLOBSTORAGE", "COSN", "OSS",
		"FILE", "INPUT", "EXECUTABLE", "EXECUTABLEPOOL",
		"MYSQL", "POSTGRESQL", "MONGODB", "REDIS", "SQLITE", "ODBC", "JDBC",
		"ICEBERG", "ICEBERGS3", "ICEBERGAZURE", "ICEBERGHDFS", "ICEBERGLOCAL",
		"DELTALAKE", "DELTALAKES3", "DELTALAKEAZURE", "HUDI", "FUZZJSON",
	}
	names := make([]string, 0, len(bases)*2)
	for _, base := range bases {
		names = append(names, base, base+"CLUSTER")
	}
	return names
}
