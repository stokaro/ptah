package builtindialects

import "ptah.run/core/platform/descriptor"

// The calls that reach outside the database, per engine, which
// internal/sqlreach refuses in a statement it scans. descriptor.Reach says
// what each family is.

// postgresReach is what the PostgreSQL family reaches with: CockroachDB,
// YugabyteDB and Spanner refuse the same names as PostgreSQL.
//
// The control functions act on the server rather than read from it:
// terminating a backend, reloading the configuration, promoting a standby,
// switching the write-ahead log. A read-only transaction permits every one of
// them and undoes none. internal/devclean refuses the same names in a
// statement it is about to run against a dev database, and the check-assertion
// validator refuses them in a predicate it is about to send to a live one.
//
// Advisory-lock functions are deliberately absent, and that is the one
// exclusion. A session advisory lock is released when the session ends, which
// an isolated query session guarantees by discarding the physical connection,
// so nothing outlives the statement for a caller to refuse.
//
// Replication state is a family of its own so a caller can say which it
// refused: internal/devclean names the two differently, while a scan that
// asks whether a statement reaches past itself takes both under one name. A
// slot outlives everything a session can undo: it is created by a SELECT, it
// survives the rollback and the discarded connection, and it retains
// write-ahead log until an operator removes it.
func postgresReach() descriptor.Reach {
	const (
		construct = "PostgreSQL server control function"
		reach     = "controls the server or its replication state rather than reading from it, " +
			"and no transaction undoes that"
	)
	return descriptor.Reach{
		ControlFunctions: descriptor.ReachCalls{Construct: construct, Reach: reach, Names: []string{
			"PG_CANCEL_BACKEND", "PG_TERMINATE_BACKEND",
			"PG_RELOAD_CONF", "PG_ROTATE_LOGFILE",
			"PG_PROMOTE", "PG_CREATE_RESTORE_POINT",
			"PG_SWITCH_WAL", "PG_WAL_REPLAY_PAUSE", "PG_WAL_REPLAY_RESUME",
			"PG_NOTIFY", "SET_CONFIG",
			"PG_STAT_RESET", "PG_STAT_RESET_SHARED", "PG_STAT_RESET_SLRU",
			"PG_STAT_RESET_SINGLE_TABLE_COUNTERS", "PG_STAT_RESET_SINGLE_FUNCTION_COUNTERS",
			"PG_STAT_RESET_REPLICATION_SLOT", "PG_STAT_RESET_SUBSCRIPTION_STATS",
			// The backend writes its memory contexts to the server log, which no
			// rollback retracts and no caller can read back.
			"PG_LOG_BACKEND_MEMORY_CONTEXTS",
			// A nontransactional logical message survives the rollback and reaches
			// whatever is decoding the write-ahead log.
			"PG_LOGICAL_EMIT_MESSAGE",
			// Backup mode forces a checkpoint and the write-ahead log activity
			// behind it. Ending the session aborts the unfinished backup and
			// leaves the checkpoint where it is.
			"PG_BACKUP_START", "PG_BACKUP_STOP",
			"PG_START_BACKUP", "PG_STOP_BACKUP",
		}},
		ReplicationFunctions: descriptor.ReachCalls{Construct: construct, Reach: reach, Names: []string{
			"PG_COPY_LOGICAL_REPLICATION_SLOT", "PG_COPY_PHYSICAL_REPLICATION_SLOT",
			"PG_CREATE_LOGICAL_REPLICATION_SLOT", "PG_CREATE_PHYSICAL_REPLICATION_SLOT",
			"PG_DROP_REPLICATION_SLOT", "PG_REPLICATION_SLOT_ADVANCE",
			"PG_REPLICATION_ORIGIN_ADVANCE", "PG_REPLICATION_ORIGIN_CREATE",
			"PG_REPLICATION_ORIGIN_DROP",
			"PG_REPLICATION_ORIGIN_SESSION_RESET", "PG_REPLICATION_ORIGIN_SESSION_SETUP",
			"PG_REPLICATION_ORIGIN_XACT_RESET", "PG_REPLICATION_ORIGIN_XACT_SETUP",
		}},
		Everywhere: postgresEverywhere(),
	}
}

// clickHouseReach is ClickHouse's: its table functions that read something
// other than the server's own storage, each with its Cluster variant, which
// ClickHouse spells by suffix and which reaches the same place from every
// node. ClickHouse takes them in the FROM clause of an ordinary SELECT, so a
// statement that declares nothing and begins with SELECT still fetches a URL
// the author named, from inside the server's network.
//
// Enumeration rather than a prefix: file and url are short enough that a
// prefix match would catch fileSize and urlHash, which read nothing. The list
// is the one a reader can check against ClickHouse's own table-function index,
// and a name missing from it is a gap to add rather than a rule to loosen.
func clickHouseReach() descriptor.Reach {
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
	return descriptor.Reach{
		TableFunctions: descriptor.ReachCalls{
			Construct: "ClickHouse remote table function",
			Reach:     "reads over the network or from the host filesystem inside a query",
			Names:     names,
		},
		Everywhere: clickHouseEverywhere(),
	}
}

// oracleReach is Oracle's. Oracle reaches the network through built-in
// packages rather than through a table or a function name of its own, and a
// package name is matched wherever it is called: the call is qualified, so
// the callable token is the procedure and the package sits in front of a dot.
//
// Oracle's own instance is the other side of the control packages. A pipe is
// server memory shared by every session, a job runs after the statement that
// created it, and both are committed by the package rather than by the
// transaction around it.
func oracleReach() descriptor.Reach {
	return descriptor.Reach{
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
			Reach:     "changes instance state that outlives the statement, which no transaction undoes",
			Names:     []string{"DBMS_PIPE", "DBMS_SCHEDULER", "DBMS_JOB"},
		},
	}
}
