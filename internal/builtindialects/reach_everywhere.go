package builtindialects

import "ptah.run/core/platform/descriptor"

// The constructs of each engine whose spelling means nothing ordinary on any
// engine, which internal/sqlreach refuses in a statement of every engine
// (descriptor.Reach.Everywhere). Each list keeps the order its engine's
// constructs are asked in.

// sqliteEverywhere are SQLite's: attaching or detaching another database
// file, writing a copy of the database or moving its storage to a path on the
// host, and loading a native extension.
func sqliteEverywhere() []descriptor.ReachConstruct {
	return []descriptor.ReachConstruct{
		{
			Construct:  "ATTACH",
			Reach:      "attaches another SQLite database file to the session, so the statement can write to databases other than the dev database",
			StartsWith: []string{"ATTACH"},
		},
		{
			Construct:  "DETACH",
			Reach:      "manipulates the session's attached SQLite databases rather than the dev database schema",
			StartsWith: []string{"DETACH"},
		},
		{
			Construct:  "VACUUM INTO",
			Reach:      "writes a copy of the database to an arbitrary path on the host filesystem",
			StartsWith: []string{"VACUUM"},
			Keyword:    "INTO",
		},
		{
			Construct: "PRAGMA temp_store_directory",
			Reach:     "redirects SQLite temporary storage to an arbitrary directory on the host filesystem",
			Pragmas:   []string{"TEMP_STORE_DIRECTORY"},
		},
		{
			Construct: "PRAGMA data_store_directory",
			Reach:     "redirects SQLite database storage to an arbitrary directory on the host filesystem",
			Pragmas:   []string{"DATA_STORE_DIRECTORY"},
		},
		{
			Construct: "load_extension",
			Reach:     "loads a native extension module from the host filesystem",
			Calls:     []string{"LOAD_EXTENSION"},
		},
	}
}

// mysqlEverywhere are MySQL's, which MariaDB shares: reading and writing
// files on the server or client host, a table stored on another server, a
// connection to another server, native code loaded into the server, and table
// data placed at a path on the host. MariaDB's descriptor states none of its
// own, since every one of its constructs is in this list and the list is
// refused on every engine.
func mysqlEverywhere() []descriptor.ReachConstruct {
	return []descriptor.ReachConstruct{
		{
			Construct:  "LOAD DATA INFILE",
			Reach:      "reads a file from the database server or the client host",
			StartsWith: []string{"LOAD"},
			Keyword:    "INFILE",
		},
		{
			// The SQL Server table form `SELECT * INTO outfile FROM src` and
			// `INSERT INTO outfile ...` name a table rather than a path, and no
			// string follows.
			Construct:       "SELECT ... INTO OUTFILE",
			Reach:           "writes a file on the database server host",
			WordsThenString: []string{"INTO", "OUTFILE"},
		},
		{
			Construct:       "SELECT ... INTO DUMPFILE",
			Reach:           "writes a file on the database server host",
			WordsThenString: []string{"INTO", "DUMPFILE"},
		},
		{
			Construct: "LOAD_FILE",
			Reach:     "reads a file on the database server host",
			Calls:     []string{"LOAD_FILE"},
		},
		{
			Construct: "ENGINE=FEDERATED",
			Reach:     "stores the table on another database server reached over the network",
			Engines:   []string{"FEDERATED"},
		},
		createServer(),
		{
			Construct:  "INSTALL PLUGIN",
			Reach:      "loads a server-side plugin, which runs native code on the database server host",
			StartsWith: []string{"INSTALL", "PLUGIN"},
		},
		{
			Construct:  "INSTALL COMPONENT",
			Reach:      "loads a server-side component, which runs native code on the database server host",
			StartsWith: []string{"INSTALL", "COMPONENT"},
		},
		{
			// A column or an index named directory never carries a path.
			Construct:  "DATA DIRECTORY",
			Reach:      "places table data at an arbitrary path on the database server host",
			PathOption: []string{"DATA", "DIRECTORY"},
		},
		{
			Construct:  "INDEX DIRECTORY",
			Reach:      "places index data at an arbitrary path on the database server host",
			PathOption: []string{"INDEX", "DIRECTORY"},
		},
	}
}

// createServer is CREATE SERVER, which defines a foreign server on
// PostgreSQL and a FEDERATED connection on MySQL. Both state it, and a scan
// asks it once.
func createServer() descriptor.ReachConstruct {
	return descriptor.ReachConstruct{
		Construct:  "CREATE SERVER",
		Reach:      "defines a connection to another database server",
		StartsWith: []string{"CREATE", "SERVER"},
	}
}

// clickHouseEverywhere are ClickHouse's table engines that store or read a
// table over the network or from the host, written ENGINE = URL(...) and so
// on. Its table functions are a family of descriptor.Reach of their own,
// since url(...) is an ordinary call elsewhere.
func clickHouseEverywhere() []descriptor.ReachConstruct {
	return []descriptor.ReachConstruct{{
		Construct: "ClickHouse remote table engine",
		Reach:     "stores or reads the table over the network or from the host filesystem",
		Engines:   []string{"URL", "FILE", "S3", "HDFS", "MYSQL", "POSTGRESQL"},
	}}
}

// postgresEverywhere are PostgreSQL's: COPY to or from a program or a file
// on the server host, the extensions that connect to another server or expose
// its files, the functions that read, write or list files on the host, and
// the large-object import and export.
func postgresEverywhere() []descriptor.ReachConstruct {
	return []descriptor.ReachConstruct{
		createServer(),
		{
			Construct:  "COPY ... PROGRAM",
			Reach:      "executes a shell command on the database server host",
			StartsWith: []string{"COPY"},
			Keyword:    "PROGRAM",
		},
		{
			// COPY t FROM STDIN has no string argument.
			Construct:   "COPY with a file path",
			Reach:       "reads or writes a file on the database server host",
			StartsWith:  []string{"COPY"},
			StringAfter: []string{"FROM", "TO"},
		},
		{
			Construct:    "dblink",
			Reach:        "opens a connection to another database server",
			CallPrefixes: []string{"DBLINK"},
		},
		{
			Construct: "postgres_fdw",
			Reach:     "federates data from another database server",
			Keyword:   "POSTGRES_FDW",
		},
		{
			Construct: "file_fdw",
			Reach:     "exposes files on the database server host as tables",
			Keyword:   "FILE_FDW",
		},
		{
			Construct: "pg_read_file",
			Reach:     "reads a file on the database server host",
			Calls:     []string{"PG_READ_FILE"},
		},
		{
			Construct: "pg_read_binary_file",
			Reach:     "reads a file on the database server host",
			Calls:     []string{"PG_READ_BINARY_FILE"},
		},
		{
			// adminpack, which ships with PostgreSQL and is installed on plenty
			// of servers. Each of these fits in one scalar SELECT and changes a
			// file on the host, which no transaction restores.
			Construct: "adminpack file function",
			Reach:     "writes, renames or removes a file on the database server host",
			Calls:     []string{"PG_FILE_WRITE", "PG_FILE_RENAME", "PG_FILE_UNLINK", "PG_FILE_SYNC", "PG_LOGDIR_LS"},
		},
		{
			// Every predefined pg_ls_ function lists a directory on the server
			// host -- pg_ls_dir, pg_ls_logdir, pg_ls_waldir, pg_ls_tmpdir,
			// pg_ls_archive_statusdir, pg_ls_replslotdir -- so the prefix is
			// the rule rather than a list that goes stale on the next release.
			Construct:    "pg_ls_ directory listing",
			Reach:        "lists a directory on the database server host",
			CallPrefixes: []string{"PG_LS_"},
		},
		{
			Construct: "lo_import",
			Reach:     "reads a file on the database server host",
			Calls:     []string{"LO_IMPORT"},
		},
		{
			Construct: "lo_export",
			Reach:     "writes a file on the database server host",
			Calls:     []string{"LO_EXPORT"},
		},
	}
}

// sqlServerEverywhere are SQL Server's: bulk loads from a file, the extended
// procedures that run a shell or list the host's directories, linked
// servers, and the functions that query another server or data source.
func sqlServerEverywhere() []descriptor.ReachConstruct {
	return []descriptor.ReachConstruct{
		{
			Construct:  "BULK INSERT",
			Reach:      "reads a file on the database server host",
			StartsWith: []string{"BULK", "INSERT"},
		},
		{
			Construct:  "xp_cmdshell",
			Reach:      "executes a shell command on the database server host",
			Procedures: []string{"XP_CMDSHELL"},
		},
		{
			Construct:  "xp_dirtree",
			Reach:      "lists directories on the database server host",
			Procedures: []string{"XP_DIRTREE"},
		},
		{
			Construct:  "sp_addlinkedserver",
			Reach:      "defines a connection to another database server",
			Procedures: []string{"SP_ADDLINKEDSERVER"},
		},
		{
			Construct: "OPENQUERY",
			Reach:     "runs a query on a linked server, which is another database server",
			Calls:     []string{"OPENQUERY"},
		},
		{
			Construct: "OPENROWSET",
			Reach:     "reads from another data source or a file on the database server host",
			Calls:     []string{"OPENROWSET"},
		},
		{
			Construct: "OPENDATASOURCE",
			Reach:     "reads from another data source reached over the network",
			Calls:     []string{"OPENDATASOURCE"},
		},
	}
}
