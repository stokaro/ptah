package builtindialects_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/builtindialects"
)

// TestDescriptors_EverywhereAgreesWithTheRulesItReplaces pins which engine
// states which construct of descriptor.Reach.Everywhere, in the order it
// states them. They are the rules internal/sqlreach refused on every engine,
// each given to the engine whose grammar has it: the PostgreSQL family shares
// PostgreSQL's, MariaDB's are MySQL's, and CREATE SERVER is both PostgreSQL's
// and MySQL's. The words of each refusal are pinned in internal/sqlreach,
// where the scan names them.
func TestDescriptors_EverywhereAgreesWithTheRulesItReplaces(t *testing.T) {
	postgresFamily := []string{
		"CREATE SERVER", "COPY ... PROGRAM", "COPY with a file path", "dblink", "postgres_fdw", "file_fdw",
		"pg_read_file", "pg_read_binary_file", "adminpack file function", "pg_ls_ directory listing",
		"lo_import", "lo_export",
	}
	want := map[string][]string{
		platform.Postgres:    postgresFamily,
		platform.CockroachDB: postgresFamily,
		platform.YugabyteDB:  postgresFamily,
		platform.Spanner:     postgresFamily,
		platform.MySQL: {
			"LOAD DATA INFILE", "SELECT ... INTO OUTFILE", "SELECT ... INTO DUMPFILE", "LOAD_FILE",
			"ENGINE=FEDERATED", "CREATE SERVER", "INSTALL PLUGIN", "INSTALL COMPONENT",
			"DATA DIRECTORY", "INDEX DIRECTORY",
		},
		platform.MariaDB:    nil,
		platform.ClickHouse: {"ClickHouse remote table engine"},
		platform.SQLite: {
			"ATTACH", "DETACH", "VACUUM INTO", "PRAGMA temp_store_directory", "PRAGMA data_store_directory",
			"load_extension",
		},
		platform.SQLServer: {
			"BULK INSERT", "xp_cmdshell", "xp_dirtree", "sp_addlinkedserver", "OPENQUERY", "OPENROWSET",
			"OPENDATASOURCE",
		},
		platform.Oracle: nil,
		platform.YDB:    nil,
	}
	described := builtindialects.Descriptors()
	c := qt.New(t)
	c.Assert(described, qt.HasLen, len(want))
	for _, dialect := range described {
		t.Run(dialect.Name, func(t *testing.T) {
			c := qt.New(t)
			expected, found := want[dialect.Name]
			c.Assert(found, qt.IsTrue)
			c.Assert(constructNames(dialect.Reach.Everywhere), qt.DeepEquals, expected)
		})
	}
}

func constructNames(constructs []descriptor.ReachConstruct) []string {
	var names []string
	for _, construct := range constructs {
		names = append(names, construct.Construct)
	}
	return names
}
