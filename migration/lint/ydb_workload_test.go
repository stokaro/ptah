package lint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/lint"
)

// TestYDBWorkloadRules_Report pins the three traps measured on 26.2.1.14 and
// 25.1.4.7: a drop of the pool default, after which no query of the database
// runs (YD120); a drop of a backup collection, which deletes its backups and
// stops a 25.1 server (YD121); and ANALYZE, which YDB refuses unless a flag
// that is off by default is on (YD122). Each reads the down half too.
func TestYDBWorkloadRules_Report(t *testing.T) {
	c := qt.New(t)

	sites := ydbSites(ydbLint(c, map[string]string{
		"0001_ops.up.sql": "CREATE TABLE t (id Uint64 NOT NULL, PRIMARY KEY (id));\n" +
			"DROP RESOURCE POOL default;\n" +
			"DROP RESOURCE POOL `default`;\n" +
			"DROP BACKUP COLLECTION nightly;\n" +
			"ANALYZE t;\n",
		"0001_ops.down.sql": "DROP BACKUP COLLECTION `nightly`;\n",
	}, ""))

	c.Assert(sites, qt.DeepEquals, []string{
		"0001_ops.down.sql:1:YD121",
		"0001_ops.up.sql:2:YD120",
		"0001_ops.up.sql:3:YD120",
		"0001_ops.up.sql:4:YD121",
		"0001_ops.up.sql:5:YD122",
	})
}

// TestYDBWorkloadRules_LeaveWhatTheServerKeeps holds the controls: another
// pool, the pool Default, whose name differs from default because a YDB name
// is case-sensitive, a classifier named default, a change of the pool
// default's settings, and the statements around a backup that keep it.
func TestYDBWorkloadRules_LeaveWhatTheServerKeeps(t *testing.T) {
	c := qt.New(t)

	sites := ydbSites(ydbLint(c, map[string]string{
		"0001_ops.up.sql": "DROP RESOURCE POOL batch;\n" +
			"DROP RESOURCE POOL `Default`;\n" +
			"DROP RESOURCE POOL CLASSIFIER default;\n" +
			"ALTER RESOURCE POOL default SET (CONCURRENT_QUERY_LIMIT = 10);\n" +
			"CREATE BACKUP COLLECTION nightly (TABLE t) WITH (STORAGE = 'cluster');\n" +
			"BACKUP nightly;\n" +
			"RESTORE nightly;\n",
	}, ""))

	c.Assert(sites, qt.HasLen, 0)
}

// TestYDBWorkloadRules_SayWhatToDo pins the three messages.
func TestYDBWorkloadRules_SayWhatToDo(t *testing.T) {
	c := qt.New(t)

	findings, err := lint.LintFS(fixture(map[string]string{
		"0001_ops.up.sql": "DROP RESOURCE POOL default;\nDROP BACKUP COLLECTION nightly;\nANALYZE t;\n",
	}), lint.Options{Dialect: "ydb"})

	c.Assert(err, qt.IsNil)
	messages := make([]string, 0, len(findings))
	for _, finding := range findings {
		messages = append(messages, finding.Rule+": "+finding.Message)
	}
	c.Assert(messages, qt.Contains, "YD120: DROP RESOURCE POOL default drops the pool YDB runs every query in that "+
		"no classifier sends elsewhere; YDB takes it, and every later query of the database, CREATE RESOURCE POOL "+
		"default included, fails with `Resource pool default not found`. Change its settings with ALTER RESOURCE "+
		"POOL default instead")
	c.Assert(messages, qt.Contains, "YD121: DROP BACKUP COLLECTION deletes every backup the collection holds, and a "+
		"YDB 25.1 server stops on it: measured on 25.1.4.7, the server process exits; copy or restore the backups "+
		"first, and run it only on a later line")
	c.Assert(messages, qt.Contains, "YD122: ANALYZE collects column statistics and changes no schema; YDB runs it "+
		"only with the EnableColumnStatistics flag on, which is off by default, and refuses it on a row table on "+
		"25.1, so on such a cluster the migration stops here after the statements before it applied. Collect "+
		"statistics outside the migration")
}
