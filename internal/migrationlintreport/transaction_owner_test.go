package migrationlintreport_test

import (
	"context"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/config/projectconfig"
	"ptah.run/core/lintext"
	"ptah.run/internal/migrationlintreport"
	migrationlint "ptah.run/migration/lint"
	"ptah.run/migration/migrationfile"
)

// TestBuild_TransactionOwnerReachesTheRules is the report both lint commands
// build, over the Atlas-layout copy ptah-compat makes of a golang-migrate
// directory. Options.TransactionOwner reaches the owner's TX201; a Build that
// leaves it out of the analysis options reports TX201 on the second row.
func TestBuild_TransactionOwnerReachesTheRules(t *testing.T) {
	for _, test := range []struct {
		name  string
		owner lintext.TransactionOwner
		want  []string
	}{
		{"the migrator wraps each file", lintext.TransactionOwnerMigrator, []string{"TX201"}},
		{"each file opens its own", lintext.TransactionOwnerFile, make([]string, 0)},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			report, err := migrationlintreport.Build(context.Background(), migrationlintreport.Options{
				Runtime: selectedRuntime(c),
				Dir:     c.TempDir(),
				FS: fstest.MapFS{"20240826120919_init.sql": {
					Data: []byte("BEGIN;\nCREATE TABLE t (id int);\nINSERT INTO t VALUES (1);\nCOMMIT;\n"),
				}},
				DirFormat:        string(migrationfile.DirFormatAtlas),
				Dialect:          "postgres",
				FailOn:           migrationlintreport.FailOnNone,
				Compatibility:    migrationlint.CompatibilityProfileAtlas,
				TransactionOwner: test.owner,
				Changed:          migrationlintreport.ChangedOptions{Dialect: true},
			}, projectconfig.Config{})

			c.Assert(err, qt.IsNil)
			c.Assert(findingRules(report.Findings), qt.DeepEquals, test.want)
		})
	}
}
