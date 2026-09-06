package migrationlintreport_test

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/config/projectconfig"
	"ptah.run/internal/migrationlintreport"
	migrationlint "ptah.run/migration/lint"
	"ptah.run/migration/migrationfile"
)

// buildWithProjectNaming runs a lint report over one migration with the
// project config a `lint { naming ... }` block would have produced, and the
// `.ptah-lint.yaml` beside the migration when one is given.
func buildWithProjectNaming(
	c *qt.C, sql, policy string, naming *projectconfig.LintNamingConfig,
) (migrationlintreport.Report, error) {
	c.Helper()
	fsys := fstest.MapFS{"1_init.sql": {Data: []byte(sql)}}
	if policy != "" {
		fsys[migrationlint.ConfigFileName] = &fstest.MapFile{Data: []byte(policy)}
	}
	return migrationlintreport.Build(c.TB.Context(), migrationlintreport.Options{
		Dir:       "unused",
		FS:        fsys,
		DirFormat: string(migrationfile.DirFormatAtlas),
		Dialect:   "postgres",
		FailOn:    migrationlintreport.FailOnNone,
		Changed:   migrationlintreport.ChangedOptions{Dialect: true},
	}, projectconfig.Config{Lint: projectconfig.LintConfig{Naming: naming}})
}

// TestBuild_ProjectNamingBlockReachesTheRules is the end-to-end assertion for
// the project-file spelling: the Atlas naming block reaches the six rules,
// `error = true` is their severity, and a kind's own pattern wins.
func TestBuild_ProjectNamingBlockReachesTheRules(t *testing.T) {
	c := qt.New(t)

	report, err := buildWithProjectNaming(c, "CREATE TABLE Invoices (id int);\nCREATE INDEX invoices_id ON Invoices (id);\n", "",
		&projectconfig.LintNamingConfig{
			Match:   "^[a-z_]+$",
			Message: "lower snake case",
			Error:   true,
			Index:   &projectconfig.LintNamingPattern{Match: "^idx_", Message: "prefix with idx_"},
		})

	c.Assert(err, qt.IsNil)
	c.Assert(messagesOf(report, "NM102"), qt.DeepEquals, []string{"table name Invoices does not match the naming convention ^[a-z_]+$: lower snake case"})
	c.Assert(messagesOf(report, "NM104"), qt.DeepEquals, []string{"index name invoices_id does not match the naming convention ^idx_: prefix with idx_"})
	c.Assert(severitiesOf(report, "NM102"), qt.DeepEquals, []migrationlint.Severity{migrationlint.SeverityError})
}

// TestBuild_PolicyFileNamingWinsOverTheProjectBlock: the merge keeps the
// precedence `rules` entries have, so the file beside the migrations decides.
func TestBuild_PolicyFileNamingWinsOverTheProjectBlock(t *testing.T) {
	c := qt.New(t)

	report, err := buildWithProjectNaming(c, "CREATE TABLE Invoices (id int);\n",
		"naming:\n  match: '^[A-Z][a-z]+$'\n",
		&projectconfig.LintNamingConfig{Match: "^[a-z_]+$", Error: true})

	c.Assert(err, qt.IsNil)
	c.Assert(messagesOf(report, "NM102"), qt.HasLen, 0)
	c.Assert(messagesOf(report, "NM103"), qt.DeepEquals, []string{"column name id does not match the naming convention ^[A-Z][a-z]+$"})
}

func TestBuild_WithoutANamingBlockTheRulesStaySilent(t *testing.T) {
	c := qt.New(t)

	report, err := buildWithProjectNaming(c, "CREATE TABLE Invoices (Id int);\n", "", nil)

	c.Assert(err, qt.IsNil)
	c.Assert(messagesOf(report, "NM102"), qt.HasLen, 0)
	c.Assert(messagesOf(report, "NM103"), qt.HasLen, 0)
}
