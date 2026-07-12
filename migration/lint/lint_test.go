package lint_test

import (
	"os"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/migration/lint"
)

// fixture builds an in-memory migrations directory.
func fixture(files map[string]string) fstest.MapFS {
	fsys := fstest.MapFS{}
	for name, content := range files {
		fsys[name] = &fstest.MapFile{Data: []byte(content)}
	}
	return fsys
}

// rulesOf collects the rule codes of findings, with duplicates.
func rulesOf(findings []lint.Finding) []string {
	codes := make([]string, 0, len(findings))
	for _, f := range findings {
		codes = append(codes, f.Rule)
	}
	return codes
}

func TestLintFS_CuratedHazardsProduceExpectedRuleHits(t *testing.T) {
	c := qt.New(t)

	fsys := fixture(map[string]string{
		"0000000001_bad.up.sql": `-- hazards ahead; note the semicolon in this comment: DROP TABLE decoy;
DROP TABLE audit_log;
ALTER TABLE users DROP COLUMN legacy;
ALTER TABLE users RENAME COLUMN email TO email_address;
ALTER TABLE users MODIFY COLUMN email VARCHAR(64);
CREATE UNIQUE INDEX uq_users_email ON users (email);
ALTER TYPE mood ADD VALUE 'ambivalent';
`,
		"0000000001_bad.down.sql": "-- restore\n",
	})

	findings, err := lint.LintFS(fsys, lint.Options{})
	c.Assert(err, qt.IsNil)

	c.Assert(rulesOf(findings), qt.DeepEquals, []string{
		"DS101", // DROP TABLE audit_log
		"DS102", // DROP COLUMN legacy
		"BC101", // RENAME COLUMN
		"DS103", // MODIFY COLUMN (lossy)
		"MY101", // MODIFY COLUMN (lock-heavy)
		"PG101", // CREATE UNIQUE INDEX without CONCURRENTLY
		"PG102", // ALTER TYPE ADD VALUE
	})

	// Line numbers point at the offending statements, not the file head:
	// the comment on line 1 (with its decoy semicolon) shifts DROP TABLE to
	// line 2 and everything after accordingly.
	byRule := map[string]lint.Finding{}
	for _, f := range findings {
		byRule[f.Rule] = f
	}
	c.Assert(byRule["DS101"].Line, qt.Equals, 2)
	c.Assert(byRule["DS102"].Line, qt.Equals, 3)
	c.Assert(byRule["BC101"].Line, qt.Equals, 4)
	c.Assert(byRule["PG102"].Line, qt.Equals, 7)
	c.Assert(byRule["DS101"].Severity, qt.Equals, lint.SeverityError)
	c.Assert(byRule["PG101"].Severity, qt.Equals, lint.SeverityWarning)
}

func TestLintFS_CleanMigrationHasNoFindings(t *testing.T) {
	c := qt.New(t)

	fsys := fixture(map[string]string{
		"0000000001_create_users.up.sql":   "CREATE TABLE users (id SERIAL PRIMARY KEY, email VARCHAR(255) NOT NULL);\n",
		"0000000001_create_users.down.sql": "DROP TABLE IF EXISTS users;\n",
	})

	findings, err := lint.LintFS(fsys, lint.Options{Dialect: "mysql"})
	c.Assert(err, qt.IsNil)
	c.Assert(findings, qt.HasLen, 0,
		qt.Commentf("a plain CREATE TABLE with a paired down file is clean; got: %v", findings))
}

func TestLintFS_DownMigrationStatementsAreNotLinted(t *testing.T) {
	c := qt.New(t)

	// A down migration dropping what its up created is the expected shape.
	fsys := fixture(map[string]string{
		"0000000001_create_users.up.sql":   "CREATE TABLE users (id SERIAL PRIMARY KEY);\n",
		"0000000001_create_users.down.sql": "DROP TABLE users;\nALTER TABLE audit DROP COLUMN old;\n",
	})

	findings, err := lint.LintFS(fsys, lint.Options{})
	c.Assert(err, qt.IsNil)
	c.Assert(findings, qt.HasLen, 0, qt.Commentf("got: %v", findings))
}

func TestLintFS_MigrationFormRules(t *testing.T) {
	c := qt.New(t)

	fsys := fixture(map[string]string{
		"0000000001_orphan.up.sql":  "CREATE TABLE t (id INT);\n",         // MF101: no down
		"0000000002_empty.up.sql":   "-- only comments; nothing to run\n", // MF102
		"0000000002_empty.down.sql": "-- nothing\n",
		"misnamed.sql":              "CREATE TABLE stray (id INT);\n", // MF103
	})

	findings, err := lint.LintFS(fsys, lint.Options{})
	c.Assert(err, qt.IsNil)
	c.Assert(rulesOf(findings), qt.DeepEquals, []string{"MF101", "MF102", "MF103"})
	for _, f := range findings {
		c.Assert(f.Line, qt.Equals, 0, qt.Commentf("file-level findings carry no line: %v", f))
	}
}

// lintOne lints a single-statement up migration (with a paired down file)
// and returns the rule codes that fired.
func lintOne(c *qt.C, sql string) []string {
	return lintOneDialect(c, "", sql)
}

// lintOneDialect is lintOne with an explicit target dialect.
func lintOneDialect(c *qt.C, dialect, sql string) []string {
	c.Helper()
	fsys := fixture(map[string]string{
		"0000000001_x.up.sql":   sql + "\n",
		"0000000001_x.down.sql": "-- restore\n",
	})
	findings, err := lint.LintFS(fsys, lint.Options{Dialect: dialect})
	c.Assert(err, qt.IsNil)
	return rulesOf(findings)
}

func TestLintFS_OptionalKeywordForms(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		// The COLUMN keyword is optional in PostgreSQL and the MySQL family.
		{"drop without COLUMN keyword", "ALTER TABLE users DROP email;", []string{"DS102"}},
		{"drop column if exists", "ALTER TABLE users DROP COLUMN IF EXISTS email;", []string{"DS102"}},
		{"drop if exists without COLUMN", "ALTER TABLE users DROP IF EXISTS email;", []string{"DS102"}},
		{"drop on schema-qualified table", "ALTER TABLE public.users DROP email;", []string{"DS102"}},
		{"modify without COLUMN keyword", "ALTER TABLE users MODIFY name VARCHAR(500);", []string{"DS103", "MY101"}},
		{"change without COLUMN keyword", "ALTER TABLE users CHANGE old_name new_name TEXT;", []string{"DS103", "MY101"}},
		{"alter type without COLUMN keyword", "ALTER TABLE users ALTER email TYPE TEXT;", []string{"DS103"}},
		{"set data type spelling", "ALTER TABLE users ALTER COLUMN price SET DATA TYPE NUMERIC(12,2);", []string{"DS103"}},
		{"rename without COLUMN keyword", "ALTER TABLE users RENAME email TO email_address;", []string{"BC101"}},
		{"standalone rename table", "RENAME TABLE users TO users_archive;", []string{"BC101"}},
		{"mysql rename table without TO", "ALTER TABLE users RENAME users_archive;", []string{"BC101"}},

		// PostgreSQL: ALTER TABLE [ IF EXISTS ] [ ONLY ] name [ * ] — every
		// modifier combination must still anchor the clause scan.
		{"if exists only drop", "ALTER TABLE IF EXISTS ONLY users DROP COLUMN email;", []string{"DS102"}},
		{"if exists only alter type", "ALTER TABLE IF EXISTS ONLY users ALTER COLUMN age TYPE BIGINT;", []string{"DS103"}},
		{"if exists only rename", "ALTER TABLE IF EXISTS ONLY users RENAME COLUMN email TO email_old;", []string{"BC101"}},
		{"descendant asterisk form", "ALTER TABLE users * DROP COLUMN email;", []string{"DS102"}},

		// CONVERT TO CHARACTER SET and its CHARSET synonym rebuild the table.
		{"convert to character set", "ALTER TABLE users CONVERT TO CHARACTER SET utf8mb4;", []string{"MY101"}},
		{"convert to charset synonym", "ALTER TABLE users CONVERT TO CHARSET utf8mb4;", []string{"MY101"}},

		// Non-column DROP clauses and column attributes must stay silent.
		{"drop constraint", "ALTER TABLE users DROP CONSTRAINT uq_users_email;", nil},
		{"drop foreign key", "ALTER TABLE orders DROP FOREIGN KEY fk_orders_user;", nil},
		{"drop primary key", "ALTER TABLE users DROP PRIMARY KEY;", nil},
		{"drop index", "ALTER TABLE users DROP INDEX idx_users_email;", nil},
		{"drop check", "ALTER TABLE users DROP CHECK chk_age;", nil},
		{"drop default attribute", "ALTER TABLE users ALTER COLUMN a DROP DEFAULT;", nil},
		{"drop not null attribute", "ALTER TABLE users ALTER COLUMN a DROP NOT NULL;", nil},
		{"drop identity attribute", "ALTER TABLE users ALTER COLUMN a DROP IDENTITY IF EXISTS;", nil},
		{"drop key", "ALTER TABLE users DROP KEY idx_email;", nil},
		{"drop partition", "ALTER TABLE metrics DROP PARTITION p2024;", nil},
		{"drop system versioning", "ALTER TABLE users DROP SYSTEM VERSIONING;", nil},

		// Columns that happen to be named like keywords are not hazards.
		{"column named type set not null", "ALTER TABLE users ALTER COLUMN type SET NOT NULL;", nil},
		{"quoted column named type", `ALTER TABLE users ALTER COLUMN "type" SET NOT NULL;`, nil},
		{"added column named modify", "ALTER TABLE users ADD COLUMN modify TEXT;", nil},
		{"added column named rename", "ALTER TABLE users ADD COLUMN rename TEXT;", nil},

		// Renames invisible to application code are not BC breaks.
		{"rename index", "ALTER TABLE users RENAME INDEX i1 TO i2;", nil},
		{"rename constraint", "ALTER TABLE users RENAME CONSTRAINT c1 TO c2;", nil},

		// Top-level commas separate clauses; commas in parens do not.
		{"comma-adjacent drop clause", "ALTER TABLE t ADD COLUMN a NUMERIC(10,2),DROP COLUMN b;", []string{"DS102"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			got := lintOne(c, tt.sql)
			if len(tt.want) == 0 {
				c.Assert(got, qt.HasLen, 0, qt.Commentf("%s must be clean", tt.sql))
			} else {
				c.Assert(got, qt.DeepEquals, tt.want, qt.Commentf("sql: %s", tt.sql))
			}
		})
	}
}

func TestLintFS_CommentsAndLiteralsDoNotHideOrFakeHazards(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{"comment glued between keywords", "DROP/*hidden*/TABLE users;", []string{"DS101"}},
		{"comment inside alter clause", "ALTER TABLE users DROP/*hidden*/COLUMN email;", []string{"DS102"}},
		{"hazard text inside a string literal", "ALTER TABLE t ADD COLUMN note TEXT DEFAULT 'use DROP COLUMN x';", nil},
		{"concurrently in a literal is no guard", "CREATE INDEX i ON t (a) WHERE b = 'CONCURRENTLY';", []string{"PG101"}},
		{"create index concurrently is safe", "CREATE UNIQUE INDEX CONCURRENTLY uq ON t (a);", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			got := lintOne(c, tt.sql)
			if len(tt.want) == 0 {
				c.Assert(got, qt.HasLen, 0, qt.Commentf("%s must be clean", tt.sql))
			} else {
				c.Assert(got, qt.DeepEquals, tt.want, qt.Commentf("sql: %s", tt.sql))
			}
		})
	}
}

func TestLintFS_DialectAwareScanning(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
		want    []string
	}{
		// Under standard_conforming_strings (the PostgreSQL default since
		// 9.1) a backslash is a literal character, so a trailing backslash
		// must not swallow the closing quote and everything after it.
		{"postgres trailing backslash literal", "postgres",
			"INSERT INTO paths (prefix) VALUES ('C:\\');\nALTER TABLE users DROP COLUMN email;", []string{"DS102"}},
		{"postgres like escape literal", "postgres",
			"ALTER TABLE t ADD CONSTRAINT chk CHECK (code NOT LIKE '%\\_%' ESCAPE '\\');\nALTER TABLE users DROP COLUMN email;", []string{"DS102"}},
		// MySQL treats backslash as an escape: \' stays inside the literal.
		{"mysql backslash-escaped quote", "mysql",
			"INSERT INTO notes (t) VALUES ('it\\'s; fine');\nALTER TABLE users DROP COLUMN email;", []string{"DS102"}},

		// # line comments are MySQL/MariaDB syntax and must neither hide
		// hazards nor leak decoy text into statements.
		{"mysql hash comment before statement", "mysql",
			"# drop unused column\nALTER TABLE users DROP COLUMN email;", []string{"DS102"}},
		{"mysql hash comment inside statement", "mysql",
			"ALTER TABLE users\n# remove the legacy column\nDROP COLUMN email;", []string{"DS102"}},
		{"mysql hash comment decoys are inert", "mysql",
			"# decoy; DROP TABLE x;\nCREATE TABLE t (id INT);", nil},
		// In PostgreSQL # is an operator, not a comment starter.
		{"postgres hash operator in index expression", "postgres",
			"CREATE INDEX idx ON t ((data #>> '{a}'));", []string{"PG101"}},

		// MySQL executable comments are real SQL to the server.
		{"mysql executable comment hides real ddl", "mysql",
			"/*!50003 ALTER TABLE users DROP COLUMN email */;", []string{"DS102"}},

		// PostgreSQL block comments nest.
		{"postgres nested block comment", "postgres",
			"/* cleanup /* legacy */ block */\nALTER TABLE users DROP COLUMN email;", []string{"DS102"}},

		// Encoding and termination edge cases.
		{"utf8 bom before first statement", "", "\uFEFFDROP TABLE users;", []string{"DS101"}},
		{"final statement without semicolon", "", "DROP TABLE users", []string{"DS101"}},
		{"unicode table name", "", "ALTER TABLE пользователи DROP COLUMN email;", []string{"DS102"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			got := lintOneDialect(c, tt.dialect, tt.sql)
			if len(tt.want) == 0 {
				c.Assert(got, qt.HasLen, 0, qt.Commentf("%s must be clean", tt.sql))
			} else {
				c.Assert(got, qt.DeepEquals, tt.want, qt.Commentf("sql: %s", tt.sql))
			}
		})
	}
}

func TestLintFS_ScanningKeepsLineNumbers(t *testing.T) {
	c := qt.New(t)

	fsys := fixture(map[string]string{
		"0000000001_x.up.sql": "# leading comment with a decoy; DROP TABLE decoy;\n" +
			"CREATE TABLE t (id INT);\n" +
			"ALTER TABLE users\n" +
			"# mid-statement comment\n" +
			"DROP COLUMN email;\n",
		"0000000001_x.down.sql": "-- restore\n",
	})

	findings, err := lint.LintFS(fsys, lint.Options{Dialect: "mysql"})
	c.Assert(err, qt.IsNil)
	c.Assert(findings, qt.HasLen, 1)
	c.Assert(findings[0].Rule, qt.Equals, "DS102")
	c.Assert(findings[0].Line, qt.Equals, 3,
		qt.Commentf("the finding points at the ALTER TABLE start, not the comment"))
}

func TestLintFS_SameFileCreatedTablesAreExempt(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
		want    []string
	}{
		// The create-staging/backfill/drop pattern destroys no existing data.
		{"drop of table created in same file", "postgres",
			"CREATE TEMPORARY TABLE tmp_backfill AS SELECT id FROM users;\nDROP TABLE tmp_backfill;", nil},
		{"drop if exists of created table", "postgres",
			"CREATE TABLE staging (id INT);\nDROP TABLE IF EXISTS staging;", nil},
		{"drop of pre-existing table still fires", "postgres",
			"CREATE TABLE staging (id INT);\nDROP TABLE users;", []string{"DS101"}},
		{"drop before create still fires", "postgres",
			"DROP TABLE staging;\nCREATE TABLE staging (id INT);", []string{"DS101"}},
		{"multi-table drop with one pre-existing fires", "postgres",
			"CREATE TABLE staging (id INT);\nDROP TABLE staging, users;", []string{"DS101"}},

		// An index on a table created two statements earlier is built on an
		// empty table — no lock hazard.
		{"index on table created in same file", "postgres",
			"CREATE TABLE orders (id BIGSERIAL PRIMARY KEY, user_id BIGINT);\nCREATE INDEX idx_orders_user ON orders (user_id);", nil},
		{"index on schema-qualified created table", "postgres",
			"CREATE TABLE app.orders (id INT);\nCREATE UNIQUE INDEX uq ON app.orders (id);", nil},
		{"index on pre-existing table still fires", "postgres",
			"CREATE TABLE orders (id INT);\nCREATE INDEX idx_users_email ON users (email);", []string{"PG101"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			got := lintOneDialect(c, tt.dialect, tt.sql)
			if len(tt.want) == 0 {
				c.Assert(got, qt.HasLen, 0, qt.Commentf("%s must be clean", tt.sql))
			} else {
				c.Assert(got, qt.DeepEquals, tt.want, qt.Commentf("sql: %s", tt.sql))
			}
		})
	}
}

func TestLintFS_MY101PinnedOnlineDDLIsExempt(t *testing.T) {
	c := qt.New(t)

	// Pinned ALGORITHM/LOCK make the server refuse a blocking rebuild, so
	// the lock hazard cannot occur; the lossy-type-change warning stays.
	got := lintOneDialect(c, "mysql",
		"ALTER TABLE users MODIFY COLUMN bio VARCHAR(500) NOT NULL, ALGORITHM=INPLACE, LOCK=NONE;")
	c.Assert(got, qt.DeepEquals, []string{"DS103"})

	// The = is optional in the MySQL grammar.
	got = lintOneDialect(c, "mysql",
		"ALTER TABLE users MODIFY COLUMN bio VARCHAR(500), ALGORITHM INPLACE;")
	c.Assert(got, qt.DeepEquals, []string{"DS103"})

	// ALGORITHM=COPY pins the blocking path; MY101 must still fire.
	got = lintOneDialect(c, "mysql",
		"ALTER TABLE users MODIFY COLUMN bio VARCHAR(500), ALGORITHM=COPY;")
	c.Assert(got, qt.DeepEquals, []string{"DS103", "MY101"})
}

func TestLintFS_NestedDirectoriesAreLinted(t *testing.T) {
	c := qt.New(t)

	// The migrator's FSMigrationProvider discovers migrations recursively,
	// so lint must walk subdirectories too.
	fsys := fixture(map[string]string{
		"sub/0000000001_a.up.sql":   "DROP TABLE users;\n",
		"sub/0000000001_a.down.sql": "-- restore\n",
	})

	findings, err := lint.LintFS(fsys, lint.Options{PathPrefix: "db/migrations"})
	c.Assert(err, qt.IsNil)
	c.Assert(findings, qt.HasLen, 1)
	c.Assert(findings[0].Rule, qt.Equals, "DS101")
	c.Assert(findings[0].File, qt.Equals, "db/migrations/sub/0000000001_a.up.sql")
	c.Assert(findings[0].Line, qt.Equals, 1)
}

func TestLintFS_UpSuffixFallbackScansMalformedVersions(t *testing.T) {
	c := qt.New(t)

	// A .up.sql file whose version prefix the migrator rejects still gets
	// hazard scanning via the IsUp suffix fallback — the author clearly
	// meant it as an up migration, and MF103 explains why it will not run.
	fsys := fixture(map[string]string{
		"001_bad_version.up.sql":   "DROP TABLE users;\n",
		"001_bad_version.down.sql": "-- restore\n",
	})

	findings, err := lint.LintFS(fsys, lint.Options{})
	c.Assert(err, qt.IsNil)
	c.Assert(rulesOf(findings), qt.DeepEquals, []string{"MF103", "MF103", "DS101"},
		qt.Commentf("naming warnings for both files plus the hazard in the up file; got %v", findings))
}

func TestLintFS_SuffixlessNamesFollowTheMigrator(t *testing.T) {
	c := qt.New(t)

	// Since the migrator's name regexp was fixed (#245), a description
	// merely ending in up/down is not a migration: the migrator skips the
	// file, so lint reports the naming problem instead of scanning
	// statements that will never run.
	fsys := fixture(map[string]string{
		"0000000001_cleanup.sql":  "DROP TABLE users;\n",
		"0000000002_teardown.sql": "DROP TABLE audit;\n",
	})

	findings, err := lint.LintFS(fsys, lint.Options{})
	c.Assert(err, qt.IsNil)

	c.Assert(rulesOf(findings), qt.DeepEquals, []string{"MF103", "MF103"},
		qt.Commentf("both files are invisible to the migrator: naming warnings only; got %v", findings))
	for _, f := range findings {
		c.Assert(f.Message, qt.Contains, "the migrator will not pick it up")
	}
}

func TestLintFS_CaseVariantSQLFilesGetNamingWarning(t *testing.T) {
	c := qt.New(t)

	fsys := fixture(map[string]string{
		"0000000001_x.up.sql":   "CREATE TABLE t (id INT);\n",
		"0000000001_x.down.sql": "DROP TABLE t;\n",
		"0000000002_y.UP.SQL":   "DROP TABLE users;\n",
	})

	findings, err := lint.LintFS(fsys, lint.Options{})
	c.Assert(err, qt.IsNil)
	c.Assert(rulesOf(findings), qt.DeepEquals, []string{"MF103"},
		qt.Commentf("a case-variant file the migrator will not run earns a naming warning instead of vanishing"))
}

func TestLintFS_DialectGating(t *testing.T) {
	c := qt.New(t)

	fsys := fixture(map[string]string{
		"0000000001_mixed.up.sql": `CREATE INDEX idx ON users (email);
ALTER TABLE users MODIFY COLUMN email VARCHAR(64);
`,
		"0000000001_mixed.down.sql": "-- restore\n",
	})

	// postgres: PG rules fire, MY rules do not (DS103 is generic and stays).
	findings, err := lint.LintFS(fsys, lint.Options{Dialect: "postgres"})
	c.Assert(err, qt.IsNil)
	c.Assert(rulesOf(findings), qt.DeepEquals, []string{"PG101", "DS103"})

	// mariadb: MY rules fire, PG rules do not.
	findings, err = lint.LintFS(fsys, lint.Options{Dialect: "mariadb"})
	c.Assert(err, qt.IsNil)
	c.Assert(rulesOf(findings), qt.DeepEquals, []string{"DS103", "MY101"})

	// mysql gates identically to mariadb.
	findings, err = lint.LintFS(fsys, lint.Options{Dialect: "mysql"})
	c.Assert(err, qt.IsNil)
	c.Assert(rulesOf(findings), qt.DeepEquals, []string{"DS103", "MY101"})

	// empty dialect: everything fires.
	findings, err = lint.LintFS(fsys, lint.Options{})
	c.Assert(err, qt.IsNil)
	c.Assert(rulesOf(findings), qt.DeepEquals, []string{"PG101", "DS103", "MY101"})
}

func TestLintFS_DisabledRulesAndFamilies(t *testing.T) {
	c := qt.New(t)

	fsys := fixture(map[string]string{
		"0000000001_bad.up.sql": `DROP TABLE a;
ALTER TABLE b DROP COLUMN c;
CREATE INDEX i ON b (c);
`,
		"0000000001_bad.down.sql": "-- restore\n",
	})

	// Disable one exact code.
	findings, err := lint.LintFS(fsys, lint.Options{Disabled: []string{"DS101"}})
	c.Assert(err, qt.IsNil)
	c.Assert(rulesOf(findings), qt.DeepEquals, []string{"DS102", "PG101"})

	// Disable a whole family by prefix.
	findings, err = lint.LintFS(fsys, lint.Options{Disabled: []string{"DS"}})
	c.Assert(err, qt.IsNil)
	c.Assert(rulesOf(findings), qt.DeepEquals, []string{"PG101"})

	// A stray empty entry must not disable everything.
	findings, err = lint.LintFS(fsys, lint.Options{Disabled: []string{""}})
	c.Assert(err, qt.IsNil)
	c.Assert(rulesOf(findings), qt.DeepEquals, []string{"DS101", "DS102", "PG101"})
}

func TestLintFS_DollarQuotedBodiesDoNotSplitStatements(t *testing.T) {
	c := qt.New(t)

	fsys := fixture(map[string]string{
		"0000000001_fn.up.sql": `CREATE FUNCTION noop() RETURNS void AS $ptah$
BEGIN
    -- DROP TABLE decoy; inside a dollar-quoted body and a comment
    PERFORM 1;
END;
$ptah$ LANGUAGE plpgsql;
`,
		"0000000001_fn.down.sql": "DROP FUNCTION noop();\n",
	})

	findings, err := lint.LintFS(fsys, lint.Options{})
	c.Assert(err, qt.IsNil)
	c.Assert(findings, qt.HasLen, 0,
		qt.Commentf("statements inside dollar-quoted bodies must not trigger rules; got: %v", findings))
}

func TestLintFS_PathPrefixAppearsInFindings(t *testing.T) {
	c := qt.New(t)

	fsys := fixture(map[string]string{
		"0000000001_bad.up.sql":   "DROP TABLE a;\n",
		"0000000001_bad.down.sql": "-- restore\n",
	})

	findings, err := lint.LintFS(fsys, lint.Options{PathPrefix: "db/migrations"})
	c.Assert(err, qt.IsNil)
	c.Assert(findings, qt.HasLen, 1)
	c.Assert(findings[0].File, qt.Equals, "db/migrations/0000000001_bad.up.sql")
}

func TestLintFS_NoMigrationFilesIsAnError(t *testing.T) {
	c := qt.New(t)

	_, err := lint.LintFS(fixture(map[string]string{"README.md": "not sql"}), lint.Options{})
	c.Assert(err, qt.ErrorMatches, "no \\*\\.sql migration files found")
}

func TestLoadConfig(t *testing.T) {
	c := qt.New(t)

	dir := t.TempDir()
	c.Assert(writeFile(dir+"/.ptah-lint.yaml", "dialect: postgres\ndisabled-rules:\n  - MF\n  - BC101\n"), qt.IsNil)

	cfg, err := lint.LoadConfig(dir + "/.ptah-lint.yaml")
	c.Assert(err, qt.IsNil)
	c.Assert(cfg.Dialect, qt.Equals, "postgres")
	c.Assert(cfg.DisabledRules, qt.DeepEquals, []string{"MF", "BC101"})

	// A missing file at the conventional location is not an error.
	cfg, err = lint.LoadConfig(dir + "/nope.yaml")
	c.Assert(err, qt.IsNil)
	c.Assert(cfg.Dialect, qt.Equals, "")
	c.Assert(cfg.DisabledRules, qt.HasLen, 0)

	// A malformed file is.
	c.Assert(writeFile(dir+"/broken.yaml", "dialect: [not, a, string"), qt.IsNil)
	_, err = lint.LoadConfig(dir + "/broken.yaml")
	c.Assert(err, qt.ErrorMatches, "failed to parse lint config .*")
}

func TestRules_EveryRuleHasCodeTitleAndOneChecker(t *testing.T) {
	c := qt.New(t)

	seen := map[string]bool{}
	for _, rule := range lint.Rules() {
		c.Assert(rule.Code, qt.Not(qt.Equals), "")
		c.Assert(rule.Title, qt.Not(qt.Equals), "")
		c.Assert(seen[rule.Code], qt.IsFalse, qt.Commentf("duplicate rule code %s", rule.Code))
		seen[rule.Code] = true
		oneChecker := (rule.CheckStatement != nil) != (rule.CheckFile != nil)
		c.Assert(oneChecker, qt.IsTrue, qt.Commentf("rule %s must set exactly one checker", rule.Code))
		c.Assert(rule.Severity == lint.SeverityWarning || rule.Severity == lint.SeverityError, qt.IsTrue)
	}
}

func writeFile(path, content string) error {
	return os.WriteFile(path, []byte(content), 0o600)
}
