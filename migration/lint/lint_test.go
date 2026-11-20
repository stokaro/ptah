package lint_test

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/engine"
	"ptah.run/internal/builtintest"
	"ptah.run/migration/lint"
	"ptah.run/migration/migrationfile"
)

// fixture builds an in-memory migrations directory.
func fixture(files map[string]string) fstest.MapFS {
	fsys := fstest.MapFS{}
	for name, content := range files {
		fsys[name] = &fstest.MapFile{Data: []byte(content)}
	}
	return fsys
}

// findingSites returns each finding as file:line:rule.
func findingSites(findings []lint.Finding) []string {
	sites := make([]string, 0, len(findings))
	for _, finding := range findings {
		sites = append(sites, fmt.Sprintf("%s:%d:%s", finding.File, finding.Line, finding.Rule))
	}
	return sites
}

// dsSites keeps the sites of the DS family.
func dsSites(sites []string) []string {
	return slices.DeleteFunc(slices.Clone(sites), func(site string) bool {
		return !strings.Contains(site, ":DS")
	})
}

// rulesOf collects the rule codes of findings, with duplicates.
func rulesOf(findings []lint.Finding) []string {
	codes := make([]string, 0, len(findings))
	for _, f := range findings {
		codes = append(codes, f.Rule)
	}
	return codes
}

// sortedRulesOf is rulesOf in sorted order, for an assertion about which rules
// fire that does not depend on the order the rules are registered in.
func sortedRulesOf(findings []lint.Finding) []string {
	return slices.Sorted(slices.Values(rulesOf(findings)))
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

	findings, err := lint.LintFS(c.Context(), builtintest.Runtime(), fsys, lint.Options{})
	c.Assert(err, qt.IsNil)

	c.Assert(rulesOf(findings), qt.DeepEquals, []string{
		"rollout/drop-table",
		"destructive/drop-table", // DROP TABLE audit_log
		"rollout/drop-column",
		"destructive/drop-column",          // DROP COLUMN legacy
		"rollout/rename",                   // RENAME COLUMN
		"data-dependent/alter-column-type", // MODIFY COLUMN (lossy)
		"locking/blocking-alter-table",     // MODIFY COLUMN (lock-heavy)
		"data-dependent/add-unique-index",  // CREATE UNIQUE INDEX over existing rows
		"locking/create-index",             // CREATE UNIQUE INDEX without CONCURRENTLY
		"transaction/add-enum-value",       // ALTER TYPE ADD VALUE
	})

	// Line numbers point at the offending statements, not the file head:
	// the comment on line 1 (with its decoy semicolon) shifts DROP TABLE to
	// line 2 and everything after accordingly.
	byRule := make(map[string]lint.Finding)
	for _, f := range findings {
		byRule[f.Rule] = f
	}
	c.Assert(byRule["destructive/drop-table"].Line, qt.Equals, 2)
	c.Assert(byRule["destructive/drop-column"].Line, qt.Equals, 3)
	c.Assert(byRule["rollout/rename"].Line, qt.Equals, 4)
	c.Assert(byRule["transaction/add-enum-value"].Line, qt.Equals, 7)
	c.Assert(byRule["destructive/drop-table"].Severity, qt.Equals, lint.SeverityError)
	c.Assert(byRule["locking/create-index"].Severity, qt.Equals, lint.SeverityWarning)
}

func TestLintFS_InvalidProgrammaticRuleSeverityFailsFast(t *testing.T) {
	c := qt.New(t)

	_, err := lint.LintFS(c.Context(), builtintest.Runtime(), fixture(map[string]string{
		"0000000001_init.up.sql":   "CREATE TABLE users (id INTEGER);\n",
		"0000000001_init.down.sql": "DROP TABLE users;\n",
	}), lint.Options{
		RuleConfigs: map[string]lint.RuleConfig{
			"destructive/drop-table": {Severity: lint.Severity("fatal")},
		},
	})

	c.Assert(err, qt.ErrorMatches, `rule destructive/drop-table has unsupported severity "fatal"`)
}

func TestLintFS_InvalidProgrammaticRuleSelectorFailsFast(t *testing.T) {
	c := qt.New(t)

	_, err := lint.LintFS(c.Context(), builtintest.Runtime(), fixture(map[string]string{
		"0000000001_init.up.sql":   "DROP TABLE users;\n",
		"0000000001_init.down.sql": "CREATE TABLE users (id INTEGER);\n",
	}), lint.Options{
		RuleConfigs: map[string]lint.RuleConfig{
			"ds101": {Severity: lint.SeverityError},
		},
	})

	c.Assert(err, qt.ErrorMatches, `rule selector "ds101" must start with an uppercase ASCII letter.*`)
}

func TestLintFS_WhitespacePaddedProgrammaticRuleSelectorFailsFast(t *testing.T) {
	c := qt.New(t)

	_, err := lint.LintFS(c.Context(), builtintest.Runtime(), fixture(map[string]string{
		"0000000001_init.up.sql":   "DROP TABLE users;\n",
		"0000000001_init.down.sql": "CREATE TABLE users (id INTEGER);\n",
	}), lint.Options{Disabled: []string{" destructive/drop-table "}})

	c.Assert(err, qt.ErrorMatches, `rule selector " destructive/drop-table " must start with an uppercase ASCII letter.*`)
}

func TestLintFS_UnknownProgrammaticRuleSelectorFailsFast(t *testing.T) {
	c := qt.New(t)

	_, err := lint.LintFS(c.Context(), builtintest.Runtime(), fixture(map[string]string{
		"0000000001_init.up.sql":   "DROP TABLE users;\n",
		"0000000001_init.down.sql": "CREATE TABLE users (id INTEGER);\n",
	}), lint.Options{
		RuleConfigs: map[string]lint.RuleConfig{
			"design/zz404": {Severity: lint.SeverityError},
		},
	})

	c.Assert(err, qt.ErrorMatches, `rule selector "design/zz404" does not match any registered rule`)
}

func TestLintFS_VersionsRestrictsFindings(t *testing.T) {
	c := qt.New(t)

	fsys := fixture(map[string]string{
		"0000000001_old.up.sql":    "DROP TABLE old_data;\n",
		"0000000001_old.down.sql":  "CREATE TABLE old_data (id INT);\n",
		"0000000002_next.up.sql":   "ALTER TABLE users DROP COLUMN legacy;\n",
		"0000000002_next.down.sql": "ALTER TABLE users ADD COLUMN legacy TEXT;\n",
	})

	findings, err := lint.LintFS(c.Context(), builtintest.Runtime(), fsys, lint.Options{
		Disabled: []string{"file-form", "rollout", "locking", "rewrite", "transaction"},
		Selection: lint.VersionSelection{
			Versions:   []int64{2},
			Restricted: true,
		},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(rulesOf(findings), qt.DeepEquals, []string{"destructive/drop-column"})
	c.Assert(findings[0].File, qt.Equals, "0000000002_next.up.sql")
}

func TestLintFS_CleanMigrationHasNoFindings(t *testing.T) {
	c := qt.New(t)

	fsys := fixture(map[string]string{
		"0000000001_create_users.up.sql":   "CREATE TABLE users (id SERIAL PRIMARY KEY, email VARCHAR(255) NOT NULL);\n",
		"0000000001_create_users.down.sql": "DROP TABLE IF EXISTS users;\n",
	})

	findings, err := lint.LintFS(c.Context(), builtintest.Runtime(), fsys, lint.Options{Dialect: "mysql"})
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

	findings, err := lint.LintFS(c.Context(), builtintest.Runtime(), fsys, lint.Options{})
	c.Assert(err, qt.IsNil)
	c.Assert(findings, qt.HasLen, 0, qt.Commentf("got: %v", findings))
}

func TestLintFS_MigrationFormRules(t *testing.T) {
	c := qt.New(t)

	fsys := fixture(map[string]string{
		"0000000001_orphan.up.sql":  "CREATE TABLE t (id INT);\n",         // file-form/missing-down: no down
		"0000000002_empty.up.sql":   "-- only comments; nothing to run\n", // file-form/empty-migration
		"0000000002_empty.down.sql": "-- nothing\n",
		"misnamed.sql":              "CREATE TABLE stray (id INT);\n", // file-form/refused-file: the migrator refuses the directory
		"shared/misnamed.sql":       "CREATE TABLE stray (id INT);\n", // file-form/file-name: a file of another directory
	})

	findings, err := lint.LintFS(c.Context(), builtintest.Runtime(), fsys, lint.Options{})
	c.Assert(err, qt.IsNil)
	c.Assert(sortedRulesOf(findings), qt.DeepEquals, []string{"file-form/empty-migration", "file-form/file-name", "file-form/missing-down", "file-form/refused-file"})
	for _, f := range findings {
		c.Assert(f.Line, qt.Equals, 0, qt.Commentf("file-level findings carry no line: %v", f))
	}
}

func TestLintFS_PairsByVersionLikeMigrator(t *testing.T) {
	c := qt.New(t)

	// The migrator pairs an up and a down by their shared version prefix,
	// regardless of description, so lint must not raise file-form/missing-down (missing down)
	// when the counterpart down exists for the same version under a
	// different description.
	fsys := fixture(map[string]string{
		"0000000001_create_users.up.sql":         "CREATE TABLE users (id INT);\n",
		"0000000001_create_users_table.down.sql": "DROP TABLE users;\n",
	})

	findings, err := lint.LintFS(c.Context(), builtintest.Runtime(), fsys, lint.Options{})
	c.Assert(err, qt.IsNil)
	c.Assert(findings, qt.HasLen, 0,
		qt.Commentf("version 1 has both an up and a down; no file-form/missing-down expected; got %v", findings))
}

func TestLintFS_AtlasImportedFlywayRepeatableIsContentLinted(t *testing.T) {
	c := qt.New(t)

	fsys := fixture(map[string]string{
		"2_baseline.sql": "CREATE TABLE users (id INT);\n",
		"3R_views.sql":   "DROP TABLE users;\n",
	})

	// The Atlas revision table keeps the version 3R, which Ptah's table, which
	// records numbers, refuses the directory for.
	findings, err := lint.LintFS(c.Context(), builtintest.Runtime(), fsys, lint.Options{AtlasRevisionTable: true})
	c.Assert(err, qt.IsNil)
	c.Assert(rulesOf(findings), qt.DeepEquals, []string{"destructive/drop-table", "rollout/drop-table"})
	c.Assert(findings[0].File, qt.Equals, "3R_views.sql")
	c.Assert(findings[0].Line, qt.Equals, 1)
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
	findings, err := lint.LintFS(c.Context(), builtintest.Runtime(), fsys, lint.Options{Dialect: dialect})
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
		{"drop without COLUMN keyword", "ALTER TABLE users DROP email;", []string{"destructive/drop-column", "rollout/drop-column"}},
		{"drop column if exists", "ALTER TABLE users DROP COLUMN IF EXISTS email;", []string{"destructive/drop-column", "rollout/drop-column"}},
		{"drop if exists without COLUMN", "ALTER TABLE users DROP IF EXISTS email;", []string{"destructive/drop-column", "rollout/drop-column"}},
		{"drop on schema-qualified table", "ALTER TABLE public.users DROP email;", []string{"destructive/drop-column", "rollout/drop-column"}},
		{"modify without COLUMN keyword", "ALTER TABLE users MODIFY name VARCHAR(500);", []string{"data-dependent/alter-column-type", "locking/blocking-alter-table"}},
		{"change without COLUMN keyword", "ALTER TABLE users CHANGE old_name new_name TEXT;", []string{"data-dependent/alter-column-type", "locking/blocking-alter-table"}},
		{"alter type without COLUMN keyword", "ALTER TABLE users ALTER email TYPE TEXT;", []string{"data-dependent/alter-column-type"}},
		{"set data type spelling", "ALTER TABLE users ALTER COLUMN price SET DATA TYPE NUMERIC(12,2);", []string{"data-dependent/alter-column-type"}},
		{"rename without COLUMN keyword", "ALTER TABLE users RENAME email TO email_address;", []string{"rollout/rename"}},
		{"standalone rename table", "RENAME TABLE users TO users_archive;", []string{"rollout/rename"}},
		{"mysql rename table without TO", "ALTER TABLE users RENAME users_archive;", []string{"rollout/rename"}},

		// PostgreSQL: ALTER TABLE [ IF EXISTS ] [ ONLY ] name [ * ] — every
		// modifier combination must still anchor the clause scan.
		{"if exists only drop", "ALTER TABLE IF EXISTS ONLY users DROP COLUMN email;", []string{"destructive/drop-column", "rollout/drop-column"}},
		{"if exists only alter type", "ALTER TABLE IF EXISTS ONLY users ALTER COLUMN age TYPE BIGINT;", []string{"data-dependent/alter-column-type"}},
		{"if exists only rename", "ALTER TABLE IF EXISTS ONLY users RENAME COLUMN email TO email_old;", []string{"rollout/rename"}},
		{"descendant asterisk form", "ALTER TABLE users * DROP COLUMN email;", []string{"destructive/drop-column", "rollout/drop-column"}},

		// CONVERT TO CHARACTER SET and its CHARSET synonym rebuild the table.
		{"convert to character set", "ALTER TABLE users CONVERT TO CHARACTER SET utf8mb4;", []string{"locking/blocking-alter-table"}},
		{"convert to charset synonym", "ALTER TABLE users CONVERT TO CHARSET utf8mb4;", []string{"locking/blocking-alter-table"}},

		// Data-protection drops are destructive; storage-only drops stay silent.
		// The untyped ANSI DROP CONSTRAINT <name> form stays integrity/drop-untyped-constraint; the typed
		// MySQL-family forms get their per-type CD codes without double-flagging.
		{"drop named constraint", "ALTER TABLE users DROP CONSTRAINT uq_users_email;", []string{"integrity/drop-untyped-constraint"}},
		{"drop foreign key", "ALTER TABLE orders DROP FOREIGN KEY fk_orders_user;", []string{"integrity/drop-foreign-key"}},
		// Without a dialect every family runs, so the MySQL copy rule reports
		// the drop beside the lost guarantee.
		{"drop primary key", "ALTER TABLE users DROP PRIMARY KEY;", []string{"integrity/drop-primary-key", "rewrite/drop-primary-key"}},
		{"drop index", "ALTER TABLE users DROP INDEX idx_users_email;", nil},
		{"drop check", "ALTER TABLE users DROP CHECK chk_age;", []string{"integrity/drop-check"}},
		{"drop foreign key schema-qualified", "ALTER TABLE shop.orders DROP FOREIGN KEY fk_orders_user;", []string{"integrity/drop-foreign-key"}},
		{"drop foreign key if exists", "ALTER TABLE orders DROP FOREIGN KEY IF EXISTS fk_orders_user;", []string{"integrity/drop-foreign-key"}},
		{"drop named constraint cascade", "ALTER TABLE users DROP CONSTRAINT uq_users_email CASCADE;", []string{"integrity/drop-untyped-constraint"}},
		{"drop default attribute", "ALTER TABLE users ALTER COLUMN a DROP DEFAULT;", nil},
		{"drop not null attribute", "ALTER TABLE users ALTER COLUMN a DROP NOT NULL;", []string{"integrity/drop-not-null"}},
		{"drop identity attribute", "ALTER TABLE users ALTER COLUMN a DROP IDENTITY IF EXISTS;", nil},
		{"drop key", "ALTER TABLE users DROP KEY idx_email;", nil},
		// YDB drops a changefeed, an index and a topic's consumer by name, and
		// none is a column. A column named changefeed dropped without the
		// COLUMN keyword has no name after it, and stays a column drop.
		{"drop changefeed", "ALTER TABLE `app/items` DROP CHANGEFEED `updates`;", nil},
		{"drop changefeed with an unquoted name", "ALTER TABLE items DROP CHANGEFEED updates;", nil},
		{"drop changefeed beside a column", "ALTER TABLE items DROP CHANGEFEED updates, DROP COLUMN note;", []string{"destructive/drop-column", "rollout/drop-column"}},
		{"drop index by name", "ALTER TABLE `app/items` DROP INDEX `by_label`;", nil},
		{"drop consumer", "ALTER TOPIC `app/items/updates` DROP CONSUMER `audit`;", nil},
		{"column named changefeed", "ALTER TABLE items DROP changefeed;", []string{"destructive/drop-column", "rollout/drop-column"}},
		{"column named changefeed with cascade", "ALTER TABLE items DROP changefeed CASCADE;", []string{"destructive/drop-column", "rollout/drop-column"}},
		{"drop partition", "ALTER TABLE metrics DROP PARTITION p2024;", nil},
		// Not a column drop, which is what this block is about, but a hazard of
		// its own: the history goes with the versioning.
		{"drop system versioning", "ALTER TABLE users DROP SYSTEM VERSIONING;", []string{"destructive/drop-system-versioning"}},

		// Columns that happen to be named like keywords are not hazards.
		{"column named type set not null", "ALTER TABLE users ALTER COLUMN type SET NOT NULL;", []string{"locking/set-not-null", "rewrite/set-not-null"}},
		{"quoted column named type", `ALTER TABLE users ALTER COLUMN "type" SET NOT NULL;`, []string{"locking/set-not-null", "rewrite/set-not-null"}},
		{"column named not drop default", "ALTER TABLE users ALTER COLUMN not DROP DEFAULT;", nil},
		{"added column named modify", "ALTER TABLE users ADD COLUMN modify TEXT;", nil},
		{"added column named rename", "ALTER TABLE users ADD COLUMN rename TEXT;", nil},

		// Renames invisible to application code are not BC breaks.
		{"rename index", "ALTER TABLE users RENAME INDEX i1 TO i2;", nil},
		{"rename constraint", "ALTER TABLE users RENAME CONSTRAINT c1 TO c2;", nil},
		{"enum value catalog delete", "DELETE FROM pg_enum WHERE enumlabel = 'archived';", []string{"destructive/remove-enum-value"}},
		{"enum value drop syntax", "ALTER TYPE status DROP VALUE 'archived';", []string{"destructive/remove-enum-value"}},
		{"drop type", "DROP TYPE IF EXISTS status;", []string{"destructive/drop-object"}},
		{"drop extension", "DROP EXTENSION IF EXISTS hstore;", []string{"destructive/drop-object"}},
		{"drop function", "DROP FUNCTION IF EXISTS refresh_user();", []string{"destructive/drop-object"}},
		{"drop role", "DROP ROLE old_role;", []string{"destructive/drop-object"}},
		{"drop policy", "DROP POLICY IF EXISTS tenant_isolation ON accounts;", []string{"destructive/drop-object"}},
		{"truncate table keyword", "TRUNCATE TABLE audit_log;", []string{"destructive/truncate-table"}},
		{"truncate without table keyword", "TRUNCATE audit_log;", []string{"destructive/truncate-table"}},
		{"disable row level security", "ALTER TABLE accounts DISABLE ROW LEVEL SECURITY;", []string{"security/disable-row-level-security"}},
		// NO FORCE leaves row-level security enabled and exempts the owner, so
		// it is security/no-force-row-level-security and not security/disable-row-level-security. FORCE and ENABLE add a protection.
		{"no force row level security", "ALTER TABLE accounts NO FORCE ROW LEVEL SECURITY;", []string{"security/no-force-row-level-security"}},
		{"no force on a qualified table", "ALTER TABLE IF EXISTS ONLY app.accounts NO FORCE ROW LEVEL SECURITY;", []string{"security/no-force-row-level-security"}},
		{"no force beside enable", "ALTER TABLE accounts ENABLE ROW LEVEL SECURITY, NO FORCE ROW LEVEL SECURITY;", []string{"security/no-force-row-level-security"}},
		{"no force beside disable", "ALTER TABLE accounts DISABLE ROW LEVEL SECURITY, NO FORCE ROW LEVEL SECURITY;", []string{"security/disable-row-level-security", "security/no-force-row-level-security"}},
		{"force row level security", "ALTER TABLE accounts FORCE ROW LEVEL SECURITY;", nil},
		{"enable row level security", "ALTER TABLE accounts ENABLE ROW LEVEL SECURITY;", nil},

		// Top-level commas separate clauses; commas in parens do not.
		{"comma-adjacent drop clause", "ALTER TABLE t ADD COLUMN a NUMERIC(10,2),DROP COLUMN b;", []string{"destructive/drop-column", "rollout/drop-column"}},
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
		{"comment glued between keywords", "DROP/*hidden*/TABLE users;", []string{"destructive/drop-table", "rollout/drop-table"}},
		{"comment inside alter clause", "ALTER TABLE users DROP/*hidden*/COLUMN email;", []string{"destructive/drop-column", "rollout/drop-column"}},
		{"hazard text inside a string literal", "ALTER TABLE t ADD COLUMN note TEXT DEFAULT 'use DROP COLUMN x';", nil},
		{"no force inside a comment", "ALTER TABLE accounts /* NO FORCE ROW LEVEL SECURITY */ FORCE ROW LEVEL SECURITY;", nil},
		{"concurrently in a literal is no guard", "CREATE INDEX i ON t (a) WHERE b = 'CONCURRENTLY';", []string{"locking/create-index"}},
		{"create index concurrently requires non-transactional migration", "CREATE UNIQUE INDEX CONCURRENTLY uq ON t (a);", []string{"data-dependent/add-unique-index", "transaction/concurrent-index"}},
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

func TestLintFS_AtlasAnalyzerCatalogHazards(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
		want    []string
	}{
		{"drop schema", "postgres", "DROP SCHEMA s;", []string{"destructive/drop-object"}},
		{"add non-nullable column without default", "postgres", "ALTER TABLE t ADD COLUMN c INT NOT NULL;", []string{"data-dependent/add-not-null-column"}},
		{"add unique constraint", "postgres", "ALTER TABLE t ADD CONSTRAINT u UNIQUE (email);", []string{"data-dependent/add-unique-index", "locking/add-unique-constraint"}},
		{"drop index without concurrently", "postgres", "DROP INDEX idx;", []string{"locking/drop-index"}},
		// The discriminating partner for the row above: locking/drop-index must stay silent
		// on the spelling Ptah now generates for a concurrent-index rollback.
		// Without this row a rule that fired on every DROP INDEX would still
		// pass the table. transaction/concurrent-index is the correct residual finding — this fixture
		// has no no_transaction directive, and CONCURRENTLY cannot run inside
		// the migration transaction.
		{"drop index concurrently", "postgres", "DROP INDEX CONCURRENTLY idx;", []string{"transaction/concurrent-index"}},
		{"add primary key", "postgres", "ALTER TABLE t ADD PRIMARY KEY (id);", []string{"locking/add-primary-key"}},
		{"volatile default", "postgres", "ALTER TABLE t ADD COLUMN c UUID DEFAULT gen_random_uuid();", []string{"rewrite/volatile-default"}},
		{"set not null", "postgres", "ALTER TABLE t ALTER COLUMN c SET NOT NULL;", []string{"locking/set-not-null"}},
		{"add a named not null", "postgres", `ALTER TABLE t ADD CONSTRAINT "nn" NOT NULL c;`, []string{"locking/set-not-null"}},
		{"add an unnamed not null", "postgres", "ALTER TABLE t ADD NOT NULL c;", []string{"locking/set-not-null"}},
		{"a named not null beside another clause", "postgres", "ALTER TABLE t ALTER COLUMN b DROP DEFAULT, ADD CONSTRAINT nn NOT NULL c;", []string{"locking/set-not-null"}},
		{"a check that reads is not null", "postgres", "ALTER TABLE t ADD CONSTRAINT ck CHECK (c IS NOT NULL) NOT VALID;", make([]string, 0)},
		{"add check", "postgres", "ALTER TABLE t ADD CONSTRAINT ck CHECK (id > 0);", []string{"locking/add-check"}},
		{"add foreign key", "postgres", "ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY (p_id) REFERENCES p (id);", []string{"locking/add-foreign-key"}},
		{"set unlogged", "postgres", "ALTER TABLE t SET UNLOGGED;", []string{"rewrite/set-persistence"}},
		{"create trigger", "postgres", "CREATE TRIGGER tr BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION f();", []string{"locking/create-trigger"}},
		{"stored generated column", "postgres", "ALTER TABLE t ADD COLUMN g INT GENERATED ALWAYS AS (c * 2) STORED;", []string{"rewrite/add-stored-generated-column"}},
		{"identity column", "postgres", "ALTER TABLE t ADD COLUMN n INT GENERATED ALWAYS AS IDENTITY;", []string{"rewrite/add-identity-column"}},
		{"access method", "postgres", "ALTER TABLE t SET ACCESS METHOD heap2;", []string{"rewrite/set-access-method"}},
		{"column alignment", "postgres", "CREATE TABLE t (a BOOLEAN, b BIGINT, c BOOLEAN, d BIGINT);", []string{"design/column-alignment"}},
		{"mysql inline references", "mysql", "ALTER TABLE t ADD COLUMN p_id INT REFERENCES p (id);", []string{"side-effect/inline-reference"}},
		{"mysql add foreign key", "mysql", "ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY (p_id) REFERENCES p (id);", []string{"locking/add-foreign-key"}},
		{"mysql add primary key", "mysql", "ALTER TABLE t ADD PRIMARY KEY (id);", []string{"rewrite/add-primary-key"}},
		{"mysql fulltext index", "mysql", "ALTER TABLE t ADD FULLTEXT INDEX ft (c);", []string{"locking/add-fulltext-index"}},
		{"mysql spatial index", "mysql", "ALTER TABLE t ADD SPATIAL INDEX sp (g);", []string{"locking/add-spatial-index"}},
		{"sqlite set not null", "sqlite", "ALTER TABLE t ALTER COLUMN c SET NOT NULL;", []string{"rewrite/set-not-null"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			got := lintOneDialect(c, tt.dialect, tt.sql)
			c.Assert(got, qt.DeepEquals, tt.want, qt.Commentf("sql: %s", tt.sql))
		})
	}
}

func TestLintFS_TransactionHazards(t *testing.T) {
	c := qt.New(t)

	fsys := fixture(map[string]string{
		"0000000001_x.up.sql": `CREATE INDEX CONCURRENTLY idx ON t (id);
ALTER TABLE t ADD COLUMN c INT;
`,
		"0000000001_x.down.sql": "-- restore\n",
	})
	findings, err := lint.LintFS(c.Context(), builtintest.Runtime(), fsys, lint.Options{Dialect: "postgres"})
	c.Assert(err, qt.IsNil)
	c.Assert(rulesOf(findings), qt.DeepEquals, []string{"transaction/concurrent-index", "transaction/mixed-statements"})

	c.Assert(lintOneDialect(c, "postgres", "BEGIN;\nALTER TABLE t ADD COLUMN c INT;\nCOMMIT;"), qt.DeepEquals, []string{"transaction/explicit-transaction"})
}

func TestLintFS_NonTransactionalDirectivesSuppressTransactionFindings(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "ptah directive",
			sql: `-- +ptah no_transaction
CREATE INDEX CONCURRENTLY idx ON t (id);
ALTER TABLE t ADD COLUMN c INT;`,
		},
		{
			name: "atlas directive",
			sql: `-- atlas:txmode none

CREATE INDEX CONCURRENTLY idx ON t (id);
ALTER TABLE t ADD COLUMN c INT;`,
		},
		// A byte order mark in front of the directive is the file's encoding,
		// not text on the directive's line, so the directive still holds.
		{
			name: "ptah directive after a byte order mark",
			sql:  "\uFEFF-- +ptah no_transaction\nCREATE INDEX CONCURRENTLY idx ON t (id);\nALTER TABLE t ADD COLUMN c INT;",
		},
		{
			name: "atlas directive after a byte order mark",
			sql:  "\uFEFF-- atlas:txmode none\n\nCREATE INDEX CONCURRENTLY idx ON t (id);\nALTER TABLE t ADD COLUMN c INT;",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			fsys := fixture(map[string]string{
				"0000000001_x.up.sql":   tt.sql,
				"0000000001_x.down.sql": "-- restore\n",
			})
			findings, err := lint.LintFS(c.Context(), builtintest.Runtime(), fsys, lint.Options{Dialect: "postgres"})
			c.Assert(err, qt.IsNil)
			c.Assert(rulesOf(findings), qt.HasLen, 0)
		})
	}
}

func TestLintFS_AtlasTxtarTxModeIsDirectional(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "down none does not suppress up findings",
			sql: `-- atlas:txtar

-- migration.sql --
CREATE INDEX CONCURRENTLY idx ON t (id);
ALTER TABLE t ADD COLUMN c INT;

-- down.sql --
-- atlas:txmode none

DROP INDEX CONCURRENTLY idx;
`,
			want: []string{"transaction/concurrent-index", "transaction/mixed-statements"},
		},
		{
			name: "up none suppresses up findings",
			sql: `-- atlas:txtar

-- migration.sql --
-- atlas:txmode none

CREATE INDEX CONCURRENTLY idx ON t (id);
ALTER TABLE t ADD COLUMN c INT;

-- down.sql --
DROP INDEX idx;
`,
			want: make([]string, 0),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			findings, err := lint.LintFS(c.Context(), builtintest.Runtime(),
				fixture(map[string]string{"1_txmode.sql": test.sql}),
				lint.Options{
					Dialect:   "postgres",
					DirFormat: migrationfile.DirFormatAtlas,
				},
			)
			c.Assert(err, qt.IsNil)
			c.Assert(rulesOf(findings), qt.DeepEquals, test.want)
		})
	}
}

func TestLintFS_AtlasTxtarFindingUsesSourceLine(t *testing.T) {
	c := qt.New(t)
	findings, err := lint.LintFS(c.Context(), builtintest.Runtime(),
		fixture(map[string]string{
			"1_drop.sql": `-- atlas:txtar

-- migration.sql --
DROP TABLE users;

-- down.sql --
CREATE TABLE users (id INTEGER PRIMARY KEY);
`,
		}),
		lint.Options{DirFormat: migrationfile.DirFormatAtlas},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(rulesOf(findings), qt.DeepEquals, []string{"destructive/drop-table", "rollout/drop-table"})
	c.Assert(findings[0].Line, qt.Equals, 4)
}

func TestLintFS_AtlasTxModeNoneDirectiveRequiresLineComment(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "string literal",
			sql: `INSERT INTO notes (body) VALUES ('runbook:
-- atlas:txmode none
done');
CREATE INDEX CONCURRENTLY idx ON t (id);
ALTER TABLE t ADD COLUMN c INT;`,
		},
		{
			name: "block comment",
			sql: `/*
-- atlas:txmode none
*/
CREATE INDEX CONCURRENTLY idx ON t (id);
ALTER TABLE t ADD COLUMN c INT;`,
		},
		{
			name: "trailing comment",
			sql: `SELECT 1; -- atlas:txmode none
CREATE INDEX CONCURRENTLY idx ON t (id);
ALTER TABLE t ADD COLUMN c INT;`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			fsys := fixture(map[string]string{
				"0000000001_x.up.sql":   tt.sql,
				"0000000001_x.down.sql": "-- restore\n",
			})
			findings, err := lint.LintFS(c.Context(), builtintest.Runtime(), fsys, lint.Options{Dialect: "postgres"})
			c.Assert(err, qt.IsNil)
			c.Assert(rulesOf(findings), qt.DeepEquals, []string{"transaction/concurrent-index", "transaction/mixed-statements"})
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
			"INSERT INTO paths (prefix) VALUES ('C:\\');\nALTER TABLE users DROP COLUMN email;", []string{"destructive/drop-column", "rollout/drop-column"}},
		{"postgres like escape literal", "postgres",
			"ALTER TABLE t ADD CONSTRAINT chk CHECK (code NOT LIKE '%\\_%' ESCAPE '\\');\nALTER TABLE users DROP COLUMN email;", []string{"locking/add-check", "destructive/drop-column", "rollout/drop-column"}},
		// MySQL treats backslash as an escape: \' stays inside the literal.
		{"mysql backslash-escaped quote", "mysql",
			"INSERT INTO notes (t) VALUES ('it\\'s; fine');\nALTER TABLE users DROP COLUMN email;", []string{"destructive/drop-column", "rollout/drop-column"}},

		// # line comments are MySQL/MariaDB syntax and must neither hide
		// hazards nor leak decoy text into statements.
		{"mysql hash comment before statement", "mysql",
			"# drop unused column\nALTER TABLE users DROP COLUMN email;", []string{"destructive/drop-column", "rollout/drop-column"}},
		{"mysql hash comment inside statement", "mysql",
			"ALTER TABLE users\n# remove the legacy column\nDROP COLUMN email;", []string{"destructive/drop-column", "rollout/drop-column"}},
		{"mysql hash comment decoys are inert", "mysql",
			"# decoy; DROP TABLE x;\nCREATE TABLE t (id INT);", nil},
		// In PostgreSQL # is an operator, not a comment starter.
		{"postgres hash operator in index expression", "postgres",
			"CREATE INDEX idx ON t ((data #>> '{a}'));", []string{"locking/create-index"}},
		// The default (no --dialect) hybrid must NOT treat # as a comment:
		// # is a PostgreSQL operator (jsonb #>>, bitwise XOR), and swallowing
		// the rest of the line would merge statements and hide the following
		// DROP TABLE (a destructive/drop-table false negative in the default CI invocation).
		{"default dialect jsonb operator does not hide drop table", "",
			"UPDATE cfg SET v = data #>> '{key}';\nDROP TABLE legacy_audit;", []string{"destructive/drop-table", "rollout/drop-table"}},
		{"default dialect xor operator does not hide drop table", "",
			"UPDATE flags SET mask = mask # 1;\nDROP TABLE audit_log;", []string{"destructive/drop-table", "rollout/drop-table"}},
		// Mirror false positive: a # merge that buries a same-file CREATE makes
		// the later DROP of that created table fire destructive/drop-table wrongly.
		{"default dialect xor does not manufacture same-file drop false positive", "",
			"UPDATE flags SET mask = mask # 1;\nCREATE TABLE tmp_backfill (id INT);\nDROP TABLE tmp_backfill;", nil},
		// Block comments do not nest in MySQL/MariaDB, so the default hybrid
		// must close at the first '*/' — otherwise it keeps scanning for a
		// second '*/' and swallows the DROP that MySQL would execute.
		{"default dialect non-nesting comment does not hide drop table", "",
			"/* note /* inner */\nDROP TABLE t;", []string{"destructive/drop-table", "rollout/drop-table"}},
		// Under an explicit postgres dialect, block comments DO nest, so the
		// same input is one comment and nothing fires.
		{"postgres nesting comment hides nothing real", "postgres",
			"/* note /* inner */ still comment */\nSELECT 1;", nil},

		// MySQL executable comments are real SQL to the server.
		{"mysql executable comment hides real ddl", "mysql",
			"/*!50003 ALTER TABLE users DROP COLUMN email */;", []string{"destructive/drop-column", "rollout/drop-column"}},

		// PostgreSQL block comments nest.
		{"postgres nested block comment", "postgres",
			"/* cleanup /* legacy */ block */\nALTER TABLE users DROP COLUMN email;", []string{"destructive/drop-column", "rollout/drop-column"}},

		// Encoding and termination edge cases.
		{"utf8 bom before first statement", "", "\uFEFFDROP TABLE users;", []string{"destructive/drop-table", "rollout/drop-table"}},
		{"final statement without semicolon", "", "DROP TABLE users", []string{"destructive/drop-table", "rollout/drop-table"}},
		{"unicode table name", "", "ALTER TABLE пользователи DROP COLUMN email;", []string{"destructive/drop-column", "rollout/drop-column"}},
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

	findings, err := lint.LintFS(c.Context(), builtintest.Runtime(), fsys, lint.Options{Dialect: "mysql"})
	c.Assert(err, qt.IsNil)
	c.Assert(rulesOf(findings), qt.DeepEquals, []string{"destructive/drop-column", "rollout/drop-column"})
	c.Assert(linesOf(findings), qt.DeepEquals, []int{3, 3},
		qt.Commentf("every finding points at the ALTER TABLE start, not the comment"))
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
			"CREATE TABLE staging (id INT);\nDROP TABLE users;", []string{"destructive/drop-table", "rollout/drop-table"}},
		{"drop before create still fires", "postgres",
			"DROP TABLE staging;\nCREATE TABLE staging (id INT);", []string{"destructive/drop-table", "rollout/drop-table"}},
		{"multi-table drop with one pre-existing fires", "postgres",
			"CREATE TABLE staging (id INT);\nDROP TABLE staging, users;", []string{"destructive/drop-table", "rollout/drop-table"}},

		// An index on a table created two statements earlier is built on an
		// empty table — no lock hazard.
		{"index on table created in same file", "postgres",
			"CREATE TABLE orders (id BIGSERIAL PRIMARY KEY, user_id BIGINT);\nCREATE INDEX idx_orders_user ON orders (user_id);", nil},
		{"index on schema-qualified created table", "postgres",
			"CREATE TABLE app.orders (id INT);\nCREATE UNIQUE INDEX uq ON app.orders (id);", nil},
		{"index on pre-existing table still fires", "postgres",
			"CREATE TABLE orders (id INT);\nCREATE INDEX idx_users_email ON users (email);", []string{"locking/create-index"}},
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

// CREATE OR REPLACE TABLE is MariaDB syntax that drops the named table and
// recreates it empty. Measured on MariaDB 12.3.3: a table holding rows holds
// none once its CREATE OR REPLACE TABLE runs, so destructive/drop-table has to catch it exactly
// as it catches DROP TABLE (stokaro/ptah-operator#481).
func TestLintFS_ReplacedTableIsDestructive(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{"basic form", "CREATE OR REPLACE TABLE victim (id INT);", []string{"destructive/drop-table"}},
		{"lower case", "create or replace table victim (id int);", []string{"destructive/drop-table"}},
		{"schema-qualified", "CREATE OR REPLACE TABLE app.victim (id INT);", []string{"destructive/drop-table"}},
		{"from a query", "CREATE OR REPLACE TABLE victim AS SELECT id FROM source;", []string{"destructive/drop-table"}},
		{"like another table", "CREATE OR REPLACE TABLE victim LIKE template;", []string{"destructive/drop-table"}},
		// TEMPORARY replaces only a table local to the session, so it destroys
		// no row that outlives it.
		{"temporary form stays safe", "CREATE OR REPLACE TEMPORARY TABLE scratch (id INT);", make([]string, 0)},
		// A table literally named "replace" is a quoted identifier, never the
		// OR REPLACE keyword pair.
		{"table literally named replace", "CREATE TABLE `replace` (id INT);", make([]string, 0)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			got := lintOne(c, tt.sql)
			c.Assert(got, qt.DeepEquals, tt.want, qt.Commentf("sql: %s", tt.sql))
		})
	}
}

// The create-staging/backfill/replace pattern mirrors DROP TABLE's same-file
// exemption: replacing a table this same migration created destroys no
// pre-existing data.
func TestLintFS_ReplacedTableSameFileCreatedIsExempt(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{"replace of table created in same file",
			"CREATE TABLE staging (id INT);\nCREATE OR REPLACE TABLE staging (id INT);", make([]string, 0)},
		{"replace of pre-existing table still fires",
			"CREATE TABLE staging (id INT);\nCREATE OR REPLACE TABLE users (id INT);", []string{"destructive/drop-table"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			got := lintOne(c, tt.sql)
			c.Assert(got, qt.DeepEquals, tt.want, qt.Commentf("sql: %s", tt.sql))
		})
	}
}

// MariaDB's own /*M!...*/ executable comment is real SQL to the server. The
// default hybrid and an explicit mysql/mariadb target all scan its content
// rather than hide the hazard inside it, exactly as they already do for the
// bare /*!NNNNN form; PostgreSQL has no such convention, and the block there
// stays an ordinary comment.
func TestLintFS_MariaDBExecutableCommentIsScanned(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
		want    []string
	}{
		{"mariadb executable comment hides real ddl", "mariadb",
			"/*M! CREATE OR REPLACE TABLE `victim` (`id` INT) */;", []string{"destructive/drop-table"}},
		{"mariadb executable comment with a version marker", "mariadb",
			"/*M!100001 DROP TABLE victim */;", []string{"destructive/drop-table", "rollout/drop-table"}},
		{"default dialect scans it too", "",
			"/*M! CREATE OR REPLACE TABLE victim (id INT) */;", []string{"destructive/drop-table"}},
		{"mysql target scans it too", "mysql",
			"/*M! CREATE OR REPLACE TABLE victim (id INT) */;", []string{"destructive/drop-table"}},
		{"postgres leaves the marker as an inert comment", "postgres",
			"/*M! DROP TABLE users */;\nSELECT 1;", make([]string, 0)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			got := lintOneDialect(c, tt.dialect, tt.sql)
			c.Assert(got, qt.DeepEquals, tt.want, qt.Commentf("sql: %s", tt.sql))
		})
	}
}

func TestLintFS_MY101PinnedOnlineDDLIsExempt(t *testing.T) {
	c := qt.New(t)

	// Pinned ALGORITHM/LOCK make the server refuse a blocking rebuild, so
	// the lock hazard cannot occur; the lossy-type-change warning stays.
	got := lintOneDialect(c, "mysql",
		"ALTER TABLE users MODIFY COLUMN bio VARCHAR(500) NOT NULL, ALGORITHM=INPLACE, LOCK=NONE;")
	c.Assert(got, qt.DeepEquals, []string{"data-dependent/alter-column-type"})

	// The = is optional in the MySQL grammar.
	got = lintOneDialect(c, "mysql",
		"ALTER TABLE users MODIFY COLUMN bio VARCHAR(500), ALGORITHM INPLACE;")
	c.Assert(got, qt.DeepEquals, []string{"data-dependent/alter-column-type"})

	// ALGORITHM=COPY pins the blocking path; locking/blocking-alter-table must still fire.
	got = lintOneDialect(c, "mysql",
		"ALTER TABLE users MODIFY COLUMN bio VARCHAR(500), ALGORITHM=COPY;")
	c.Assert(got, qt.DeepEquals, []string{"data-dependent/alter-column-type", "locking/blocking-alter-table"})
}

func TestLintFS_NestedDirectoriesAreLinted(t *testing.T) {
	c := qt.New(t)

	// The migrator's FSMigrationProvider discovers migrations recursively,
	// so lint must walk subdirectories too.
	fsys := fixture(map[string]string{
		"sub/0000000001_a.up.sql":   "DROP TABLE users;\n",
		"sub/0000000001_a.down.sql": "-- restore\n",
	})

	findings, err := lint.LintFS(c.Context(), builtintest.Runtime(), fsys, lint.Options{PathPrefix: "db/migrations"})
	c.Assert(err, qt.IsNil)
	c.Assert(rulesOf(findings), qt.DeepEquals, []string{"destructive/drop-table", "rollout/drop-table"})
	c.Assert(filesOf(findings), qt.DeepEquals, []string{
		"db/migrations/sub/0000000001_a.up.sql",
		"db/migrations/sub/0000000001_a.up.sql",
	})
	c.Assert(linesOf(findings), qt.DeepEquals, []int{1, 1})
}

func TestLintFS_UpSuffixFallbackScansMalformedVersions(t *testing.T) {
	c := qt.New(t)

	// A .up.sql file whose version prefix the migrator rejects still gets
	// hazard scanning via the IsUp suffix fallback — the author clearly
	// meant it as an up migration, and file-form/file-name explains why it will not run.
	fsys := fixture(map[string]string{
		"bad_version.up.sql":   "DROP TABLE users;\n",
		"bad_version.down.sql": "-- restore\n",
	})

	findings, err := lint.LintFS(c.Context(), builtintest.Runtime(), fsys, lint.Options{})
	c.Assert(err, qt.IsNil)
	c.Assert(rulesOf(findings), qt.DeepEquals, []string{"file-form/file-name", "file-form/file-name", "destructive/drop-table", "rollout/drop-table"},
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

	findings, err := lint.LintFS(c.Context(), builtintest.Runtime(), fsys, lint.Options{})
	c.Assert(err, qt.IsNil)

	c.Assert(rulesOf(findings), qt.DeepEquals, []string{"file-form/file-name", "file-form/file-name"},
		qt.Commentf("both files are invisible to the migrator: naming warnings only; got %v", findings))
	for _, f := range findings {
		c.Assert(f.Message, qt.Contains, "the migrator will not pick it up")
	}
}

func TestLintFS_AtlasMigrationNamesAreScanned(t *testing.T) {
	c := qt.New(t)

	fsys := fixture(map[string]string{
		"20220318104614_team_A.sql": "DROP TABLE users;\n",
	})

	findings, err := lint.LintFS(c.Context(), builtintest.Runtime(), fsys, lint.Options{})
	c.Assert(err, qt.IsNil)
	c.Assert(rulesOf(findings), qt.DeepEquals, []string{"destructive/drop-table", "rollout/drop-table"},
		qt.Commentf("Atlas files should be treated as runnable up migrations, not MF103-only noise; got %v", findings))
	c.Assert(findings[0].File, qt.Equals, "20220318104614_team_A.sql")
}

func TestLintFS_AtlasImportedMigrationNamesAreScanned(t *testing.T) {
	c := qt.New(t)

	fsys := fixture(map[string]string{
		"1_initial.up.sql":   "DROP TABLE users;\n",
		"1_initial.down.sql": "DROP TABLE users;\n",
		"2.10.x-20_next.sql": "CREATE TABLE next_step (id INT);\n",
	})

	findings, err := lint.LintFS(c.Context(), builtintest.Runtime(), fsys, lint.Options{})
	c.Assert(err, qt.IsNil)
	c.Assert(rulesOf(findings), qt.DeepEquals, []string{"destructive/drop-table", "rollout/drop-table"},
		qt.Commentf("Atlas-imported files should be treated as runnable migrations, not MF-only noise; got %v", findings))
	c.Assert(findings[0].File, qt.Equals, "1_initial.up.sql")
}

func TestLintFS_AtlasTemplateMigrationsAreRendered(t *testing.T) {
	c := qt.New(t)

	fsys := fixture(map[string]string{
		"1.sql": `{{- if eq .Env "dev" }}
CREATE TABLE dev1 (id INT);
{{- else }}
DROP TABLE prod1;
{{- end }}
`,
		"2.sql": `{{ template "shared/users" "prod2" }}`,
		"shared/users.sql": `{{- define "shared/users" }}
CREATE TABLE users_{{ $ }} (id INT);
{{- end }}
`,
	})

	findings, err := lint.LintFS(c.Context(), builtintest.Runtime(), fsys, lint.Options{})
	c.Assert(err, qt.IsNil)
	c.Assert(rulesOf(findings), qt.DeepEquals, []string{"destructive/drop-table", "rollout/drop-table"},
		qt.Commentf("Atlas templates should be rendered before linting and shared definitions should not emit MF103; got %v", findings))
	c.Assert(findings[0].File, qt.Equals, "1.sql")
}

func TestLintFS_PromptPlaceholdersDoNotSelectTemplateRendering(t *testing.T) {
	c := qt.New(t)
	fsys := fixture(map[string]string{
		"001_prompt.sql":   "UPDATE prompts SET body = '{{DATE}}; {{ ... }}' WHERE id = 1;\nDROP TABLE old_prompts;\n",
		"002_template.sql": `{{ if eq .Env "" }}DROP TABLE old_settings;{{ end }}`,
	})

	findings, err := lint.LintFS(c.Context(), builtintest.Runtime(), fsys, lint.Options{})
	c.Assert(err, qt.IsNil)
	c.Assert(rulesOf(findings), qt.DeepEquals, []string{"destructive/drop-table", "rollout/drop-table", "destructive/drop-table", "rollout/drop-table"})
}

func TestLintFS_CaseVariantSQLFilesStopTheRun(t *testing.T) {
	c := qt.New(t)

	fsys := fixture(map[string]string{
		"0000000001_x.up.sql":   "CREATE TABLE t (id INT);\n",
		"0000000001_x.down.sql": "DROP TABLE t;\n",
		"0000000002_y.UP.SQL":   "DROP TABLE users;\n",
	})

	findings, err := lint.LintFS(c.Context(), builtintest.Runtime(), fsys, lint.Options{})
	c.Assert(err, qt.IsNil)
	c.Assert(rulesOf(findings), qt.DeepEquals, []string{"file-form/refused-file"},
		qt.Commentf("a case-variant file the migrator refuses the directory for is reported instead of vanishing"))
}

// refusedFiles names the files the findings report under file-form/refused-file, in order.
func refusedFiles(findings []lint.Finding) []string {
	files := make([]string, 0, len(findings))
	for _, finding := range findings {
		if finding.Rule == "file-form/refused-file" {
			files = append(files, finding.File)
		}
	}
	return files
}

// Disabling a rule that is not the refusal leaves the refusal of a file a run
// refuses the directory for reported.
func TestLintFS_DisablingAnotherRuleKeepsTheRunRefusal(t *testing.T) {
	c := qt.New(t)
	fsys := fixture(map[string]string{
		"0000000001_a.up.sql":   "CREATE TABLE a (id INT);\n",
		"0000000001_a.down.sql": "DROP TABLE a;\n",
		"notes.sql":             "CREATE TABLE notes (id INT);\n",
	})

	findings, err := lint.LintFS(c.Context(), builtintest.Runtime(), fsys, lint.Options{Disabled: []string{"data-dependent/set-not-null"}})

	c.Assert(err, qt.IsNil)
	c.Assert(refusedFiles(findings), qt.DeepEquals, []string{"notes.sql"})
}

// TestLintFS_FilesTheMigratorRefusesTheDirectoryFor reports under file-form/refused-file each
// file the migrator's reading leaves out at the top of the directory, which a
// run refuses the directory for, whichever grammar its name resembles. The
// Atlas profile reports none: the community binary runs every top-level file.
func TestLintFS_FilesTheMigratorRefusesTheDirectoryFor(t *testing.T) {
	tests := []struct {
		name    string
		files   map[string]string
		profile lint.CompatibilityProfile
		want    []string
	}{
		{"an Atlas name beside a Ptah pair", map[string]string{
			"0000000001_a.up.sql":   "CREATE TABLE a (id INT);\n",
			"0000000001_a.down.sql": "DROP TABLE a;\n",
			"20240101_seed.sql":     "CREATE TABLE seed (id INT);\n",
		}, lint.CompatibilityProfileNative, []string{"20240101_seed.sql"}},
		{"a version auto-detection does not read beside a numbered file", map[string]string{
			"1_a.sql":  "CREATE TABLE a (id INT);\n",
			"v2_b.sql": "CREATE TABLE b (id INT);\n",
		}, lint.CompatibilityProfileNative, []string{"v2_b.sql"}},
		{"the Atlas profile", map[string]string{
			"1_a.sql":  "CREATE TABLE a (id INT);\n",
			"v2_b.sql": "CREATE TABLE b (id INT);\n",
		}, lint.CompatibilityProfileAtlas, make([]string, 0)},
		{"two up files of one Ptah version", map[string]string{
			"0000000001_a.up.sql":   "CREATE TABLE a (id INT);\n",
			"0000000001_a.down.sql": "DROP TABLE a;\n",
			"0000000001_b.up.sql":   "CREATE TABLE b (id INT);\n",
		}, lint.CompatibilityProfileNative, []string{"0000000001_a.up.sql", "0000000001_b.up.sql"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			findings, err := lint.LintFS(c.Context(), builtintest.Runtime(), fixture(test.files), lint.Options{Compatibility: test.profile})

			c.Assert(err, qt.IsNil)
			c.Assert(refusedFiles(findings), qt.DeepEquals, test.want)
		})
	}
}

// TestLintFS_VersionsTheNumericRevisionTableCannotKeep reports under file-form/refused-file
// each Atlas file whose version Ptah's revision table, which records numbers,
// cannot keep, as the migrator's guard refuses the directory for it: R__x.sql,
// the version R, and under positions a version such as v1. The Atlas revision
// table keeps them, the Ptah format reads R__x.sql as a repeatable migration,
// and the Atlas profile reports none.
func TestLintFS_VersionsTheNumericRevisionTableCannotKeep(t *testing.T) {
	repeatable := map[string]string{
		"1_a.sql":  "CREATE TABLE a (id INT);\n",
		"R__x.sql": "CREATE VIEW x AS SELECT 1;\n",
	}
	tests := []struct {
		name  string
		files map[string]string
		opts  lint.Options
		want  []string
	}{
		{"a repeatable over the numeric table", repeatable, lint.Options{}, []string{"R__x.sql"}},
		{"a repeatable alone", map[string]string{"R__x.sql": "CREATE VIEW x AS SELECT 1;\n"}, lint.Options{}, []string{"R__x.sql"}},
		{"a version under positions", map[string]string{
			"1_a.sql":  "CREATE TABLE a (id INT);\n",
			"v2_b.sql": "CREATE TABLE b (id INT);\n",
		}, lint.Options{DirFormat: migrationfile.DirFormatAtlas}, []string{"v2_b.sql"}},
		{"the Atlas revision table", repeatable, lint.Options{AtlasRevisionTable: true}, make([]string, 0)},
		{"the Ptah format", map[string]string{
			"0000000001_a.up.sql":   "CREATE TABLE a (id INT);\n",
			"0000000001_a.down.sql": "DROP TABLE a;\n",
			"R__x.sql":              "CREATE VIEW x AS SELECT 1;\n",
		}, lint.Options{}, make([]string, 0)},
		{"the Atlas profile", repeatable, lint.Options{Compatibility: lint.CompatibilityProfileAtlas}, make([]string, 0)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			findings, err := lint.LintFS(c.Context(), builtintest.Runtime(), fixture(test.files), test.opts)

			c.Assert(err, qt.IsNil)
			c.Assert(refusedFiles(findings), qt.DeepEquals, test.want)
		})
	}
}

// The finding on a repeatable names the version and every way out, as the
// migrator's refusal does, and it is an error.
func TestLintFS_ARepeatableOverTheNumericRevisionTable(t *testing.T) {
	c := qt.New(t)
	fsys := fixture(map[string]string{
		"1_a.sql":  "CREATE TABLE a (id INT);\n",
		"R__x.sql": "CREATE VIEW x AS SELECT 1;\n",
	})

	findings, err := lint.LintFS(c.Context(), builtintest.Runtime(), fsys, lint.Options{})

	c.Assert(err, qt.IsNil)
	c.Assert(findings, qt.HasLen, 1)
	c.Assert(findings[0].Severity, qt.Equals, lint.SeverityError)
	c.Assert(findings[0].Message, qt.Equals, `Ptah's revision table records numbers and cannot keep this file's version, "R", `+
		`so a run refuses the directory; use the Atlas revision table (--revision-format atlas), or Ptah's file names, `+
		`under which R__<name>.sql is a repeatable migration; a directory of repeatable migrations alone is read in `+
		`Ptah's format with --dir-format ptah`)
}

func TestLintFS_DialectGating(t *testing.T) {
	c := qt.New(t)

	fsys := fixture(map[string]string{
		"0000000001_mixed.up.sql": `CREATE INDEX idx ON users (email);
ALTER TABLE users MODIFY COLUMN email VARCHAR(64);
`,
		"0000000001_mixed.down.sql": "-- restore\n",
	})

	// postgres: PG rules fire, MY rules do not (data-dependent/alter-column-type is generic and stays).
	findings, err := lint.LintFS(c.Context(), builtintest.Runtime(), fsys, lint.Options{Dialect: "postgres"})
	c.Assert(err, qt.IsNil)
	c.Assert(rulesOf(findings), qt.DeepEquals, []string{"locking/create-index", "data-dependent/alter-column-type"})

	// mariadb: MY rules fire, PG rules do not.
	findings, err = lint.LintFS(c.Context(), builtintest.Runtime(), fsys, lint.Options{Dialect: "mariadb"})
	c.Assert(err, qt.IsNil)
	c.Assert(rulesOf(findings), qt.DeepEquals, []string{"data-dependent/alter-column-type", "locking/blocking-alter-table"})

	// mysql gates identically to mariadb.
	findings, err = lint.LintFS(c.Context(), builtintest.Runtime(), fsys, lint.Options{Dialect: "mysql"})
	c.Assert(err, qt.IsNil)
	c.Assert(rulesOf(findings), qt.DeepEquals, []string{"data-dependent/alter-column-type", "locking/blocking-alter-table"})

	// empty dialect: everything fires.
	findings, err = lint.LintFS(c.Context(), builtintest.Runtime(), fsys, lint.Options{})
	c.Assert(err, qt.IsNil)
	c.Assert(rulesOf(findings), qt.DeepEquals, []string{"locking/create-index", "data-dependent/alter-column-type", "locking/blocking-alter-table"})
}

func TestAnalyzeFS_CustomRuleUsesPublicScanner(t *testing.T) {
	c := qt.New(t)

	fsys := fixture(map[string]string{
		"0000000001_create_users.up.sql":   "CREATE TABLE users (id INT);\n",
		"0000000001_create_users.down.sql": "DROP TABLE users;\n",
	})

	analysis, err := lint.AnalyzeFS(c.Context(), builtintest.Runtime(), fsys, lint.Options{})
	c.Assert(err, qt.IsNil)
	files := analysis.Files()
	c.Assert(files, qt.HasLen, 2)
	c.Assert(files[1].Name, qt.Equals, "0000000001_create_users.up.sql")
	c.Assert(files[1].Statements[0].Words, qt.DeepEquals, []string{"CREATE", "TABLE", "USERS", "(", "ID", "INT", ")"})

	customRule := lint.Rule{
		Name:           "design/xx001",
		Title:          "custom create table analyzer",
		Severity:       lint.SeverityWarning,
		CheckStatement: checkCustomCreateTable,
	}
	findings, err := lint.LintFS(c.Context(), builtintest.Runtime(), fsys, lint.Options{
		ExtraRules: []lint.Rule{customRule},
		RuleConfigs: map[string]lint.RuleConfig{
			"design/xx001": {Severity: lint.SeverityError},
		},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(rulesOf(findings), qt.DeepEquals, []string{"design/xx001"})
	c.Assert(findings[0].Severity, qt.Equals, lint.SeverityError)
	c.Assert(findings[0].Message, qt.Contains, "Ptah's scanner")
}

func checkCustomCreateTable(stmt *lint.Statement) (bool, string) {
	if len(stmt.Words) >= 3 && stmt.Words[0] == "CREATE" && stmt.Words[1] == "TABLE" {
		return true, "custom analyzer saw a create table statement through Ptah's scanner"
	}
	return false, ""
}

func TestLintFS_InvalidCustomRuleFailsFast(t *testing.T) {
	c := qt.New(t)

	_, err := lint.LintFS(c.Context(), builtintest.Runtime(), fixture(map[string]string{
		"0000000001_x.up.sql":   "SELECT 1;\n",
		"0000000001_x.down.sql": "-- restore\n",
	}), lint.Options{ExtraRules: []lint.Rule{{Name: "design/xx001", Title: "broken"}}})

	c.Assert(err, qt.ErrorMatches, "rule design/xx001 has unsupported severity.*")
}

func TestLintFS_LowercaseCustomRuleCodeFailsFast(t *testing.T) {
	c := qt.New(t)

	_, err := lint.LintFS(c.Context(), builtintest.Runtime(), fixture(map[string]string{
		"0000000001_x.up.sql":   "SELECT 1;\n",
		"0000000001_x.down.sql": "-- restore\n",
	}), lint.Options{ExtraRules: []lint.Rule{{
		Name:     "ext001",
		Title:    "lowercase code",
		Severity: lint.SeverityWarning,
		CheckStatement: func(*lint.Statement) (bool, string) {
			return false, ""
		},
	}}})

	c.Assert(err, qt.ErrorMatches, `rule code "ext001" must start with an uppercase ASCII letter.*`)
}

func TestLintFS_RuleConfigsOverrideSeverityAndExcludePaths(t *testing.T) {
	c := qt.New(t)

	fsys := fixture(map[string]string{
		"legacy/0000000001_legacy.up.sql":   "ALTER TABLE users DROP COLUMN old_legacy;\n",
		"legacy/0000000001_legacy.down.sql": "ALTER TABLE users ADD COLUMN old_legacy TEXT;\n",
		"main/0000000002_main.up.sql":       "ALTER TABLE users DROP COLUMN old_main;\n",
		"main/0000000002_main.down.sql":     "ALTER TABLE users ADD COLUMN old_main TEXT;\n",
	})

	findings, err := lint.LintFS(c.Context(), builtintest.Runtime(), fsys, lint.Options{
		RuleConfigs: map[string]lint.RuleConfig{
			"destructive/drop-column": {
				Severity: lint.SeverityWarning,
				Exclude:  []string{"legacy/**"},
			},
		},
	})

	c.Assert(err, qt.IsNil)
	// The override and the exclusion both name destructive/drop-column, and both reach destructive/drop-column
	// alone: rollout/drop-column keeps its own severity and is still reported under
	// legacy/**, because excluding a path for one rule is not excluding the
	// path.
	c.Assert(rulesOf(findings), qt.DeepEquals, []string{"rollout/drop-column", "destructive/drop-column", "rollout/drop-column"})
	c.Assert(severitiesOf(findings), qt.DeepEquals, []lint.Severity{
		lint.SeverityWarning, lint.SeverityWarning, lint.SeverityWarning,
	})
	c.Assert(filesOf(findings), qt.DeepEquals, []string{
		"legacy/0000000001_legacy.up.sql",
		"main/0000000002_main.up.sql",
		"main/0000000002_main.up.sql",
	})
}

func TestLintFS_InlineNoLintSuppressesStatementAndFileFindings(t *testing.T) {
	c := qt.New(t)

	fsys := fixture(map[string]string{
		"0000000001_suppress.up.sql": `-- ptah:nolint destructive/drop-column
ALTER TABLE users DROP COLUMN legacy;
-- atlas:nolint DS102
ALTER TABLE users DROP COLUMN legacy_two;
-- ptah:nolint destructive/drop-table
DROP TABLE audit_log;
ALTER TABLE users ALTER COLUMN email TYPE TEXT;
`,
		"0000000001_suppress.down.sql": "-- restore\n",
	})

	findings, err := lint.LintFS(c.Context(), builtintest.Runtime(), fsys, lint.Options{})
	c.Assert(err, qt.IsNil)
	// Each directive names one rule, and each rule reports one consequence, so
	// the rollout break of every dropped name survives a directive that accepts
	// the data loss.
	c.Assert(rulesOf(findings), qt.DeepEquals, []string{"rollout/drop-column", "rollout/drop-column", "rollout/drop-table", "data-dependent/alter-column-type"})
}

func TestLintFS_TrailingNoLintCommentDoesNotSuppressNextStatement(t *testing.T) {
	c := qt.New(t)

	fsys := fixture(map[string]string{
		"0000000001_trailing.up.sql": `ALTER TABLE users ALTER COLUMN email TYPE TEXT; -- ptah:nolint destructive/drop-column
ALTER TABLE users DROP COLUMN legacy;
`,
		"0000000001_trailing.down.sql": "-- restore\n",
	})

	findings, err := lint.LintFS(c.Context(), builtintest.Runtime(), fsys, lint.Options{})
	c.Assert(err, qt.IsNil)
	c.Assert(rulesOf(findings), qt.DeepEquals, []string{"data-dependent/alter-column-type", "destructive/drop-column", "rollout/drop-column"})
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
	findings, err := lint.LintFS(c.Context(), builtintest.Runtime(), fsys, lint.Options{Disabled: []string{"rollout/drop-table", "destructive/drop-table"}})
	c.Assert(err, qt.IsNil)
	c.Assert(rulesOf(findings), qt.DeepEquals, []string{"destructive/drop-column", "rollout/drop-column", "locking/create-index"})

	// Disable a whole category.
	findings, err = lint.LintFS(c.Context(), builtintest.Runtime(), fsys, lint.Options{Disabled: []string{"destructive"}})
	c.Assert(err, qt.IsNil)
	c.Assert(rulesOf(findings), qt.DeepEquals, []string{"rollout/drop-table", "rollout/drop-column", "locking/create-index"})

	// A stray empty entry must not disable everything.
	findings, err = lint.LintFS(c.Context(), builtintest.Runtime(), fsys, lint.Options{Disabled: []string{""}})
	c.Assert(err, qt.IsNil)
	c.Assert(rulesOf(findings), qt.DeepEquals, []string{"destructive/drop-table", "rollout/drop-table", "destructive/drop-column", "rollout/drop-column", "locking/create-index"})
}

func TestLintFS_ConstraintDeletionFamily(t *testing.T) {
	c := qt.New(t)

	fsys := fixture(map[string]string{
		"0000000001_drops.up.sql": `ALTER TABLE orders DROP FOREIGN KEY fk_orders_user;
ALTER TABLE users DROP CHECK chk_age;
ALTER TABLE users DROP PRIMARY KEY;
ALTER TABLE users DROP CONSTRAINT uq_email;
`,
		"0000000001_drops.down.sql": "-- restore\n",
	})

	// Each typed drop gets its own CD code; the untyped ANSI DROP CONSTRAINT
	// stays integrity/drop-untyped-constraint. No statement is flagged by both a CD code and integrity/drop-untyped-constraint. The
	// primary key drop is also the table copy rewrite/drop-primary-key names, a different
	// hazard from the guarantee integrity/drop-primary-key names, so both report it.
	findings, err := lint.LintFS(c.Context(), builtintest.Runtime(), fsys, lint.Options{})
	c.Assert(err, qt.IsNil)
	c.Assert(rulesOf(findings), qt.DeepEquals, []string{"integrity/drop-foreign-key", "integrity/drop-check", "integrity/drop-primary-key", "rewrite/drop-primary-key", "integrity/drop-untyped-constraint"})

	// Disabling the typed constraint rules keeps the integrity/drop-untyped-constraint fallback.
	findings, err = lint.LintFS(c.Context(), builtintest.Runtime(), fsys, lint.Options{Disabled: []string{
		"integrity/drop-foreign-key", "integrity/drop-check", "integrity/drop-primary-key",
	}})
	c.Assert(err, qt.IsNil)
	c.Assert(rulesOf(findings), qt.DeepEquals, []string{"rewrite/drop-primary-key", "integrity/drop-untyped-constraint"})

	// Disabling one CD code silences only that constraint type.
	findings, err = lint.LintFS(c.Context(), builtintest.Runtime(), fsys, lint.Options{Disabled: []string{"integrity/drop-foreign-key"}})
	c.Assert(err, qt.IsNil)
	c.Assert(rulesOf(findings), qt.DeepEquals, []string{"integrity/drop-check", "integrity/drop-primary-key", "rewrite/drop-primary-key", "integrity/drop-untyped-constraint"})

	// A per-code RuleConfig.Severity override applies to a CD code.
	fkOnly := fixture(map[string]string{
		"0000000001_fk.up.sql":   "ALTER TABLE orders DROP FOREIGN KEY fk_orders_user;\n",
		"0000000001_fk.down.sql": "-- restore\n",
	})
	findings, err = lint.LintFS(c.Context(), builtintest.Runtime(), fkOnly, lint.Options{
		RuleConfigs: map[string]lint.RuleConfig{"integrity/drop-foreign-key": {Severity: lint.SeverityWarning}},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(findings, qt.HasLen, 1)
	c.Assert(findings[0].Rule, qt.Equals, "integrity/drop-foreign-key")
	c.Assert(findings[0].Severity, qt.Equals, lint.SeverityWarning)
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

	findings, err := lint.LintFS(c.Context(), builtintest.Runtime(), fsys, lint.Options{})
	c.Assert(err, qt.IsNil)
	// The claim is that a statement INSIDE the body triggers no rule. coverage/routine-body is
	// about the body not being read at all, which is the opposite of reading it
	// and is what stops the run from looking clean (stokaro/ptah#1270).
	for _, finding := range findings {
		c.Assert(finding.Rule, qt.Equals, "coverage/routine-body",
			qt.Commentf("statements inside dollar-quoted bodies must not trigger rules; got: %v", findings))
	}
}

func TestLintFS_PathPrefixAppearsInFindings(t *testing.T) {
	c := qt.New(t)

	fsys := fixture(map[string]string{
		"0000000001_bad.up.sql":   "DROP TABLE a;\n",
		"0000000001_bad.down.sql": "-- restore\n",
	})

	findings, err := lint.LintFS(c.Context(), builtintest.Runtime(), fsys, lint.Options{PathPrefix: "db/migrations"})
	c.Assert(err, qt.IsNil)
	c.Assert(filesOf(findings), qt.DeepEquals, []string{
		"db/migrations/0000000001_bad.up.sql",
		"db/migrations/0000000001_bad.up.sql",
	})
}

func TestLintFS_NoMigrationFilesIsAnError(t *testing.T) {
	c := qt.New(t)

	_, err := lint.LintFS(c.Context(), builtintest.Runtime(), fixture(map[string]string{"README.md": "not sql"}), lint.Options{})
	c.Assert(err, qt.ErrorMatches, "no \\*\\.sql migration files found")
}

func TestLoadConfig_HappyPath(t *testing.T) {
	c := qt.New(t)

	dir := t.TempDir()
	c.Assert(writeFile(dir+"/.ptah-lint.yaml", `dialect: postgres
disabled-rules:
  - file-form
  - rollout/rename
rules:
  data-dependent/alter-column-type:
    severity: warning
    exclude:
      - legacy/**
`), qt.IsNil)

	cfg, err := lint.LoadConfig(builtintest.Runtime(), dir+"/.ptah-lint.yaml")
	c.Assert(err, qt.IsNil)
	c.Assert(cfg.Dialect, qt.Equals, "postgres")
	c.Assert(cfg.DisabledRules, qt.DeepEquals, []string{"file-form", "rollout/rename"})
	c.Assert(cfg.Rules["data-dependent/alter-column-type"].Severity, qt.Equals, lint.SeverityWarning)
	c.Assert(cfg.Rules["data-dependent/alter-column-type"].Exclude, qt.DeepEquals, []string{"legacy/**"})
}

func TestLoadConfig_FailurePath(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()

	t.Run("missing explicit file", func(t *testing.T) {
		c := qt.New(t)
		_, err := lint.LoadConfig(builtintest.Runtime(), dir+"/nope.yaml")
		c.Assert(err, qt.ErrorMatches, "failed to read lint config .*nope.yaml.*")
	})

	c.Assert(writeFile(dir+"/broken.yaml", "dialect: [not, a, string"), qt.IsNil)
	t.Run("malformed YAML", func(t *testing.T) {
		c := qt.New(t)
		_, err := lint.LoadConfig(builtintest.Runtime(), dir+"/broken.yaml")
		c.Assert(err, qt.ErrorMatches, "failed to parse lint config .*")
	})

	c.Assert(writeFile(dir+"/bad-severity.yaml", "rules:\n  destructive/drop-column:\n    severity: fatal\n"), qt.IsNil)
	t.Run("invalid rule severity", func(t *testing.T) {
		c := qt.New(t)
		_, err := lint.LoadConfig(builtintest.Runtime(), dir+"/bad-severity.yaml")
		c.Assert(err, qt.ErrorMatches, `failed to parse lint config .*unsupported severity "fatal".*`)
	})

	c.Assert(writeFile(dir+"/bad-dialect.yaml", "dialect: oracle\n"), qt.IsNil)
	t.Run("unsupported dialect", func(t *testing.T) {
		c := qt.New(t)
		_, err := lint.LoadConfig(builtintest.Runtime(), dir+"/bad-dialect.yaml")
		c.Assert(err, qt.ErrorMatches, `failed to parse lint config .*unsupported lint dialect "oracle": expected postgres.*`)
	})

	c.Assert(writeFile(dir+"/bad-exclude.yaml", "rules:\n  destructive/drop-column:\n    exclude:\n      - '[legacy/**'\n"), qt.IsNil)
	t.Run("malformed exclude glob", func(t *testing.T) {
		c := qt.New(t)
		_, err := lint.LoadConfig(builtintest.Runtime(), dir+"/bad-exclude.yaml")
		c.Assert(err, qt.ErrorMatches, `failed to parse lint config .*rule destructive/drop-column has invalid exclude pattern "\[legacy/\*\*": syntax error in pattern`)
	})

	c.Assert(writeFile(dir+"/empty-exclude.yaml", "rules:\n  destructive/drop-column:\n    exclude:\n      - '  '\n"), qt.IsNil)
	t.Run("empty exclude glob", func(t *testing.T) {
		c := qt.New(t)
		_, err := lint.LoadConfig(builtintest.Runtime(), dir+"/empty-exclude.yaml")
		c.Assert(err, qt.ErrorMatches, `failed to parse lint config .*rule destructive/drop-column has invalid exclude pattern "  ": pattern must not be empty`)
	})

	c.Assert(writeFile(dir+"/non-normalized-exclude.yaml", "rules:\n  destructive/drop-column:\n    exclude:\n      - '**/../**'\n"), qt.IsNil)
	t.Run("non-normalized exclude glob", func(t *testing.T) {
		c := qt.New(t)
		_, err := lint.LoadConfig(builtintest.Runtime(), dir+"/non-normalized-exclude.yaml")
		c.Assert(err, qt.ErrorMatches, `failed to parse lint config .*rule destructive/drop-column has invalid exclude pattern "\*\*/\.\./\*\*": pattern must be a normalized slash-separated path`)
	})

	c.Assert(writeFile(dir+"/unknown-top-level.yaml", "severty: error\n"), qt.IsNil)
	t.Run("unknown top-level key", func(t *testing.T) {
		c := qt.New(t)
		_, err := lint.LoadConfig(builtintest.Runtime(), dir+"/unknown-top-level.yaml")
		c.Assert(err, qt.ErrorMatches, `(?s)failed to parse lint config .*field severty not found in type lint.Config.*`)
	})

	c.Assert(writeFile(dir+"/unknown-rule-key.yaml", "rules:\n  data-dependent/alter-column-type:\n    severty: error\n"), qt.IsNil)
	t.Run("unknown nested rule key", func(t *testing.T) {
		c := qt.New(t)
		_, err := lint.LoadConfig(builtintest.Runtime(), dir+"/unknown-rule-key.yaml")
		c.Assert(err, qt.ErrorMatches, `(?s)failed to parse lint config .*field severty not found in type lint.RuleConfig.*`)
	})

	c.Assert(writeFile(dir+"/lowercase-selector.yaml", "rules:\n  ds103:\n    severity: error\n"), qt.IsNil)
	t.Run("lowercase rule selector", func(t *testing.T) {
		c := qt.New(t)
		_, err := lint.LoadConfig(builtintest.Runtime(), dir+"/lowercase-selector.yaml")
		c.Assert(err, qt.ErrorMatches, `failed to parse lint config .*rule selector "ds103" must start with an uppercase ASCII letter.*`)
	})

	c.Assert(writeFile(dir+"/whitespace-selector.yaml", "disabled-rules:\n  - ' data-dependent/alter-column-type '\n"), qt.IsNil)
	t.Run("whitespace-padded rule selector", func(t *testing.T) {
		c := qt.New(t)
		_, err := lint.LoadConfig(builtintest.Runtime(), dir+"/whitespace-selector.yaml")
		c.Assert(err, qt.ErrorMatches, `failed to parse lint config .*rule selector " data-dependent/alter-column-type " must start with an uppercase ASCII letter.*`)
	})

	c.Assert(writeFile(dir+"/multiple-documents.yaml", "dialect: postgres\n---\ndialect: sqlite\n"), qt.IsNil)
	t.Run("multiple YAML documents", func(t *testing.T) {
		c := qt.New(t)
		_, err := lint.LoadConfig(builtintest.Runtime(), dir+"/multiple-documents.yaml")
		c.Assert(err, qt.ErrorMatches, `failed to parse lint config .*multiple YAML documents are not supported`)
	})
}

func TestLoadConfigFS_MissingConventionalFileReturnsEmptyConfig(t *testing.T) {
	c := qt.New(t)

	cfg, err := lint.LoadConfigFS(builtintest.Runtime(), fstest.MapFS{}, lint.ConfigFileName)

	c.Assert(err, qt.IsNil)
	c.Assert(cfg, qt.DeepEquals, &lint.Config{})
}

func TestLintFS_FailurePath_InvalidProgrammaticExclusionGlob(t *testing.T) {
	c := qt.New(t)
	fsys := fixture(map[string]string{
		"0000000001_drop.up.sql":   "DROP TABLE users;\n",
		"0000000001_drop.down.sql": "CREATE TABLE users (id INTEGER);\n",
	})

	findings, err := lint.LintFS(c.Context(), builtintest.Runtime(), fsys, lint.Options{
		RuleConfigs: map[string]lint.RuleConfig{
			"destructive/drop-table": {Exclude: []string{"[legacy/**"}},
		},
	})

	c.Assert(err, qt.ErrorMatches, `rule destructive/drop-table has invalid exclude pattern "\[legacy/\*\*": syntax error in pattern`)
	c.Assert(findings, qt.IsNil)
}

func TestValidateOptions_FailurePath_InvalidProgrammaticExclusionGlob(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		wantErr string
	}{
		{
			name:    "non-normalized parent segment",
			pattern: "**/../**",
			wantErr: `rule destructive/drop-table has invalid exclude pattern "\*\*/\.\./\*\*": pattern must be a normalized slash-separated path`,
		},
		{
			name:    "current directory",
			pattern: ".",
			wantErr: `rule destructive/drop-table has invalid exclude pattern "\.": pattern must not contain \. or \.\. path segments`,
		},
		{
			name:    "parent directory",
			pattern: "..",
			wantErr: `rule destructive/drop-table has invalid exclude pattern "\.\.": pattern must not contain \. or \.\. path segments`,
		},
		{
			name:    "leading parent directory",
			pattern: "../legacy/**",
			wantErr: `rule destructive/drop-table has invalid exclude pattern "\.\./legacy/\*\*": pattern must not contain \. or \.\. path segments`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := lint.ValidateOptions(builtintest.Runtime(), lint.Options{
				RuleConfigs: map[string]lint.RuleConfig{
					"destructive/drop-table": {Exclude: []string{test.pattern}},
				},
			})

			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

func TestLoadConfig_FailurePath_DotSegmentExclusionGlobs(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()

	tests := []struct {
		name    string
		pattern string
	}{
		{name: "current directory", pattern: "."},
		{name: "parent directory", pattern: ".."},
		{name: "leading parent directory", pattern: "../legacy/**"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			configPath := filepath.Join(dir, test.name+".yaml")
			contents := fmt.Sprintf("rules:\n  destructive/drop-table:\n    exclude:\n      - %q\n", test.pattern)
			c.Assert(writeFile(configPath, contents), qt.IsNil)

			_, err := lint.LoadConfig(builtintest.Runtime(), configPath)

			c.Assert(err, qt.ErrorMatches, `failed to parse lint config .*pattern must not contain \. or \.\. path segments`)
		})
	}
}

func TestRules_EveryRuleHasCodeTitleAndOneChecker(t *testing.T) {
	c := qt.New(t)

	// A runtime with no owner holds the rules this package checks itself,
	// and each of them carries its checker; an owner's rule carries none.
	for _, rule := range must.Must(lint.Rules(must.Must(engine.New()))) {
		oneChecker := (rule.CheckStatement != nil) != (rule.CheckFile != nil)
		c.Assert(oneChecker, qt.IsTrue, qt.Commentf("rule %s must set exactly one checker", rule.Name))
	}
	seen := make(map[string]bool)
	for _, rule := range must.Must(lint.Rules(builtintest.Runtime())) {
		c.Assert(rule.Name, qt.Not(qt.Equals), "")
		c.Assert(rule.Title, qt.Not(qt.Equals), "")
		c.Assert(seen[rule.Name], qt.IsFalse, qt.Commentf("duplicate rule code %s", rule.Name))
		seen[rule.Name] = true
		// All three, not two. SeverityInfo is a designed severity -- reported
		// and never gated on, so a rule can be introduced to a repository that
		// still violates it (stokaro/ptah#1633) -- and the two-way assertion
		// was a snapshot of a registry that happened to have none.
		c.Assert(rule.Severity == lint.SeverityWarning ||
			rule.Severity == lint.SeverityError ||
			rule.Severity == lint.SeverityInfo, qt.IsTrue,
			qt.Commentf("rule %s has severity %q", rule.Name, rule.Severity))
	}
}

func writeFile(path, content string) error {
	return os.WriteFile(path, []byte(content), 0o600)
}
