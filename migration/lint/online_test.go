package lint_test

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/config/projectconfig"
	"ptah.run/engine/builtin"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/migrationlintreport"
	"ptah.run/migration/lint"
)

// onlineRuleCodes lints one up statement under the online mode and returns the
// ON codes it reported.
func onlineRuleCodes(c *qt.C, dialect, sql string) []string {
	c.Helper()
	findings, err := lint.LintFS(c.Context(), builtintest.Runtime(),
		fixture(map[string]string{"0000000001_x.up.sql": sql}),
		lint.Options{Dialect: dialect, RequireOnline: true},
	)
	c.Assert(err, qt.IsNil)
	codes := make([]string, 0, len(findings))
	for _, finding := range findings {
		if strings.HasPrefix(finding.Rule, lint.OnlineCategory+"/") {
			codes = append(codes, finding.Rule)
		}
	}
	return codes
}

// TestOnlineMode_PostgresProvesTheMeasuredSet is the allowlist working: each
// statement here is in the set measured to take no lock conflicting with reads
// and writes, so the mode reports nothing about it.
func TestOnlineMode_PostgresProvesTheMeasuredSet(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{name: "concurrent index build", sql: "CREATE INDEX CONCURRENTLY idx ON users (email);"},
		{name: "concurrent index drop", sql: "DROP INDEX CONCURRENTLY idx;"},
		{name: "plain column addition", sql: "ALTER TABLE users ADD COLUMN nickname text;"},
		{name: "column addition with a default", sql: "ALTER TABLE users ADD COLUMN tier text DEFAULT 'free';"},
		{name: "column drop", sql: "ALTER TABLE users DROP COLUMN nickname;"},
		{name: "default set", sql: "ALTER TABLE users ALTER COLUMN tier SET DEFAULT 'free';"},
		{name: "not null dropped", sql: "ALTER TABLE users ALTER COLUMN tier DROP NOT NULL;"},
		{name: "constraint added without its scan", sql: "ALTER TABLE users ADD CONSTRAINT ck CHECK (id > 0) NOT VALID;"},
		{name: "the validation that completes it", sql: "ALTER TABLE users VALIDATE CONSTRAINT ck;"},
		{name: "a new table locks nothing that exists", sql: "CREATE TABLE t (id bigint PRIMARY KEY);"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(onlineRuleCodes(c, "postgres", test.sql), qt.HasLen, 0)
		})
	}
}

// TestOnlineMode_PostgresReportsEverythingElse is the half that makes it a
// guarantee. The last row is the one that separates an allowlist from a
// blocklist: a statement no rule in this package describes is reported here
// because it was not proven, not because somebody anticipated it.
func TestOnlineMode_PostgresReportsEverythingElse(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{name: "blocking index build", sql: "CREATE INDEX idx ON users (email);"},
		{name: "blocking index drop", sql: "DROP INDEX idx;"},
		{name: "not null added", sql: "ALTER TABLE users ALTER COLUMN tier SET NOT NULL;"},
		{name: "type change", sql: "ALTER TABLE users ALTER COLUMN id TYPE bigint;"},
		{name: "constraint added with its scan", sql: "ALTER TABLE users ADD CONSTRAINT ck CHECK (id > 0);"},
		{name: "primary key added", sql: "ALTER TABLE users ADD CONSTRAINT pk PRIMARY KEY (id) NOT VALID;"},
		{name: "column added with a constraint", sql: "ALTER TABLE users ADD COLUMN email text NOT NULL;"},
		{name: "a statement no rule here describes", sql: "CLUSTER users USING idx;"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(onlineRuleCodes(c, "postgres", test.sql), qt.DeepEquals, []string{"online/unproven"})
		})
	}
}

// TestOnlineMode_MySQLAsksTheServer holds the shorter and stronger rule: a
// statement is proven online when it asked the server to refuse what it cannot
// do online, and not otherwise.
func TestOnlineMode_MySQLAsksTheServer(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "the request the planner writes",
			sql:  "ALTER TABLE users ADD COLUMN nickname VARCHAR(64), ALGORITHM=INPLACE, LOCK=NONE;",
			want: make([]string, 0),
		},
		{
			name: "an instant change needs no lock level beside it",
			sql:  "ALTER TABLE users ADD COLUMN nickname VARCHAR(64), ALGORITHM=INSTANT;",
			want: make([]string, 0),
		},
		{
			name: "a new table locks nothing that exists",
			sql:  "CREATE TABLE t (id BIGINT PRIMARY KEY);",
			want: make([]string, 0),
		},
		{
			name: "the same change with no request at all",
			sql:  "ALTER TABLE users ADD COLUMN nickname VARCHAR(64);",
			want: []string{"online/unproven"},
		},
		{
			name: "an algorithm without a lock level says nothing about writes",
			sql:  "ALTER TABLE users ADD COLUMN nickname VARCHAR(64), ALGORITHM=INPLACE;",
			want: []string{"online/unproven"},
		},
		{
			name: "a copy asked for by name",
			sql:  "ALTER TABLE users ADD COLUMN nickname VARCHAR(64), ALGORITHM=COPY;",
			want: []string{"online/unproven"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(onlineRuleCodes(c, "mysql", test.sql), qt.DeepEquals, test.want)
		})
	}
}

// TestOnlineMode_StaysSilentWhenNobodyAskedForIt is the control the whole
// family needs. An allowlist left on by default would report most statements
// of most migrations, so a run that did not select the mode must see none of
// it.
func TestOnlineMode_StaysSilentWhenNobodyAskedForIt(t *testing.T) {
	c := qt.New(t)

	findings, err := lint.LintFS(c.Context(), builtintest.Runtime(),
		fixture(map[string]string{"0000000001_x.up.sql": "CREATE INDEX idx ON users (email);"}),
		lint.Options{Dialect: "postgres"},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(slices.ContainsFunc(findings, func(finding lint.Finding) bool {
		return strings.HasPrefix(finding.Rule, lint.OnlineCategory+"/")
	}), qt.IsFalse)
}

// The mode is refused on an engine nothing here has measured. A run that
// proved nothing and reported nothing is the one answer this family must never
// give, so the refusal happens before any analysis.
func TestOnlineMode_RefusesAnUnmeasuredEngine(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "sqlite rebuilds the table for most changes", dialect: "sqlite"},
		{name: "cockroachdb is online by design and unmeasured here", dialect: "cockroachdb"},
		{name: "sqlserver answers differently by edition", dialect: "sqlserver"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := lint.ValidateOnlineDialect(test.dialect)

			c.Assert(err, qt.ErrorMatches, `online mode covers postgres, mysql, mariadb; it is refused on ".*" .*`)
		})
	}
}

// The control: the three engines the mode covers are accepted.
func TestOnlineMode_AcceptsTheMeasuredEngines(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "postgres", dialect: "postgres"},
		{name: "mysql", dialect: "mysql"},
		{name: "mariadb", dialect: "mariadb"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(lint.ValidateOnlineDialect(test.dialect), qt.IsNil)
		})
	}
}

// A policy that selects the mode refuses the apply on its findings without
// naming the category under `gate` as well: two ways to say one thing would
// leave a mode that reported and ran.
func TestOnlineMode_GateBlocksOnTheCategory(t *testing.T) {
	c := qt.New(t)
	cfg := &lint.Config{Dialect: "postgres", Online: lint.OnlineRequire}

	c.Assert(cfg.GateRules(), qt.DeepEquals, append(slices.Clone(lint.DefaultGateRules), lint.OnlineCategory))
}

// The control: a policy that did not select the mode leaves the gate where it
// was.
func TestOnlineMode_GateIsUnchangedWithoutTheMode(t *testing.T) {
	c := qt.New(t)
	cfg := &lint.Config{Dialect: "postgres"}

	c.Assert(cfg.GateRules(), qt.DeepEquals, lint.DefaultGateRules)
}

// A column addition is free only when the catalog can answer for the rows that
// already exist. A generated column and a default the server has to call are
// computed per row, under the lock the ALTER took.
func TestOnlineMode_PostgresReportsARewritingColumnAddition(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{name: "identity column", sql: "ALTER TABLE users ADD COLUMN n int GENERATED ALWAYS AS IDENTITY;"},
		{name: "stored generated column", sql: "ALTER TABLE users ADD COLUMN n int GENERATED ALWAYS AS (id * 2) STORED;"},
		{name: "volatile default", sql: "ALTER TABLE users ADD COLUMN token text DEFAULT random()::text;"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(onlineRuleCodes(c, "postgres", test.sql), qt.DeepEquals, []string{"online/unproven"})
		})
	}
}

// The control: a constant default is still catalog-only, so the rule above did
// not take the ordinary addition with it.
func TestOnlineMode_PostgresProvesAConstantDefault(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{name: "a text constant", sql: "ALTER TABLE users ADD COLUMN tier text DEFAULT 'free';"},
		{name: "a number", sql: "ALTER TABLE users ADD COLUMN score int DEFAULT 0;"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(onlineRuleCodes(c, "postgres", test.sql), qt.HasLen, 0)
		})
	}
}

// PostgreSQL holds a lock until the transaction commits, so two statements
// each proven online are not an online migration: the validation's scan runs
// behind the ACCESS EXCLUSIVE lock the addition took.
func TestOnlineMode_PostgresReportsALockHeldAcrossAScan(t *testing.T) {
	c := qt.New(t)
	sql := "ALTER TABLE users ADD COLUMN tier text;\n" +
		"ALTER TABLE users ADD CONSTRAINT ck CHECK (id > 0) NOT VALID;\n" +
		"ALTER TABLE users VALIDATE CONSTRAINT ck;\n"

	c.Assert(onlineRuleCodes(c, "postgres", sql), qt.DeepEquals, []string{"online/lock-held-across-scan"})
}

// Two controls for it. A file that opted out of the transaction commits each
// statement, so the validation takes only its own weaker lock; and a
// validation with nothing before it holds nothing either.
func TestOnlineMode_PostgresAcceptsAValidationThatHoldsNothing(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "the pair in a migration of its own",
			sql: "-- +ptah no_transaction\n" +
				"ALTER TABLE users ADD CONSTRAINT ck CHECK (id > 0) NOT VALID;\n" +
				"ALTER TABLE users VALIDATE CONSTRAINT ck;\n",
		},
		{
			name: "a validation with nothing before it",
			sql:  "ALTER TABLE users VALIDATE CONSTRAINT ck;\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(onlineRuleCodes(c, "postgres", test.sql), qt.HasLen, 0)
		})
	}
}

// A policy that requires the mode and then takes its findings away is a policy
// that does not require the mode. Each of these would make the stated
// guarantee advisory without saying so.
func TestOnlineMode_RefusesAPolicyThatSoftensIt(t *testing.T) {
	tests := []struct {
		name    string
		config  string
		wantErr string
	}{
		{
			name:    "the family disabled",
			config:  "dialect: postgres\nonline: require\ndisabled-rules: [ON]\n",
			wantErr: `.*lint rules are named category/name; ON is not a rule name`,
		},
		{
			name:    "the category disabled",
			config:  "dialect: postgres\nonline: require\ndisabled-rules: [online]\n",
			wantErr: `.*disabled-rules names "online".*`,
		},
		{
			name:    "the category disabled with a slash",
			config:  "dialect: postgres\nonline: require\ndisabled-rules: [online/]\n",
			wantErr: `.*disabled-rules names "online/".*`,
		},
		{
			name:    "a rule dropped below blocking",
			config:  "dialect: postgres\nonline: require\nrules:\n  online/unproven:\n    severity: warning\n",
			wantErr: `.*rules.online/unproven sets severity "warning".*`,
		},
		{
			name:    "a rule dropped below blocking by its name",
			config:  "dialect: postgres\nonline: require\nrules:\n  online/lock-held-across-scan:\n    severity: warning\n",
			wantErr: `.*rules.online/lock-held-across-scan sets severity "warning".*`,
		},
		{
			name:    "paths excluded from it",
			config:  "dialect: postgres\nonline: require\nrules:\n  online/unproven:\n    exclude: [\"legacy/*\"]\n",
			wantErr: `.*rules.online/unproven excludes paths.*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			path := filepath.Join(dir, ".ptah-lint.yaml")
			c.Assert(os.WriteFile(path, []byte(test.config), 0o600), qt.IsNil)

			_, err := lint.LoadConfig(builtintest.Runtime(), path)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

// TestOnlineMode_KeepsAPolicyThatDisablesLocking is the control for the
// category rows above: the online rules are named under their own category,
// so disabling locking leaves the mode's findings in force.
func TestOnlineMode_KeepsAPolicyThatDisablesLocking(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(c.TempDir(), ".ptah-lint.yaml")
	c.Assert(os.WriteFile(path, []byte("dialect: postgres\nonline: require\ndisabled-rules: [locking]\n"), 0o600), qt.IsNil)

	cfg, err := lint.LoadConfig(builtintest.Runtime(), path)

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.DisabledRules, qt.DeepEquals, []string{"locking"})
}

// The standalone lint and the apply gate read the same policy, or the two
// disagree about the same directory: a `migrations lint` that reported a
// blocking migration clean and a `migrations up` that refused it is a pair an
// operator cannot act on.
func TestOnlineMode_StandaloneLintHonorsTheSameSelection(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, ".ptah-lint.yaml"),
		[]byte("dialect: postgres\nonline: require\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_x.up.sql"),
		[]byte("CREATE INDEX idx ON users (email);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_x.down.sql"),
		[]byte("-- +ptah no_transaction\nDROP INDEX CONCURRENTLY idx;\n"), 0o600), qt.IsNil)

	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	report, err := migrationlintreport.Build(c.Context(), migrationlintreport.Options{
		Runtime: runtime,
		Dir:     dir,
		FailOn:  migrationlintreport.FailOnError,
		Changed: migrationlintreport.ChangedOptions{Dir: true},
	}, projectconfig.Config{})

	c.Assert(err, qt.IsNil)
	c.Assert(slices.ContainsFunc(report.Findings, func(finding lint.Finding) bool {
		return finding.Rule == "online/unproven"
	}), qt.IsTrue, qt.Commentf("findings: %v", rulesOf(report.Findings)))
}
