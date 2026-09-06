package lint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/lint"
	"ptah.run/migration/migrationfile"
)

// snakeCase is the convention every case in this file enforces unless it
// says otherwise: lower snake case, which `Users`, `createdAt` and
// `IDX_orders` all violate.
var snakeCase = &lint.NamingConfig{Match: "^[a-z][a-z0-9_]*$", Message: "use lower snake case"}

func analyzeNaming(c *qt.C, dialect, migration string, naming *lint.NamingConfig) lint.Analysis {
	c.Helper()
	analysis, err := lint.AnalyzeFS(fixture(map[string]string{
		"1_base.sql":   "CREATE TABLE orders (id int, email text);",
		"2_change.sql": migration,
	}), lint.Options{
		Dialect:   dialect,
		DirFormat: migrationfile.DirFormatAtlas,
		Selection: lint.VersionSelection{Versions: []int64{2}, Restricted: true},
		Naming:    naming,
	})
	c.Assert(err, qt.IsNil)
	return analysis
}

// namingCodes keeps the codes of this family.
func namingCodes(codes []string) []string {
	var kept []string
	for _, code := range codes {
		if len(code) == 5 && code[:2] == "NM" {
			kept = append(kept, code)
		}
	}
	return kept
}

// TestNamingRules_ReportEveryNameAMigrationIntroduces pins each rule to the
// kind of name it judges, across the spellings the dialects use.
func TestNamingRules_ReportEveryNameAMigrationIntroduces(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		migration string
		want      []string
		message   string
	}{
		{
			name:      "a schema created",
			dialect:   "postgres",
			migration: "CREATE SCHEMA Billing;",
			want:      []string{"NM101"},
			message:   "schema name Billing does not match the naming convention ^[a-z][a-z0-9_]*$: use lower snake case",
		},
		{
			name:      "a schema renamed to",
			dialect:   "postgres",
			migration: "ALTER SCHEMA app RENAME TO Billing;",
			want:      []string{"NM101"},
		},
		{
			name:      "a table created",
			dialect:   "postgres",
			migration: "CREATE TABLE Invoices (id int);",
			want:      []string{"NM102"},
			message:   "table name Invoices does not match",
		},
		{
			name:      "a quoted table name is judged without its quotes",
			dialect:   "postgres",
			migration: `CREATE TABLE "Invoices" (id int);`,
			want:      []string{"NM102"},
			message:   `table name "Invoices" does not match`,
		},
		{
			name:      "a table renamed to",
			dialect:   "postgres",
			migration: "ALTER TABLE orders RENAME TO Orders;",
			want:      []string{"NM102"},
		},
		{
			name:      "a MySQL table renamed without TO",
			dialect:   "mysql",
			migration: "ALTER TABLE orders RENAME Orders;",
			want:      []string{"NM102"},
		},
		{
			name:      "a column declared in CREATE TABLE",
			dialect:   "postgres",
			migration: "CREATE TABLE invoices (id int, createdAt timestamp);",
			want:      []string{"NM103"},
			message:   "column name createdAt does not match",
		},
		{
			name:      "a column added",
			dialect:   "postgres",
			migration: "ALTER TABLE orders ADD COLUMN createdAt timestamp;",
			want:      []string{"NM103"},
		},
		{
			name:      "a column renamed to",
			dialect:   "postgres",
			migration: "ALTER TABLE orders RENAME COLUMN email TO Email;",
			want:      []string{"NM103"},
		},
		{
			name:      "a column renamed through MySQL CHANGE",
			dialect:   "mysql",
			migration: "ALTER TABLE orders CHANGE email Email varchar(100);",
			want:      []string{"NM103"},
		},
		{
			name:      "an index created",
			dialect:   "postgres",
			migration: "CREATE INDEX IDX_orders_email ON orders (email);",
			want:      []string{"NM104"},
			message:   "index name IDX_orders_email does not match",
		},
		{
			name:      "a unique index created concurrently",
			dialect:   "postgres",
			migration: "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS IDX_orders_email ON orders (email);",
			want:      []string{"NM104"},
		},
		{
			name:      "a MySQL index added to a table",
			dialect:   "mysql",
			migration: "ALTER TABLE orders ADD INDEX IDX_orders_email (email);",
			want:      []string{"NM104"},
		},
		{
			name:      "a MySQL index declared inline",
			dialect:   "mysql",
			migration: "CREATE TABLE invoices (id int, number int, KEY IDX_number (number));",
			want:      []string{"NM104"},
		},
		{
			name:      "a unique constraint is an index",
			dialect:   "postgres",
			migration: "ALTER TABLE orders ADD CONSTRAINT UQ_orders_email UNIQUE (email);",
			want:      []string{"NM104"},
		},
		{
			name:      "an index renamed to",
			dialect:   "postgres",
			migration: "ALTER INDEX orders_email_idx RENAME TO IDX_orders_email;",
			want:      []string{"NM104"},
		},
		{
			name:      "a foreign key added",
			dialect:   "postgres",
			migration: "ALTER TABLE orders ADD CONSTRAINT FK_orders_customer FOREIGN KEY (customer_id) REFERENCES customers (id);",
			want:      []string{"NM105"},
			message:   "foreign key name FK_orders_customer does not match",
		},
		{
			name:      "a foreign key declared inline",
			dialect:   "mysql",
			migration: "CREATE TABLE invoices (id int, order_id int, CONSTRAINT FK_invoices_order FOREIGN KEY (order_id) REFERENCES orders (id));",
			want:      []string{"NM105"},
		},
		{
			name:      "a check constraint added",
			dialect:   "postgres",
			migration: "ALTER TABLE orders ADD CONSTRAINT CK_orders_total CHECK (total >= 0);",
			want:      []string{"NM106"},
			message:   "check constraint name CK_orders_total does not match",
		},
		{
			name:      "a check constraint declared inline",
			dialect:   "postgres",
			migration: "CREATE TABLE invoices (id int, total int, CONSTRAINT CK_total CHECK (total >= 0));",
			want:      []string{"NM106"},
		},
		{
			name:      "every kind at once, one finding each",
			dialect:   "postgres",
			migration: "CREATE TABLE Invoices (Id int, CONSTRAINT PK_invoices PRIMARY KEY (Id), CONSTRAINT FK_order FOREIGN KEY (Id) REFERENCES orders (id), CONSTRAINT CK_id CHECK (Id > 0));",
			want:      []string{"NM102", "NM103", "NM104", "NM105", "NM106"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzeNaming(c, test.dialect, test.migration, snakeCase)
			c.Assert(namingCodes(rulesOf(analysis.Findings())), qt.DeepEquals, test.want)
			c.Assert(messageOf(analysis.Findings(), test.want[0]), qt.Contains, test.message)
		})
	}
}

// TestNamingRules_StayQuietWhereNoNameIsIntroduced holds the names the rules
// leave alone: ones that satisfy the convention, ones the migration only
// refers to, and every name when no convention is configured.
func TestNamingRules_StayQuietWhereNoNameIsIntroduced(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		migration string
		naming    *lint.NamingConfig
	}{
		{
			name:      "names that satisfy the convention",
			dialect:   "postgres",
			migration: "CREATE TABLE invoices (id int, created_at timestamp, CONSTRAINT fk_order FOREIGN KEY (id) REFERENCES orders (id));\nCREATE INDEX idx_invoices_created ON invoices (created_at);",
			naming:    snakeCase,
		},
		{
			name:      "a table only referred to",
			dialect:   "postgres",
			migration: "ALTER TABLE Orders ADD COLUMN total int;",
			naming:    snakeCase,
		},
		{
			name:      "a rename judges the new name, not the old one",
			dialect:   "postgres",
			migration: "ALTER TABLE Orders RENAME TO orders;",
			naming:    snakeCase,
		},
		{
			name:      "a MySQL CHANGE that keeps the name introduces none",
			dialect:   "mysql",
			migration: "ALTER TABLE orders CHANGE Email Email varchar(100);",
			naming:    snakeCase,
		},
		{
			name:      "a quoted name that satisfies the convention",
			dialect:   "postgres",
			migration: `CREATE TABLE "invoices" ("id" int); CREATE INDEX "idx_invoices_id" ON "invoices" ("id");`,
			naming:    snakeCase,
		},
		{
			name:      "a column only referred to",
			dialect:   "postgres",
			migration: "ALTER TABLE orders DROP COLUMN Email;\nCREATE INDEX idx_orders_email ON orders (Email);",
			naming:    snakeCase,
		},
		{
			name:      "an unnamed constraint or index",
			dialect:   "postgres",
			migration: "ALTER TABLE orders ADD FOREIGN KEY (id) REFERENCES customers (id), ADD CHECK (id > 0);\nCREATE INDEX ON orders (email);",
			naming:    snakeCase,
		},
		{
			name:      "no convention configured",
			dialect:   "postgres",
			migration: "CREATE TABLE Invoices (Id int);",
			naming:    nil,
		},
		{
			name:      "a kind the convention does not cover",
			dialect:   "postgres",
			migration: "CREATE TABLE Invoices (Id int);",
			naming:    &lint.NamingConfig{Index: &lint.NamingPattern{Match: "^idx_"}},
		},
		{
			name:      "a schema-qualified name is judged on its own part",
			dialect:   "postgres",
			migration: "CREATE TABLE Billing.invoices (id int);",
			naming:    snakeCase,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzeNaming(c, test.dialect, test.migration, test.naming)
			c.Assert(namingCodes(rulesOf(analysis.Findings())), qt.HasLen, 0)
		})
	}
}

// TestNamingRules_AKindsOwnPatternReplacesTheDefault: an index convention
// of its own beats the shared one, and carries its own message.
func TestNamingRules_AKindsOwnPatternReplacesTheDefault(t *testing.T) {
	c := qt.New(t)

	analysis := analyzeNaming(c, "postgres",
		"CREATE INDEX orders_email ON orders (email);\nCREATE TABLE Invoices (id int);",
		&lint.NamingConfig{
			Match:   "^[a-z][a-z0-9_]*$",
			Message: "use lower snake case",
			Index:   &lint.NamingPattern{Match: "^idx_[a-z_]+$", Message: "prefix an index with idx_"},
		})

	c.Assert(namingCodes(rulesOf(analysis.Findings())), qt.DeepEquals, []string{"NM104", "NM102"})
	c.Assert(messageOf(analysis.Findings(), "NM104"), qt.Equals, "index name orders_email does not match the naming convention ^idx_[a-z_]+$: prefix an index with idx_")
	c.Assert(messageOf(analysis.Findings(), "NM102"), qt.Equals, "table name Invoices does not match the naming convention ^[a-z][a-z0-9_]*$: use lower snake case")
}

// TestNamingRules_SeverityComesFromTheBlock: the block's severity is what
// the findings report at, and a rules: entry for one code still wins.
func TestNamingRules_SeverityComesFromTheBlock(t *testing.T) {
	c := qt.New(t)

	analysis, err := lint.AnalyzeFS(fixture(map[string]string{
		"1_base.sql":   "CREATE TABLE orders (id int);",
		"2_change.sql": "CREATE TABLE Invoices (Id int);",
	}), lint.Options{
		Dialect:     "postgres",
		DirFormat:   migrationfile.DirFormatAtlas,
		Selection:   lint.VersionSelection{Versions: []int64{2}, Restricted: true},
		Naming:      &lint.NamingConfig{Match: "^[a-z_]+$", Severity: lint.SeverityError},
		RuleConfigs: map[string]lint.RuleConfig{"NM103": {Severity: lint.SeverityInfo}},
	})

	c.Assert(err, qt.IsNil)
	findings := analysis.Findings()
	c.Assert(rulesOf(findings), qt.DeepEquals, []string{"NM102", "NM103"})
	c.Assert(findings[0].Severity, qt.Equals, lint.SeverityError)
	c.Assert(findings[1].Severity, qt.Equals, lint.SeverityInfo)
	c.Assert(findings[0].Context.Subjects, qt.DeepEquals, []lint.Subject{{Kind: lint.SubjectTable, Name: "Invoices"}})
	c.Assert(findings[1].Context.Subjects, qt.DeepEquals, []lint.Subject{{Kind: lint.SubjectColumn, Name: "Id", Parent: "Invoices"}})
}

func TestNamingRules_AreSelectedByTheirCodes(t *testing.T) {
	c := qt.New(t)

	analysis, err := lint.AnalyzeFS(fixture(map[string]string{
		"1_base.sql":   "CREATE TABLE orders (id int);",
		"2_change.sql": "CREATE TABLE Invoices (Id int);",
	}), lint.Options{
		Dialect:   "postgres",
		DirFormat: migrationfile.DirFormatAtlas,
		Selection: lint.VersionSelection{Versions: []int64{2}, Restricted: true},
		Naming:    snakeCase,
		Disabled:  []string{"NM102"},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(namingCodes(rulesOf(analysis.Findings())), qt.DeepEquals, []string{"NM103"})
}

// TestNamingConfig_IsRefusedWhenItCannotEnforceAnything holds the parse-time
// contract of the block: a pattern that does not compile, a kind block with
// no pattern, a block with no pattern anywhere, and an unknown severity all
// fail before any file is read.
func TestNamingConfig_IsRefusedWhenItCannotEnforceAnything(t *testing.T) {
	tests := []struct {
		name    string
		yaml    string
		wantErr string
	}{
		{
			name:    "a pattern that does not compile",
			yaml:    "naming:\n  match: '^[a-z'\n",
			wantErr: `.*naming: match "\^\[a-z" is not a valid regular expression: .*`,
		},
		{
			name:    "a kind pattern that does not compile",
			yaml:    "naming:\n  match: '^[a-z_]+$'\n  index:\n    match: '('\n",
			wantErr: `.*naming\.index: match "\(" is not a valid regular expression: .*`,
		},
		{
			name:    "a kind block without a match",
			yaml:    "naming:\n  match: '^[a-z_]+$'\n  table:\n    message: tables are snake case\n",
			wantErr: `.*naming\.table: a pattern block needs a match`,
		},
		{
			name:    "no pattern at all",
			yaml:    "naming:\n  message: be consistent\n",
			wantErr: `.*naming: no match pattern is set, so the block would enforce nothing`,
		},
		{
			name:    "an unknown severity",
			yaml:    "naming:\n  match: '^[a-z_]+$'\n  severity: fatal\n",
			wantErr: `.*naming: unsupported severity "fatal": expected info, warning or error`,
		},
		{
			name:    "an unknown key",
			yaml:    "naming:\n  match: '^[a-z_]+$'\n  view:\n    match: '^v_'\n",
			wantErr: `(?s).*field view not found.*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, err := lint.LoadConfigFS(fixture(map[string]string{".ptah-lint.yaml": test.yaml}), ".ptah-lint.yaml")
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

func TestNamingConfig_LoadsFromThePolicyFile(t *testing.T) {
	c := qt.New(t)

	cfg, err := lint.LoadConfigFS(fixture(map[string]string{".ptah-lint.yaml": "naming:\n  match: '^[a-z_]+$'\n  message: snake case\n  severity: error\n  foreign-key:\n    match: '^fk_'\n"}), ".ptah-lint.yaml")

	c.Assert(err, qt.IsNil)
	c.Assert(cfg.Naming, qt.DeepEquals, &lint.NamingConfig{
		Match:      "^[a-z_]+$",
		Message:    "snake case",
		Severity:   lint.SeverityError,
		ForeignKey: &lint.NamingPattern{Match: "^fk_"},
	})
}
