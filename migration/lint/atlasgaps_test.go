package lint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/lintext"
	"ptah.run/internal/builtintest"
	"ptah.run/migration/lint"
	"ptah.run/migration/migrationfile"
)

// gapFS is the fixture the gap cases analyze: one table created in an earlier
// version, then one statement against it.
//
// The two files matter. A rule that exempts an object the migration itself
// created must see the drop in a version that did NOT create it, or every
// hazardous case would be silently exempt and the test would assert nothing.
func gapFS(statement string) map[string]string {
	return map[string]string{
		"1_base.sql":   "CREATE TABLE orders (id int NOT NULL, total int, note varchar(10));",
		"2_change.sql": statement,
	}
}

func gapOptions(dialect string) lint.Options {
	return lint.Options{
		Dialect:   dialect,
		DirFormat: migrationfile.DirFormatAtlas,
		Selection: lint.VersionSelection{Versions: []int64{2}, Restricted: true},
	}
}

// analyzeGap runs the real analyzer over the fixture and returns the codes it
// reported for version 2.
func analyzeGap(c *qt.C, dialect, statement string) []string {
	c.Helper()
	analysis, err := lint.AnalyzeFS(c.Context(), builtintest.Runtime(), fixture(gapFS(statement)), gapOptions(dialect))
	c.Assert(err, qt.IsNil)
	var codes []string
	for _, finding := range analysis.Findings() {
		codes = append(codes, finding.Rule)
	}
	return codes
}

// gapMessage returns the message one rule reported, or "".
func gapMessage(c *qt.C, dialect, statement, rule string) string {
	c.Helper()
	analysis, err := lint.AnalyzeFS(c.Context(), builtintest.Runtime(), fixture(gapFS(statement)), gapOptions(dialect))
	c.Assert(err, qt.IsNil)
	return messageOf(analysis.Findings(), rule)
}

// TestAtlasGapRules_ReportTheHazard drives every rule this slice added through
// the analyzer on the statement it is about.
//
// The control beside each one is in the failure-path test below, and the two
// are what make the pair mean something: a rule that fired on both would be
// reporting the statement shape rather than the hazard.
func TestAtlasGapRules_ReportTheHazard(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		statement string
		want      string
	}{
		{
			name:      "a dropped table retires a name clients query",
			dialect:   "postgres",
			statement: "DROP TABLE orders;",
			want:      "rollout/drop-table",
		},
		{
			name:      "a dropped column retires a name clients select",
			dialect:   "postgres",
			statement: "ALTER TABLE orders DROP COLUMN note;",
			want:      "rollout/drop-column",
		},
		{
			name:      "an exclusion constraint takes an exclusive lock",
			dialect:   "postgres",
			statement: "ALTER TABLE orders ADD CONSTRAINT no_overlap EXCLUDE USING gist (total WITH =);",
			want:      "locking/add-exclusion-constraint",
		},
		{
			name:      "a redefined primary key builds its index under the lock",
			dialect:   "postgres",
			statement: `ALTER TABLE orders DROP CONSTRAINT orders_pkey, ADD PRIMARY KEY (total);`,
			want:      "locking/replace-primary-key",
		},
		{
			name:      "replica identity full changes what replication carries",
			dialect:   "postgres",
			statement: "ALTER TABLE orders REPLICA IDENTITY FULL;",
			want:      "rollout/weaken-replica-identity",
		},
		{
			name:      "autovacuum disabled leaves dead rows",
			dialect:   "postgres",
			statement: "ALTER TABLE orders SET (autovacuum_enabled = false);",
			want:      "design/disable-autovacuum",
		},
		{
			name:      "a redefined primary key rebuilds the secondary indexes",
			dialect:   "mysql",
			statement: "ALTER TABLE orders DROP PRIMARY KEY, ADD PRIMARY KEY (total);",
			want:      "rewrite/replace-primary-key",
		},
		{
			name:      "a storage engine change copies the table",
			dialect:   "mysql",
			statement: "ALTER TABLE orders ENGINE=InnoDB;",
			want:      "rewrite/change-engine",
		},
		{
			name:      "partitioning rewrites every row",
			dialect:   "mysql",
			statement: "ALTER TABLE orders PARTITION BY HASH(id) PARTITIONS 4;",
			want:      "rewrite/change-partitioning",
		},
		{
			name:      "removing partitioning rewrites every row",
			dialect:   "mysql",
			statement: "ALTER TABLE orders REMOVE PARTITIONING;",
			want:      "rewrite/change-partitioning",
		},
		{
			name:      "a stored generated column is computed for every row",
			dialect:   "mysql",
			statement: "ALTER TABLE orders ADD COLUMN doubled int GENERATED ALWAYS AS (total * 2) STORED;",
			want:      "rewrite/add-stored-generated-column",
		},
		{
			name:      "an auto-increment column numbers every row",
			dialect:   "mysql",
			statement: "ALTER TABLE orders ADD COLUMN seq int NOT NULL AUTO_INCREMENT, ADD KEY (seq);",
			want:      "locking/add-auto-increment-column",
		},
		{
			name:      "a modified generated column is recomputed for every row",
			dialect:   "mysql",
			statement: "ALTER TABLE orders MODIFY COLUMN doubled bigint GENERATED ALWAYS AS (total * 3) STORED;",
			want:      "rewrite/modify-generated-column",
		},
		{
			name:      "a check constraint validates every row",
			dialect:   "mysql",
			statement: "ALTER TABLE orders ADD CONSTRAINT positive CHECK (total > 0);",
			want:      "locking/add-check",
		},
		{
			name:      "enforcing a check revalidates every row",
			dialect:   "mysql",
			statement: "ALTER TABLE orders ALTER CONSTRAINT positive ENFORCED;",
			want:      "locking/enforce-check",
		},
		{
			name:      "dropping system versioning deletes the history",
			dialect:   "mariadb",
			statement: "ALTER TABLE orders DROP SYSTEM VERSIONING;",
			want:      "destructive/drop-system-versioning",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			codes := analyzeGap(c, test.dialect, test.statement)

			c.Assert(codes, qt.Contains, test.want)
		})
	}
}

// TestAtlasGapRules_StayQuietOnTheControl is the other half.
//
// Each row is a statement close enough to the hazardous one to share its
// shape, and without the hazard. A rule that fires here is reporting the
// keyword rather than the consequence, which is the thing the assessment
// behind this slice refused to call coverage.
func TestAtlasGapRules_StayQuietOnTheControl(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		statement string
		quietRule string
	}{
		{
			name:      "a table this migration created is not a rollout break",
			dialect:   "postgres",
			statement: "CREATE TABLE staging (id int); DROP TABLE staging;",
			quietRule: "rollout/drop-table",
		},
		{
			name:      "adding a column retires no name",
			dialect:   "postgres",
			statement: "ALTER TABLE orders ADD COLUMN shipped boolean;",
			quietRule: "rollout/drop-column",
		},
		{
			name:      "a plain unique constraint is not an exclusion constraint",
			dialect:   "postgres",
			statement: "ALTER TABLE orders ADD CONSTRAINT total_unique UNIQUE (total);",
			quietRule: "locking/add-exclusion-constraint",
		},
		{
			name:      "a primary key attached to an existing index builds nothing",
			dialect:   "postgres",
			statement: `ALTER TABLE orders DROP CONSTRAINT orders_pkey, ADD CONSTRAINT orders_pkey PRIMARY KEY USING INDEX orders_new;`,
			quietRule: "locking/replace-primary-key",
		},
		{
			name:      "replica identity default keeps a usable row identity",
			dialect:   "postgres",
			statement: "ALTER TABLE orders REPLICA IDENTITY DEFAULT;",
			quietRule: "rollout/weaken-replica-identity",
		},
		{
			name:      "turning autovacuum back on is not the hazard",
			dialect:   "postgres",
			statement: "ALTER TABLE orders SET (autovacuum_enabled = true);",
			quietRule: "design/disable-autovacuum",
		},
		{
			name:      "adding a primary key is rewrite/add-primary-key's subject, not rewrite/replace-primary-key's",
			dialect:   "mysql",
			statement: "ALTER TABLE orders ADD PRIMARY KEY (id);",
			quietRule: "rewrite/replace-primary-key",
		},
		{
			name:      "a comment change touches no engine",
			dialect:   "mysql",
			statement: `ALTER TABLE orders COMMENT = 'orders';`,
			quietRule: "rewrite/change-engine",
		},
		{
			name:      "adding a column is not a partitioning change",
			dialect:   "mysql",
			statement: "ALTER TABLE orders ADD COLUMN shipped tinyint;",
			quietRule: "rewrite/change-partitioning",
		},
		{
			name:      "a virtual generated column stores nothing",
			dialect:   "mysql",
			statement: "ALTER TABLE orders ADD COLUMN doubled int GENERATED ALWAYS AS (total * 2) VIRTUAL;",
			quietRule: "rewrite/add-stored-generated-column",
		},
		{
			name:      "an ordinary column carries no auto-increment",
			dialect:   "mysql",
			statement: "ALTER TABLE orders ADD COLUMN seq int NOT NULL;",
			quietRule: "locking/add-auto-increment-column",
		},
		{
			name:      "adding a stored column is rewrite/add-stored-generated-column's subject, not rewrite/modify-generated-column's",
			dialect:   "mysql",
			statement: "ALTER TABLE orders ADD COLUMN doubled int GENERATED ALWAYS AS (total * 2) STORED;",
			quietRule: "rewrite/modify-generated-column",
		},
		{
			name:      "dropping a check validates nothing",
			dialect:   "mysql",
			statement: "ALTER TABLE orders DROP CHECK positive;",
			quietRule: "locking/add-check",
		},
		{
			name:      "a check with no enforcement clause revalidates nothing",
			dialect:   "mysql",
			statement: "ALTER TABLE orders ADD CONSTRAINT positive CHECK (total > 0);",
			quietRule: "locking/enforce-check",
		},
		{
			name:      "adding system versioning keeps the history",
			dialect:   "mariadb",
			statement: "ALTER TABLE orders ADD SYSTEM VERSIONING;",
			quietRule: "destructive/drop-system-versioning",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			codes := analyzeGap(c, test.dialect, test.statement)

			c.Assert(codes, qt.Not(qt.Contains), test.quietRule)
		})
	}
}

// TestAtlasGapRules_DialectsAreMeasuredNotAssumed pins the two rules that a
// dialect divergence, rather than a cost difference, restricts.
//
// MariaDB has no ENFORCED syntax and MySQL 8.4 has no system versioning, so a
// finding on the other engine would describe a statement that server refuses
// to parse.
func TestAtlasGapRules_DialectsAreMeasuredNotAssumed(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		statement string
		quietRule string
	}{
		{
			name:      "check enforcement is not reported on mariadb",
			dialect:   "mariadb",
			statement: "ALTER TABLE orders ALTER CONSTRAINT positive ENFORCED;",
			quietRule: "locking/enforce-check",
		},
		{
			name:      "system versioning is not reported on mysql",
			dialect:   "mysql",
			statement: "ALTER TABLE orders DROP SYSTEM VERSIONING;",
			quietRule: "destructive/drop-system-versioning",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			codes := analyzeGap(c, test.dialect, test.statement)

			c.Assert(codes, qt.Not(qt.Contains), test.quietRule)
		})
	}
}

// TestAtlasGapRules_SubsumeTheGenericFindingOnly checks that a specific
// finding replaces the generic one about the same consequence and leaves every
// separate hazard reported.
//
// This is the half a subsumption change breaks silently: removing too much
// makes the report shorter and the run still green.
func TestAtlasGapRules_SubsumeTheGenericFindingOnly(t *testing.T) {
	t.Run("rewrite/replace-primary-key replaces rewrite/add-primary-key and keeps integrity/drop-primary-key", func(t *testing.T) {
		c := qt.New(t)

		codes := analyzeGap(c, "mysql", "ALTER TABLE orders DROP PRIMARY KEY, ADD PRIMARY KEY (total);")

		c.Assert(codes, qt.Contains, "rewrite/replace-primary-key")
		c.Assert(codes, qt.Contains, "integrity/drop-primary-key")
		c.Assert(codes, qt.Not(qt.Contains), "rewrite/add-primary-key")
	})

	t.Run("locking/replace-primary-key replaces locking/add-primary-key and keeps integrity/drop-untyped-constraint", func(t *testing.T) {
		c := qt.New(t)

		codes := analyzeGap(c, "postgres", "ALTER TABLE orders DROP CONSTRAINT orders_pkey, ADD PRIMARY KEY (total);")

		c.Assert(codes, qt.Contains, "locking/replace-primary-key")
		c.Assert(codes, qt.Contains, "integrity/drop-untyped-constraint")
		c.Assert(codes, qt.Not(qt.Contains), "locking/add-primary-key")
	})

	t.Run("locking/add-auto-increment-column replaces locking/blocking-alter-table and keeps data-dependent/add-not-null-column", func(t *testing.T) {
		c := qt.New(t)

		codes := analyzeGap(c, "mysql", "ALTER TABLE orders ADD COLUMN seq int NOT NULL AUTO_INCREMENT, ADD KEY (seq);")

		c.Assert(codes, qt.Contains, "locking/add-auto-increment-column")
		c.Assert(codes, qt.Contains, "data-dependent/add-not-null-column")
		c.Assert(codes, qt.Not(qt.Contains), "locking/blocking-alter-table")
	})
}

// TestAtlasGapRules_SayWhatWasMeasured pins each message to the measurement in
// atlasgaps.go rather than to a generic rebuild claim.
//
// Cost and locking are separate sentences because they are separate facts:
// locking/add-auto-increment-column rebuilds in place and still blocks writes, rewrite/set-not-null rebuilds in place and
// lets them through, and a message that said "rebuilds the table" for both
// would be true and useless.
func TestAtlasGapRules_SayWhatWasMeasured(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		statement string
		rule      string
		contains  []string
	}{
		{
			name:      "locking/add-auto-increment-column says in place and still blocked",
			dialect:   "mysql",
			statement: "ALTER TABLE orders ADD COLUMN seq int NOT NULL AUTO_INCREMENT, ADD KEY (seq);",
			rule:      "locking/add-auto-increment-column",
			contains:  []string{"in place", "LOCK=NONE is refused"},
		},

		{
			name:      "rewrite/change-partitioning says there is no algorithm to ask for",
			dialect:   "mysql",
			statement: "ALTER TABLE orders PARTITION BY HASH(id) PARTITIONS 4;",
			rule:      "rewrite/change-partitioning",
			contains:  []string{"does not accept an ALGORITHM or LOCK clause"},
		},
		{
			name:      "destructive/drop-system-versioning says the history is deleted, not made expensive",
			dialect:   "mariadb",
			statement: "ALTER TABLE orders DROP SYSTEM VERSIONING;",
			rule:      "destructive/drop-system-versioning",
			contains:  []string{"permanently", "no rollback"},
		},
		{
			name:      "design/disable-autovacuum says the statement is cheap and the cost is later",
			dialect:   "postgres",
			statement: "ALTER TABLE orders SET (autovacuum_enabled = false);",
			rule:      "design/disable-autovacuum",
			contains:  []string{"SHARE UPDATE EXCLUSIVE", "paid later"},
		},
		{
			name:      "rollout/drop-table says the rollout break, not the row loss",
			dialect:   "postgres",
			statement: "DROP TABLE orders;",
			rule:      "rollout/drop-table",
			contains:  []string{"deployed", "empty table", "no backup mitigates"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			message := gapMessage(c, test.dialect, test.statement, test.rule)

			c.Assert(message, qt.Not(qt.Equals), "")
			for _, want := range test.contains {
				c.Assert(message, qt.Contains, want)
			}
		})
	}
}

// TestPartitionedIndexRule_NeedsTheDeclarationItReadsFrom pins locking/partitioned-index to the
// evidence it has rather than to a guess.
//
// The statement alone cannot say a table is partitioned. Where the migration
// declares the parent, the rule reports; where it does not, it is silent and
// locking/create-index still reports the ordinary index build. Reporting on every CREATE
// INDEX would be a claim the available information cannot establish.
func TestPartitionedIndexRule_NeedsTheDeclarationItReadsFrom(t *testing.T) {
	t.Run("the parent is declared partitioned in the same file", func(t *testing.T) {
		c := qt.New(t)

		analysis, err := lint.AnalyzeFS(c.Context(), builtintest.Runtime(), fixture(map[string]string{
			"1_base.sql": "CREATE TABLE unrelated (id int);",
			"2_change.sql": "CREATE TABLE events (id int, at timestamptz) PARTITION BY RANGE (at);\n" +
				"CREATE INDEX events_at ON events (at);",
		}), gapOptions("postgres"))
		c.Assert(err, qt.IsNil)

		c.Assert(messageOf(analysis.Findings(), "locking/partitioned-index"), qt.Contains, "every one of its partitions")
	})

	t.Run("an ordinary table is left to locking/create-index", func(t *testing.T) {
		c := qt.New(t)

		codes := analyzeGap(c, "postgres", "CREATE INDEX orders_total ON orders (total);")

		c.Assert(codes, qt.Not(qt.Contains), "locking/partitioned-index")
		c.Assert(codes, qt.Contains, "locking/create-index")
	})
}

// TestNullabilityRebuildRule_NeedsTheTransitionNotTheKeyword pins rewrite/set-not-null to the
// schema state rather than to the words NOT NULL.
//
// A MODIFY that restates a nullability the column already had changes only the
// type, and reporting a rebuild there would be a cost claim about a change that
// did not happen. Without a baseline the rule finds nothing and the run reports
// the unmet input, rather than firing on every NOT NULL it sees.
func TestNullabilityRebuildRule_NeedsTheTransitionNotTheKeyword(t *testing.T) {
	const alter = "ALTER TABLE orders MODIFY COLUMN total bigint NOT NULL;"

	t.Run("the baseline says the column was nullable", func(t *testing.T) {
		c := qt.New(t)

		analysis := analyzeCost(c, "mysql", alter,
			lintext.BaselineColumn{Version: 2, Table: "orders", Name: "total", ColumnType: "int", NotNull: false})

		c.Assert(messageOf(analysis.Findings(), "rewrite/set-not-null"), qt.Contains, "making total NOT NULL rebuilds the table")
	})

	t.Run("the baseline says it was already NOT NULL", func(t *testing.T) {
		c := qt.New(t)

		analysis := analyzeCost(c, "mysql", alter,
			lintext.BaselineColumn{Version: 2, Table: "orders", Name: "total", ColumnType: "int", NotNull: true})

		c.Assert(messageOf(analysis.Findings(), "rewrite/set-not-null"), qt.Equals, "")
	})

	t.Run("no baseline reports nothing rather than guessing", func(t *testing.T) {
		c := qt.New(t)

		analysis := analyzeCost(c, "mysql", alter)

		c.Assert(messageOf(analysis.Findings(), "rewrite/set-not-null"), qt.Equals, "")
	})
}

// TestRolloutBreakRules_ReportOnTheNativeSurfaceOnly pins where rollout/drop-table and rollout/drop-column
// are reported, in both directions, because a mistake either way is silent.
//
// Reporting them on the compatibility surface would add a diagnostic to a
// statement that surface already reports as destructive, and the visible effect
// is an exit code: a migration directory that passes today would start failing.
// Dropping them from the native surface would remove the whole point of the
// pair, and nothing else in this package would notice -- the destructive rules
// keep reporting the same statements, so every other assertion stays green.
//
// Validated with the mutant: deleting the compatibility test from
// atlasGapReportsOnThisSurface turns both atlas rows into the native ones, and
// making it always answer false empties the native rows.
func TestRolloutBreakRules_ReportOnTheNativeSurfaceOnly(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		wantNative []string
		wantAtlas  []string
	}{
		{
			name:       "dropped table",
			sql:        "DROP TABLE orders;",
			wantNative: []string{"destructive/drop-table", "rollout/drop-table"},
			wantAtlas:  []string{"destructive/drop-table"},
		},
		{
			name:       "dropped column",
			sql:        "ALTER TABLE orders DROP COLUMN note;",
			wantNative: []string{"destructive/drop-column", "rollout/drop-column"},
			wantAtlas:  []string{"destructive/drop-column"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			native, err := lint.AnalyzeFS(c.Context(), builtintest.Runtime(), fixture(gapFS(test.sql)), gapOptions("postgres"))
			c.Assert(err, qt.IsNil)
			c.Assert(rulesOf(native.Findings()), qt.DeepEquals, test.wantNative)

			atlasOptions := gapOptions("postgres")
			atlasOptions.Compatibility = lint.CompatibilityProfileAtlas
			atlas, err := lint.AnalyzeFS(c.Context(), builtintest.Runtime(), fixture(gapFS(test.sql)), atlasOptions)
			c.Assert(err, qt.IsNil)
			c.Assert(rulesOf(atlas.Findings()), qt.DeepEquals, test.wantAtlas)
		})
	}
}

// TestRolloutBreakRules_SurviveADestructiveExemption is the behavior the
// separation exists for: accepting the data loss in a drop says nothing about
// the deployed versions that still name the object.
//
// Three shapes of exemption, because they take different paths: a directive
// naming the destructive rule, a directive naming the analyzer family, and a
// configuration that disables the family outright. All three leave the rollout
// break reported, and the control row shows the exemption did reach the rule it
// named rather than being ignored.
func TestRolloutBreakRules_SurviveADestructiveExemption(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		disabled []string
		want     []string
	}{
		{
			name: "no exemption reports both",
			sql:  "DROP TABLE orders;",
			want: []string{"destructive/drop-table", "rollout/drop-table"},
		},
		{
			name: "a directive naming the destructive rule",
			sql:  "-- ptah:nolint destructive/drop-table\nDROP TABLE orders;",
			want: []string{"rollout/drop-table"},
		},
		{
			name:     "a configuration disabling the destructive family",
			sql:      "DROP TABLE orders;",
			disabled: []string{"DS"},
			want:     []string{"rollout/drop-table"},
		},
		{
			name:     "disabling the rollout-break family leaves the data loss",
			sql:      "DROP TABLE orders;",
			disabled: []string{"BC"},
			want:     []string{"destructive/drop-table"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			options := gapOptions("postgres")
			options.Disabled = test.disabled
			analysis, err := lint.AnalyzeFS(c.Context(), builtintest.Runtime(), fixture(gapFS(test.sql)), options)
			c.Assert(err, qt.IsNil)
			c.Assert(rulesOf(analysis.Findings()), qt.DeepEquals, test.want)
		})
	}
}

// TestAtlasGapRules_AColumnNamedAfterAKeywordIsNotAHazard is the control class
// the clause-anchored scanners exist for.
//
// None of ENGINE, AUTO_INCREMENT, ENFORCED, STORED or VERSIONING is reserved,
// so each is a legal column name, and a scan that asks whether the word appears
// anywhere in the statement reports a table rebuild for an ordinary column add.
// Every row here was reported by exactly one of these rules before the scanners
// were anchored to clause heads.
//
// The mixed row is the one a single-clause fixture cannot catch: the keyword is
// in a clause of its own that is not the table option, so a scan that stops at
// the statement rather than at the next clause still reports it.
func TestAtlasGapRules_AColumnNamedAfterAKeywordIsNotAHazard(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{name: "engine", sql: "ALTER TABLE orders ADD COLUMN engine VARCHAR(10);"},
		{name: "auto_increment", sql: "ALTER TABLE orders ADD COLUMN auto_increment INT;"},
		{name: "enforced", sql: "ALTER TABLE orders ADD COLUMN enforced BOOLEAN;"},
		{name: "stored", sql: "ALTER TABLE orders ADD COLUMN stored INT;"},
		{name: "versioning", sql: "ALTER TABLE orders ADD COLUMN versioning INT;"},
		{
			name: "a keyword-named column beside another clause",
			sql:  "ALTER TABLE orders ADD COLUMN engine VARCHAR(10), DROP COLUMN note;",
			want: []string{"destructive/drop-column", "rollout/drop-column"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(analyzeGap(c, "mysql", test.sql), qt.DeepEquals, test.want)
		})
	}
}

// TestAtlasGapRules_TheKeywordFormsStillReport is the positive control for the
// table above: anchoring the scans must not have silenced the statements the
// rules are about.
func TestAtlasGapRules_TheKeywordFormsStillReport(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{name: "engine", sql: "ALTER TABLE orders ENGINE=InnoDB;", want: []string{"rewrite/change-engine"}},
		{
			name: "auto_increment",
			sql:  "ALTER TABLE orders ADD COLUMN seq INT AUTO_INCREMENT, ADD KEY (seq);",
			want: []string{"locking/add-auto-increment-column"},
		},
		{
			name: "enforced",
			sql:  "ALTER TABLE orders ALTER CONSTRAINT chk_total ENFORCED;",
			want: []string{"locking/enforce-check"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(analyzeGap(c, "mysql", test.sql), qt.DeepEquals, test.want)
		})
	}
}

// TestCheckConstraintRules_SeparateAddingFromEnforcing is the decision table
// for the two rules that read the same clause.
//
// Enforcement is what costs: MySQL scans every row when a check starts being
// enforced and does nothing when one is recorded unenforced. So the statement
// text alone decides four different answers, and three of them were wrong
// before this table existed -- an unenforced add was reported as a full scan,
// turning enforcement off was reported as a revalidation, and a constraint
// named `enforced` was reported as both.
//
// The mixed row is the one a per-statement answer cannot get right: one clause
// adds an enforced check and the next adds an unenforced one, and the scan for
// the first still runs.
func TestCheckConstraintRules_SeparateAddingFromEnforcing(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "adding a check validates every row",
			sql:  "ALTER TABLE orders ADD CONSTRAINT positive CHECK (total > 0);",
			want: []string{"locking/add-check"},
		},
		{
			name: "adding it unenforced validates nothing",
			sql:  "ALTER TABLE orders ADD CONSTRAINT positive CHECK (total > 0) NOT ENFORCED;",
		},
		{
			name: "spelling the default out is still the add, not a transition",
			sql:  "ALTER TABLE orders ADD CONSTRAINT positive CHECK (total > 0) ENFORCED;",
			want: []string{"locking/add-check"},
		},
		{
			name: "enforcing an existing check revalidates every row",
			sql:  "ALTER TABLE orders ALTER CONSTRAINT positive ENFORCED;",
			want: []string{"locking/enforce-check"},
		},
		{
			name: "unenforcing an existing check is a metadata edit",
			sql:  "ALTER TABLE orders ALTER CHECK positive NOT ENFORCED;",
		},
		{
			name: "a constraint named after the keyword enforces nothing",
			sql:  "ALTER TABLE orders ADD CONSTRAINT enforced CHECK (total > 0);",
			want: []string{"locking/add-check"},
		},
		{
			name: "an unenforced sibling does not silence the enforced clause",
			sql:  "ALTER TABLE orders ADD CONSTRAINT a CHECK (total > 0), ADD CONSTRAINT b CHECK (id > 0) NOT ENFORCED;",
			want: []string{"locking/add-check"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(analyzeGap(c, "mysql", test.sql), qt.DeepEquals, test.want)
		})
	}
}
