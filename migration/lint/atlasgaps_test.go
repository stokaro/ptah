package lint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

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
	analysis, err := lint.AnalyzeFS(fixture(gapFS(statement)), gapOptions(dialect))
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
	analysis, err := lint.AnalyzeFS(fixture(gapFS(statement)), gapOptions(dialect))
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
			want:      "BC103",
		},
		{
			name:      "a dropped column retires a name clients select",
			dialect:   "postgres",
			statement: "ALTER TABLE orders DROP COLUMN note;",
			want:      "BC104",
		},
		{
			name:      "an exclusion constraint takes an exclusive lock",
			dialect:   "postgres",
			statement: "ALTER TABLE orders ADD CONSTRAINT no_overlap EXCLUDE USING gist (total WITH =);",
			want:      "PG109",
		},
		{
			name:      "a redefined primary key builds its index under the lock",
			dialect:   "postgres",
			statement: `ALTER TABLE orders DROP CONSTRAINT orders_pkey, ADD PRIMARY KEY (total);`,
			want:      "PG312",
		},
		{
			name:      "replica identity full changes what replication carries",
			dialect:   "postgres",
			statement: "ALTER TABLE orders REPLICA IDENTITY FULL;",
			want:      "PG314",
		},
		{
			name:      "autovacuum disabled leaves dead rows",
			dialect:   "postgres",
			statement: "ALTER TABLE orders SET (autovacuum_enabled = false);",
			want:      "PG320",
		},
		{
			name:      "a redefined primary key rebuilds the secondary indexes",
			dialect:   "mysql",
			statement: "ALTER TABLE orders DROP PRIMARY KEY, ADD PRIMARY KEY (total);",
			want:      "MY137",
		},
		{
			name:      "a storage engine change copies the table",
			dialect:   "mysql",
			statement: "ALTER TABLE orders ENGINE=InnoDB;",
			want:      "MY138",
		},
		{
			name:      "partitioning rewrites every row",
			dialect:   "mysql",
			statement: "ALTER TABLE orders PARTITION BY HASH(id) PARTITIONS 4;",
			want:      "MY139",
		},
		{
			name:      "removing partitioning rewrites every row",
			dialect:   "mysql",
			statement: "ALTER TABLE orders REMOVE PARTITIONING;",
			want:      "MY139",
		},
		{
			name:      "a stored generated column is computed for every row",
			dialect:   "mysql",
			statement: "ALTER TABLE orders ADD COLUMN doubled int GENERATED ALWAYS AS (total * 2) STORED;",
			want:      "MY140",
		},
		{
			name:      "an auto-increment column numbers every row",
			dialect:   "mysql",
			statement: "ALTER TABLE orders ADD COLUMN seq int NOT NULL AUTO_INCREMENT, ADD KEY (seq);",
			want:      "MY141",
		},
		{
			name:      "a modified generated column is recomputed for every row",
			dialect:   "mysql",
			statement: "ALTER TABLE orders MODIFY COLUMN doubled bigint GENERATED ALWAYS AS (total * 3) STORED;",
			want:      "MY143",
		},
		{
			name:      "a check constraint validates every row",
			dialect:   "mysql",
			statement: "ALTER TABLE orders ADD CONSTRAINT positive CHECK (total > 0);",
			want:      "MY144",
		},
		{
			name:      "enforcing a check revalidates every row",
			dialect:   "mysql",
			statement: "ALTER TABLE orders ALTER CONSTRAINT positive ENFORCED;",
			want:      "MY145",
		},
		{
			name:      "dropping system versioning deletes the history",
			dialect:   "mariadb",
			statement: "ALTER TABLE orders DROP SYSTEM VERSIONING;",
			want:      "MY146",
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
			quietRule: "BC103",
		},
		{
			name:      "adding a column retires no name",
			dialect:   "postgres",
			statement: "ALTER TABLE orders ADD COLUMN shipped boolean;",
			quietRule: "BC104",
		},
		{
			name:      "a plain unique constraint is not an exclusion constraint",
			dialect:   "postgres",
			statement: "ALTER TABLE orders ADD CONSTRAINT total_unique UNIQUE (total);",
			quietRule: "PG109",
		},
		{
			name:      "a primary key attached to an existing index builds nothing",
			dialect:   "postgres",
			statement: `ALTER TABLE orders DROP CONSTRAINT orders_pkey, ADD CONSTRAINT orders_pkey PRIMARY KEY USING INDEX orders_new;`,
			quietRule: "PG312",
		},
		{
			name:      "replica identity default keeps a usable row identity",
			dialect:   "postgres",
			statement: "ALTER TABLE orders REPLICA IDENTITY DEFAULT;",
			quietRule: "PG314",
		},
		{
			name:      "turning autovacuum back on is not the hazard",
			dialect:   "postgres",
			statement: "ALTER TABLE orders SET (autovacuum_enabled = true);",
			quietRule: "PG320",
		},
		{
			name:      "adding a primary key is MY132's subject, not MY137's",
			dialect:   "mysql",
			statement: "ALTER TABLE orders ADD PRIMARY KEY (id);",
			quietRule: "MY137",
		},
		{
			name:      "a comment change touches no engine",
			dialect:   "mysql",
			statement: `ALTER TABLE orders COMMENT = 'orders';`,
			quietRule: "MY138",
		},
		{
			name:      "adding a column is not a partitioning change",
			dialect:   "mysql",
			statement: "ALTER TABLE orders ADD COLUMN shipped tinyint;",
			quietRule: "MY139",
		},
		{
			name:      "a virtual generated column stores nothing",
			dialect:   "mysql",
			statement: "ALTER TABLE orders ADD COLUMN doubled int GENERATED ALWAYS AS (total * 2) VIRTUAL;",
			quietRule: "MY140",
		},
		{
			name:      "an ordinary column carries no auto-increment",
			dialect:   "mysql",
			statement: "ALTER TABLE orders ADD COLUMN seq int NOT NULL;",
			quietRule: "MY141",
		},
		{
			name:      "adding a stored column is MY140's subject, not MY143's",
			dialect:   "mysql",
			statement: "ALTER TABLE orders ADD COLUMN doubled int GENERATED ALWAYS AS (total * 2) STORED;",
			quietRule: "MY143",
		},
		{
			name:      "dropping a check validates nothing",
			dialect:   "mysql",
			statement: "ALTER TABLE orders DROP CHECK positive;",
			quietRule: "MY144",
		},
		{
			name:      "a check with no enforcement clause revalidates nothing",
			dialect:   "mysql",
			statement: "ALTER TABLE orders ADD CONSTRAINT positive CHECK (total > 0);",
			quietRule: "MY145",
		},
		{
			name:      "adding system versioning keeps the history",
			dialect:   "mariadb",
			statement: "ALTER TABLE orders ADD SYSTEM VERSIONING;",
			quietRule: "MY146",
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
			quietRule: "MY145",
		},
		{
			name:      "system versioning is not reported on mysql",
			dialect:   "mysql",
			statement: "ALTER TABLE orders DROP SYSTEM VERSIONING;",
			quietRule: "MY146",
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
	t.Run("MY137 replaces MY132 and keeps CD103", func(t *testing.T) {
		c := qt.New(t)

		codes := analyzeGap(c, "mysql", "ALTER TABLE orders DROP PRIMARY KEY, ADD PRIMARY KEY (total);")

		c.Assert(codes, qt.Contains, "MY137")
		c.Assert(codes, qt.Contains, "CD103")
		c.Assert(codes, qt.Not(qt.Contains), "MY132")
	})

	t.Run("PG312 replaces PG104 and keeps DS105", func(t *testing.T) {
		c := qt.New(t)

		codes := analyzeGap(c, "postgres", "ALTER TABLE orders DROP CONSTRAINT orders_pkey, ADD PRIMARY KEY (total);")

		c.Assert(codes, qt.Contains, "PG312")
		c.Assert(codes, qt.Contains, "DS105")
		c.Assert(codes, qt.Not(qt.Contains), "PG104")
	})

	t.Run("MY141 replaces MY101 and keeps DD101", func(t *testing.T) {
		c := qt.New(t)

		codes := analyzeGap(c, "mysql", "ALTER TABLE orders ADD COLUMN seq int NOT NULL AUTO_INCREMENT, ADD KEY (seq);")

		c.Assert(codes, qt.Contains, "MY141")
		c.Assert(codes, qt.Contains, "DD101")
		c.Assert(codes, qt.Not(qt.Contains), "MY101")
	})
}

// TestAtlasGapRules_SayWhatWasMeasured pins each message to the measurement in
// atlasgaps.go rather than to a generic rebuild claim.
//
// Cost and locking are separate sentences because they are separate facts:
// MY141 rebuilds in place and still blocks writes, MY147 rebuilds in place and
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
			name:      "MY141 says in place and still blocked",
			dialect:   "mysql",
			statement: "ALTER TABLE orders ADD COLUMN seq int NOT NULL AUTO_INCREMENT, ADD KEY (seq);",
			rule:      "MY141",
			contains:  []string{"in place", "LOCK=NONE is refused"},
		},

		{
			name:      "MY139 says there is no algorithm to ask for",
			dialect:   "mysql",
			statement: "ALTER TABLE orders PARTITION BY HASH(id) PARTITIONS 4;",
			rule:      "MY139",
			contains:  []string{"does not accept an ALGORITHM or LOCK clause"},
		},
		{
			name:      "MY146 says the history is deleted, not made expensive",
			dialect:   "mariadb",
			statement: "ALTER TABLE orders DROP SYSTEM VERSIONING;",
			rule:      "MY146",
			contains:  []string{"permanently", "no rollback"},
		},
		{
			name:      "PG320 says the statement is cheap and the cost is later",
			dialect:   "postgres",
			statement: "ALTER TABLE orders SET (autovacuum_enabled = false);",
			rule:      "PG320",
			contains:  []string{"SHARE UPDATE EXCLUSIVE", "paid later"},
		},
		{
			name:      "BC103 says the rollout break, not the row loss",
			dialect:   "postgres",
			statement: "DROP TABLE orders;",
			rule:      "BC103",
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

// TestPartitionedIndexRule_NeedsTheDeclarationItReadsFrom pins PG108 to the
// evidence it has rather than to a guess.
//
// The statement alone cannot say a table is partitioned. Where the migration
// declares the parent, the rule reports; where it does not, it is silent and
// PG101 still reports the ordinary index build. Reporting on every CREATE
// INDEX would be a claim the available information cannot establish.
func TestPartitionedIndexRule_NeedsTheDeclarationItReadsFrom(t *testing.T) {
	t.Run("the parent is declared partitioned in the same file", func(t *testing.T) {
		c := qt.New(t)

		analysis, err := lint.AnalyzeFS(fixture(map[string]string{
			"1_base.sql": "CREATE TABLE unrelated (id int);",
			"2_change.sql": "CREATE TABLE events (id int, at timestamptz) PARTITION BY RANGE (at);\n" +
				"CREATE INDEX events_at ON events (at);",
		}), gapOptions("postgres"))
		c.Assert(err, qt.IsNil)

		c.Assert(messageOf(analysis.Findings(), "PG108"), qt.Contains, "every one of its partitions")
	})

	t.Run("an ordinary table is left to PG101", func(t *testing.T) {
		c := qt.New(t)

		codes := analyzeGap(c, "postgres", "CREATE INDEX orders_total ON orders (total);")

		c.Assert(codes, qt.Not(qt.Contains), "PG108")
		c.Assert(codes, qt.Contains, "PG101")
	})
}

// TestNullabilityRebuildRule_NeedsTheTransitionNotTheKeyword pins MY147 to the
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
			lint.BaselineColumn{Version: 2, Table: "orders", Name: "total", ColumnType: "int", NotNull: false})

		c.Assert(messageOf(analysis.Findings(), "MY147"), qt.Contains, "making total NOT NULL rebuilds the table")
	})

	t.Run("the baseline says it was already NOT NULL", func(t *testing.T) {
		c := qt.New(t)

		analysis := analyzeCost(c, "mysql", alter,
			lint.BaselineColumn{Version: 2, Table: "orders", Name: "total", ColumnType: "int", NotNull: true})

		c.Assert(messageOf(analysis.Findings(), "MY147"), qt.Equals, "")
	})

	t.Run("no baseline reports nothing rather than guessing", func(t *testing.T) {
		c := qt.New(t)

		analysis := analyzeCost(c, "mysql", alter)

		c.Assert(messageOf(analysis.Findings(), "MY147"), qt.Equals, "")
	})
}

// TestRolloutBreakRules_ReportOnTheNativeSurfaceOnly pins where BC103 and BC104
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
			wantNative: []string{"BC103", "DS101"},
			wantAtlas:  []string{"DS101"},
		},
		{
			name:       "dropped column",
			sql:        "ALTER TABLE orders DROP COLUMN note;",
			wantNative: []string{"BC104", "DS102"},
			wantAtlas:  []string{"DS102"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			native, err := lint.AnalyzeFS(fixture(gapFS(test.sql)), gapOptions("postgres"))
			c.Assert(err, qt.IsNil)
			c.Assert(rulesOf(native.Findings()), qt.DeepEquals, test.wantNative)

			atlasOptions := gapOptions("postgres")
			atlasOptions.Compatibility = lint.CompatibilityProfileAtlas
			atlas, err := lint.AnalyzeFS(fixture(gapFS(test.sql)), atlasOptions)
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
			want: []string{"BC103", "DS101"},
		},
		{
			name: "a directive naming the destructive rule",
			sql:  "-- ptah:nolint DS101\nDROP TABLE orders;",
			want: []string{"BC103"},
		},
		{
			name:     "a configuration disabling the destructive family",
			sql:      "DROP TABLE orders;",
			disabled: []string{"DS"},
			want:     []string{"BC103"},
		},
		{
			name:     "disabling the rollout-break family leaves the data loss",
			sql:      "DROP TABLE orders;",
			disabled: []string{"BC"},
			want:     []string{"DS101"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			options := gapOptions("postgres")
			options.Disabled = test.disabled
			analysis, err := lint.AnalyzeFS(fixture(gapFS(test.sql)), options)
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
			want: []string{"BC104", "DS102"},
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
		{name: "engine", sql: "ALTER TABLE orders ENGINE=InnoDB;", want: []string{"MY138"}},
		{
			name: "auto_increment",
			sql:  "ALTER TABLE orders ADD COLUMN seq INT AUTO_INCREMENT, ADD KEY (seq);",
			want: []string{"MY141"},
		},
		{
			name: "enforced",
			sql:  "ALTER TABLE orders ALTER CONSTRAINT chk_total ENFORCED;",
			want: []string{"MY145"},
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
			want: []string{"MY144"},
		},
		{
			name: "adding it unenforced validates nothing",
			sql:  "ALTER TABLE orders ADD CONSTRAINT positive CHECK (total > 0) NOT ENFORCED;",
		},
		{
			name: "spelling the default out is still the add, not a transition",
			sql:  "ALTER TABLE orders ADD CONSTRAINT positive CHECK (total > 0) ENFORCED;",
			want: []string{"MY144"},
		},
		{
			name: "enforcing an existing check revalidates every row",
			sql:  "ALTER TABLE orders ALTER CONSTRAINT positive ENFORCED;",
			want: []string{"MY145"},
		},
		{
			name: "unenforcing an existing check is a metadata edit",
			sql:  "ALTER TABLE orders ALTER CHECK positive NOT ENFORCED;",
		},
		{
			name: "a constraint named after the keyword enforces nothing",
			sql:  "ALTER TABLE orders ADD CONSTRAINT enforced CHECK (total > 0);",
			want: []string{"MY144"},
		},
		{
			name: "an unenforced sibling does not silence the enforced clause",
			sql:  "ALTER TABLE orders ADD CONSTRAINT a CHECK (total > 0), ADD CONSTRAINT b CHECK (id > 0) NOT ENFORCED;",
			want: []string{"MY144"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(analyzeGap(c, "mysql", test.sql), qt.DeepEquals, test.want)
		})
	}
}
