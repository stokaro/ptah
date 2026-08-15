package lint_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlaslint"
	"go.5x5.cz/ptah/migration/lint"
	"go.5x5.cz/ptah/migration/migrator"
)

// TestRules_EveryCompatibilityCodeIsSuppressibleByThatCode is the coverage
// gate: every code `ptah-compat migrate lint` can print must be silenced by
// `-- atlas:nolint <that code>`.
//
// Before this change 1 of the 40 distinct printed codes passed (DS102); 37
// resolved to no target at all and 2 (DS103, MF103) resolved to one of the two
// native rules that print them. Reverting the change turns this into 39 failing
// subtests, each named for its rule code and printing "value is not true" with
// the comment "native rule PG101 prints as PG101, which resolves to []".
func TestRules_EveryCompatibilityCodeIsSuppressibleByThatCode(t *testing.T) {
	for _, rule := range lint.Rules() {
		t.Run(rule.Code, func(t *testing.T) {
			c := qt.New(t)
			printed := atlaslint.RuleForNativeCode(rule.Code).Code
			targets := atlaslint.NativeSuppressionTargets(printed)
			suppressed := slices.ContainsFunc(targets, func(target atlaslint.Target) bool {
				return target.Matches(rule.Code)
			})
			c.Assert(suppressed, qt.IsTrue, qt.Commentf(
				"native rule %s prints as %s, which resolves to %v",
				rule.Code, printed, targets,
			))
		})
	}
}

// TestAnalyzeFS_AtlasCodeSelectorReachesBothProducersOfAPrintedCode is the
// collision fixture. Native DS102 (DROP COLUMN) and native DS103 (column type
// change) both print as DS103 on the compatibility surface, so one directive
// has to reach both.
//
// Before this change the same `-- atlas:nolint DS103` on two adjacent
// statements had opposite outcomes: the DROP COLUMN was silenced and the
// ALTER COLUMN ... TYPE on the next line still reported DS103. Reverting the
// change prints []string{"DS102", "DS103"} on the "silences the type change"
// row and []string{"DS103"} on the "silences both when repeated" row.
func TestAnalyzeFS_AtlasCodeSelectorReachesBothProducersOfAPrintedCode(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "no directive reports both producers",
			sql: `ALTER TABLE accounts DROP COLUMN legacy;
ALTER TABLE accounts ALTER COLUMN note TYPE varchar(10);
`,
			want: []string{"DS102", "DS103"},
		},
		{
			name: "the printed code silences the column drop",
			sql: `-- atlas:nolint DS103
ALTER TABLE accounts DROP COLUMN legacy;
ALTER TABLE accounts ALTER COLUMN note TYPE varchar(10);
`,
			want: []string{"DS103"},
		},
		{
			name: "the printed code silences the type change",
			sql: `ALTER TABLE accounts DROP COLUMN legacy;
-- atlas:nolint DS103
ALTER TABLE accounts ALTER COLUMN note TYPE varchar(10);
`,
			want: []string{"DS102"},
		},
		{
			name: "the printed code silences both when repeated",
			sql: `-- atlas:nolint DS103
ALTER TABLE accounts DROP COLUMN legacy;
-- atlas:nolint DS103
ALTER TABLE accounts ALTER COLUMN note TYPE varchar(10);
`,
			want: []string{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis, err := lint.AnalyzeFS(fixture(map[string]string{
				"1_collision.sql": test.sql,
			}), lint.Options{
				Compatibility: lint.CompatibilityProfileAtlas,
				DirFormat:     migrator.MigrationDirFormatAtlas,
				Dialect:       "postgres",
			})
			c.Assert(err, qt.IsNil)
			c.Assert(rulesOf(analysis.Findings()), qt.DeepEquals, test.want)
		})
	}
}

// TestAnalyzeFS_AtlasMF103SelectorReachesBothProducers is the second collision:
// native DD101 (a NOT NULL column added to a populated table) and native MF103
// (a file name the migrator will not pick up) both print as MF103.
//
// MF103 is a file-form rule with no statement to sit under, so the fixture uses
// the whole-file header form, which is the only directive that reaches it.
// Reverting the change leaves MF103 in the suppressed row, printing
// []string{"MF101", "MF103"} instead of []string{"MF101"}.
func TestAnalyzeFS_AtlasMF103SelectorReachesBothProducers(t *testing.T) {
	tests := []struct {
		name   string
		header string
		want   []string
	}{
		{
			name:   "no directive reports both producers",
			header: "",
			want:   []string{"MF101", "MF103", "DD101"},
		},
		{
			name:   "the printed code silences both",
			header: "-- atlas:nolint MF103\n\n",
			want:   []string{"MF101"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis, err := lint.AnalyzeFS(fixture(map[string]string{
				"weird_name.up.sql": test.header +
					"ALTER TABLE accounts ADD COLUMN tenant_id INTEGER NOT NULL;\n",
			}), lint.Options{
				Compatibility: lint.CompatibilityProfileAtlas,
				DirFormat:     migrator.MigrationDirFormatPtah,
				Dialect:       "postgres",
			})
			c.Assert(err, qt.IsNil)
			c.Assert(rulesOf(analysis.Findings()), qt.DeepEquals, test.want)
		})
	}
}

// TestAnalyzeFS_AtlasNoLintSelectorsAgreeAcrossSurfaces closes the four cells
// of the cross-binary table in #936: each selector must have the same effect on
// `ptah migrations lint` and on `ptah-compat migrate lint`.
//
// Before this change `-- atlas:nolint PG101` silenced PG101 natively and did
// nothing under the compatibility profile, while `-- atlas:nolint
// concurrent_index` did the reverse — each selector worked on exactly one of
// the two commands, and they disagreed about which. The family-prefix row is
// the third disagreement: `atlas:nolint PG` resolved through the native
// identity and silenced the whole PG family on `ptah migrations lint` while
// the pinned community binary silences nothing for a prefix.
//
// Reverting the native half (resolve `atlas:nolint` through the native
// identity again) prints the native column as []string{"PG101"} on the
// analyzer-name row and []string{} on the family-prefix row. Reverting the
// compatibility half (a code selector resolving to nothing) prints the atlas
// column as []string{"PG101"} on the printed-code row.
func TestAnalyzeFS_AtlasNoLintSelectorsAgreeAcrossSurfaces(t *testing.T) {
	tests := []struct {
		name      string
		directive string
		want      []string
	}{
		{name: "no directive", directive: "", want: []string{"PG101"}},
		{name: "printed code", directive: "-- atlas:nolint PG101\n", want: []string{}},
		{name: "analyzer name", directive: "-- atlas:nolint concurrent_index\n", want: []string{}},
		{name: "family prefix", directive: "-- atlas:nolint PG\n", want: []string{"PG101"}},
		{name: "unknown selector", directive: "-- atlas:nolint totally_bogus\n", want: []string{"PG101"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fsys := fixture(map[string]string{
				"1_index.sql": test.directive + "CREATE INDEX idx_users_id ON users (id);\n",
			})

			native, err := lint.AnalyzeFS(fsys, lint.Options{
				DirFormat: migrator.MigrationDirFormatAtlas,
				Dialect:   "postgres",
			})
			c.Assert(err, qt.IsNil)
			c.Assert(rulesOf(native.Findings()), qt.DeepEquals, test.want)

			atlas, err := lint.AnalyzeFS(fsys, lint.Options{
				Compatibility: lint.CompatibilityProfileAtlas,
				DirFormat:     migrator.MigrationDirFormatAtlas,
				Dialect:       "postgres",
			})
			c.Assert(err, qt.IsNil)
			c.Assert(rulesOf(atlas.Findings()), qt.DeepEquals, test.want)
		})
	}
}

// TestAnalyzeFS_AtlasSelectorsMatchTheCommunityBinaryOnADroppedColumn is the
// regression guard for the selector behavior that already matched the pinned
// community binary and has to keep matching it.
//
// Every row was run against the pinned community binary (atlas community
// version v1.3.0) on `ALTER TABLE t DROP COLUMN legacy;` replayed on a sqlite
// dev database. It reports DS103 and exits 1 with no directive (negative
// control), exits 0 for `DS103` and for `destructive` (positive controls), and
// exits 1 for `DS102`, for `DS`, for `D` and for `totally_bogus_selector` —
// printing nothing about the selector in the last case.
//
// Every row here also held before the change, which is the point: this is a
// guard, not a demonstration, so reverting the change leaves it green. It was
// validated with the inverse mutant instead — give the code fallback in
// [atlaslint.NativeSuppressionTargets] Family:true and the "family prefix" and
// "single letter" rows print []string{} instead of []string{"DS102"}, which is
// exit 0 where the community binary exits 1.
func TestAnalyzeFS_AtlasSelectorsMatchTheCommunityBinaryOnADroppedColumn(t *testing.T) {
	tests := []struct {
		name      string
		directive string
		want      []string
	}{
		{name: "no directive", directive: "", want: []string{"DS102"}},
		{name: "printed code", directive: "-- atlas:nolint DS103\n", want: []string{}},
		{name: "analyzer name", directive: "-- atlas:nolint destructive\n", want: []string{}},
		{name: "the other Atlas code", directive: "-- atlas:nolint DS102\n", want: []string{"DS102"}},
		{name: "family prefix", directive: "-- atlas:nolint DS\n", want: []string{"DS102"}},
		{name: "single letter", directive: "-- atlas:nolint D\n", want: []string{"DS102"}},
		{name: "unknown selector", directive: "-- atlas:nolint totally_bogus_selector\n", want: []string{"DS102"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis, err := lint.AnalyzeFS(fixture(map[string]string{
				"1_drop.sql": test.directive + "ALTER TABLE t DROP COLUMN legacy;\n",
			}), lint.Options{
				Compatibility: lint.CompatibilityProfileAtlas,
				DirFormat:     migrator.MigrationDirFormatAtlas,
				Dialect:       "postgres",
			})
			c.Assert(err, qt.IsNil)
			c.Assert(rulesOf(analysis.Findings()), qt.DeepEquals, test.want)
		})
	}
}

// TestAnalyzeFS_BlankLineDetachesANoLintDirective pins statement-local
// adjacency, measured against the pinned community binary (atlas community
// version v1.3.0) on a two-migration sqlite fixture: with `-- atlas:nolint
// DS103`, a blank line, then `ALTER TABLE t DROP COLUMN legacy;` it reports
// DS103 at L5 and exits 1, and with the blank line removed it exits 0 with no
// diagnostics. Ptah silenced both, which is exit 0 where the community binary
// exits 1.
//
// The same rule is what keeps a whole-file `atlas:nolint` header from doubling
// as a statement-local directive for the first statement of the file; see
// TestAnalyzeFS_AtlasFileSuppressionIsCompatibilityScoped and
// TestLintPendingDestructive_DoesNotApplyAtlasFileSuppression.
//
// Reverting the change prints []string{} for the detached row.
func TestAnalyzeFS_BlankLineDetachesANoLintDirective(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "directive above its statement",
			sql: `CREATE TABLE u2 (id integer);
-- atlas:nolint DS103
ALTER TABLE t DROP COLUMN legacy;
`,
			want: []string{},
		},
		{
			name: "directive detached by a blank line",
			sql: `CREATE TABLE u2 (id integer);

-- atlas:nolint DS103

ALTER TABLE t DROP COLUMN legacy;
`,
			want: []string{"DS102"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis, err := lint.AnalyzeFS(fixture(map[string]string{
				"1_gap.sql": test.sql,
			}), lint.Options{
				Compatibility: lint.CompatibilityProfileAtlas,
				DirFormat:     migrator.MigrationDirFormatAtlas,
				Dialect:       "postgres",
			})
			c.Assert(err, qt.IsNil)
			c.Assert(rulesOf(analysis.Findings()), qt.DeepEquals, test.want)
		})
	}
}
