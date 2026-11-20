package lint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/builtintest"
	"ptah.run/migration/lint"
	"ptah.run/migration/migrationfile"
)

// atlasStub is the Atlas selector table of these tests. The real one lives in
// internal/atlascodes, which migration lint must not import: lint holds no
// table from Atlas codes to rules, and the compatibility surface hands it one.
func atlasStub(selector string) []string {
	switch selector {
	case "DS102":
		return []string{"destructive/drop-table"}
	case "DS103":
		return []string{"destructive/drop-column"}
	case "MF103":
		return []string{"data-dependent/add-not-null-column"}
	case "DESTRUCTIVE":
		return []string{"destructive/drop-column", "destructive/drop-table"}
	case "DATA_DEPEND":
		return []string{"data-dependent/add-not-null-column"}
	case "CONCURRENT_INDEX":
		return []string{"locking/create-index", "transaction/concurrent-index"}
	case "NESTEDTX":
		return []string{"transaction/explicit-transaction"}
	default:
		return nil
	}
}

// TestAnalyzeFS_AtlasProfileReadsAtlasNoLintThroughTheResolver pins what the
// compatibility profile does with the selector of an `atlas:nolint` comment:
// it asks the resolver the compatibility surface supplied, matches the answer
// exactly, and silences nothing for a selector the resolver does not know.
// Atlas's own behavior is the contract here: on `ALTER TABLE t DROP COLUMN
// legacy;` the pinned community binary exits 1 for `DS`, `D` and an unknown
// selector, printing nothing about it.
func TestAnalyzeFS_AtlasProfileReadsAtlasNoLintThroughTheResolver(t *testing.T) {
	tests := []struct {
		name      string
		directive string
		resolver  lint.AtlasSelectors
		want      []string
	}{
		{name: "no directive", directive: "", resolver: atlasStub, want: []string{"destructive/drop-column"}},
		{name: "a code the resolver knows", directive: "-- atlas:nolint DS103\n", resolver: atlasStub, want: make([]string, 0)},
		{name: "a code in lower case", directive: "-- atlas:nolint ds103\n", resolver: atlasStub, want: make([]string, 0)},
		{name: "an analyzer name", directive: "-- atlas:nolint destructive\n", resolver: atlasStub, want: make([]string, 0)},
		{name: "a bare directive", directive: "-- atlas:nolint\n", resolver: atlasStub, want: make([]string, 0)},
		{name: "a code the resolver does not know", directive: "-- atlas:nolint DS102\n", resolver: atlasStub, want: []string{"destructive/drop-column"}},
		{name: "a family prefix", directive: "-- atlas:nolint DS\n", resolver: atlasStub, want: []string{"destructive/drop-column"}},
		{name: "a single letter", directive: "-- atlas:nolint D\n", resolver: atlasStub, want: []string{"destructive/drop-column"}},
		{name: "an unknown selector", directive: "-- atlas:nolint totally_bogus_selector\n", resolver: atlasStub, want: []string{"destructive/drop-column"}},
		{name: "no resolver", directive: "-- atlas:nolint DS103\n", resolver: nil, want: []string{"destructive/drop-column"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis, err := lint.AnalyzeFS(c.Context(), builtintest.Runtime(), fixture(map[string]string{
				"1_drop.sql": test.directive + "ALTER TABLE t DROP COLUMN legacy;\n",
			}), lint.Options{
				Compatibility:  lint.CompatibilityProfileAtlas,
				AtlasSelectors: test.resolver,
				DirFormat:      migrationfile.DirFormatAtlas,
				Dialect:        "postgres",
			})
			c.Assert(err, qt.IsNil)
			c.Assert(rulesOf(analysis.Findings()), qt.DeepEquals, test.want)
		})
	}
}

// TestAnalyzeFS_BlankLineDetachesANoLintDirective pins statement-local
// adjacency, measured against the pinned community binary on a two-migration
// sqlite fixture: with an `atlas:nolint` directive, a blank line, then `ALTER
// TABLE t DROP COLUMN legacy;` it reports the drop at L5 and exits 1, and with
// the blank line removed it exits 0 with no diagnostics. Ptah silenced both,
// which is exit 0 where the community binary exits 1.
//
// The same rule is what keeps a whole-file `atlas:nolint` header from doubling
// as a statement-local directive for the first statement of the file; see
// TestAnalyzeFS_AtlasFileSuppressionIsCompatibilityScoped and
// TestLintPendingDestructive_DoesNotApplyAtlasFileSuppression.
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
			want: make([]string, 0),
		},
		{
			name: "directive detached by a blank line",
			sql: `CREATE TABLE u2 (id integer);

-- atlas:nolint DS103

ALTER TABLE t DROP COLUMN legacy;
`,
			want: []string{"destructive/drop-column"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis, err := lint.AnalyzeFS(c.Context(), builtintest.Runtime(), fixture(map[string]string{
				"1_gap.sql": test.sql,
			}), lint.Options{
				Compatibility:  lint.CompatibilityProfileAtlas,
				AtlasSelectors: atlasStub,
				DirFormat:      migrationfile.DirFormatAtlas,
				Dialect:        "postgres",
			})
			c.Assert(err, qt.IsNil)
			c.Assert(rulesOf(analysis.Findings()), qt.DeepEquals, test.want)
		})
	}
}

// TestAnalyzeFS_NativeRefusesAtlasNoLint pins the native refusal: an
// `atlas:nolint` comment is Atlas's directive, read by the compatibility
// profile only, and native analysis refuses it at its line without naming a
// rule the Atlas code might stand for.
func TestAnalyzeFS_NativeRefusesAtlasNoLint(t *testing.T) {
	tests := []struct {
		name      string
		directive string
	}{
		{name: "a code", directive: "-- atlas:nolint DS103\n"},
		{name: "an analyzer name", directive: "-- atlas:nolint destructive\n"},
		{name: "a bare directive", directive: "-- atlas:nolint\n"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, err := lint.AnalyzeFS(c.Context(), builtintest.Runtime(), fixture(map[string]string{
				"0000000001_drop.up.sql": test.directive + "ALTER TABLE t DROP COLUMN legacy;\n",
			}), lint.Options{DirFormat: migrationfile.DirFormatPtah, Dialect: "postgres"})
			c.Assert(err, qt.ErrorMatches,
				`0000000001_drop.up.sql: the comment above line 2: atlas:nolint is an Atlas directive that ptah-compat reads; `+
					`native Ptah reads ptah:nolint with a rule name, such as ptah:nolint locking/add-foreign-key`)
		})
	}
}
