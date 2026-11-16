package shadow_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/selectext"
	"ptah.run/engine/builtin"
	"ptah.run/migration/shadow"
)

// TestVerifyMigration_Include_HappyPath selects only `widgets` over the
// exclusion fixture, where the shadow database holds `legacy_log` the
// declaration lacks and the declaration holds `reporting_cache` the shadow
// database lacks; TestVerifyMigration_Exclude_FailurePath shows both reported
// without a selection.
func TestVerifyMigration_Include_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		include []string
		exclude []string
	}{
		{name: "the managed table", include: []string{"widgets"}},
		{name: "with an exclusion", include: []string{"widgets", "legacy_log"}, exclude: []string{"legacy_log"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			opts := excludeVerifyOptions(c, must.Must(builtin.New()), test.exclude)
			opts.Include = test.include

			err := shadow.VerifyMigration(t.Context(), opts)

			c.Assert(err, qt.IsNil)
		})
	}
}

// TestVerifyMigration_Include_FailurePath selects one side's table with
// `widgets`, so the mismatch on that side is reported and the other side's
// is not, and refuses a selection that selects nothing at the schema-match
// stage.
func TestVerifyMigration_Include_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		include []string
		want    []shadow.Mismatch
	}{
		{
			name:    "the declared table too",
			include: []string{"widgets", "reporting_cache"},
			want: []shadow.Mismatch{
				{Kind: "missing_table", Object: "reporting_cache", Table: "reporting_cache", Message: "missing table reporting_cache"},
			},
		},
		{
			name:    "the replayed table too",
			include: []string{"widgets", "legacy_log"},
			want: []shadow.Mismatch{
				{Kind: "extra_table", Object: "legacy_log", Table: "legacy_log", Message: "extra table legacy_log"},
				extraLegacyKey,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			opts := excludeVerifyOptions(c, must.Must(builtin.New()), nil)
			opts.Include = test.include

			err := shadow.VerifyMigration(t.Context(), opts)

			var verificationErr *shadow.VerificationError
			c.Assert(err, qt.ErrorAs, &verificationErr)
			c.Assert(verificationErr.Result.Stage, qt.Equals, "schema-match")
			c.Assert(verificationErr.Result.Mismatches, qt.DeepEquals, test.want)
		})
	}
}

// TestVerifyMigration_IncludeSelectingNothingAfterTheReplay_HappyPath selects
// a table neither the shadow database nor the declaration holds after the
// replay, as a plan that dropped every object it selects leaves them: the two
// empty sides agree. The plan refuses a selection of nothing before it writes
// a candidate.
func TestVerifyMigration_IncludeSelectingNothingAfterTheReplay_HappyPath(t *testing.T) {
	c := qt.New(t)
	opts := excludeVerifyOptions(c, must.Must(builtin.New()), nil)
	opts.Include = []string{"nothing_here"}

	err := shadow.VerifyMigration(t.Context(), opts)

	c.Assert(err, qt.IsNil)
}

// TestVerifyMigration_IncludeNeedsScopeSelections_FailurePath refuses at the
// configuration stage, before the shadow database is connected.
func TestVerifyMigration_IncludeNeedsScopeSelections_FailurePath(t *testing.T) {
	c := qt.New(t)
	opts := excludeVerifyOptions(c, runtimeWithoutSelections{must.Must(builtin.New())}, nil)
	opts.Include = []string{"widgets"}

	err := shadow.VerifyMigration(t.Context(), opts)

	c.Assert(err, qt.ErrorIs, selectext.ErrUnselected)
	var verificationErr *shadow.VerificationError
	c.Assert(err, qt.ErrorAs, &verificationErr)
	c.Assert(verificationErr.Result.Stage, qt.Equals, "configuration")
	c.Assert(verificationErr.Result.Mismatches, qt.HasLen, 1)
	c.Assert(verificationErr.Result.Mismatches[0].Kind, qt.Equals, "include_error")
	_, statErr := os.Stat(filepath.FromSlash(strings.TrimPrefix(opts.ShadowDatabaseURL, "sqlite://")))
	c.Assert(statErr, qt.ErrorIs, os.ErrNotExist)
}
