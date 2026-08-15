package migrator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/directiveplacement"
	"go.5x5.cz/ptah/migration/migrator"
)

// directiveClassCase is one directive of one family, with the positions at
// which the migrator honors it and the observable that says whether it did.
type directiveClassCase struct {
	name      string
	directive string
	// honored is keyed by placement name, and every placement must appear: a
	// missing key would read as "dropped" and quietly shrink the class.
	honored map[string]bool
	// observe reports whether the directive took effect in sql, through the
	// exported API only.
	observe func(c *qt.C, sql string) bool
}

func upTxMode(tb testing.TB, sql string) migrator.MigrationFileTxMode {
	c := qt.New(tb)
	c.Helper()
	parsed, err := migrator.ParseMigrationUp("1_x.sql", sql)
	c.Assert(err, qt.IsNil)
	return parsed.TxMode
}

// TestDirectivePositionIsOneRuleAcrossBothFamilies is the class, not the
// instance.
//
// Issue #996 named one acceptance line -- a transaction-mode directive placed
// after executable SQL must not be honored -- and the two families answered it
// differently: `-- atlas:txmode none` below the statement was dropped, while
// `-- +ptah no_transaction` below the statement was honored and refused the run
// under `--tx-mode all`. Fixing only the transaction-mode pair would have left
// `online_ddl_tool`, `online_ddl_fallback` and the timeout keys reading the
// file by a different rule than the mode does, which is the same defect one
// directive further along.
//
// Every directive both families understand is enumerated by:
//
//	grep -rn 'atlasFileTxModeKey\|atlasCheckpointDirective\|atlasTxtarDirective\|atlas:assert\|DirectiveNoTransaction\|checkDirective\|lock_timeout\|statement_timeout' \
//	  --include='*.go' migration/migrator internal/onlineddl | grep -v _test
//
// The rows below are the ones with an exported observable. The timeout keys,
// `-- atlas:checkpoint`, `-- atlas:txtar` and `-- atlas:assert oneof` have none
// and are covered in directive_position_internal_test.go against the same
// placement table.
func TestDirectivePositionIsOneRuleAcrossBothFamilies(t *testing.T) {
	tests := []directiveClassCase{
		{
			name:      "ptah no_transaction",
			directive: "-- +ptah no_transaction",
			honored:   directiveplacement.BeforeTheStatement(),
			observe: func(c *qt.C, sql string) bool {
				return upTxMode(c.TB, sql) == migrator.MigrationFileTxModeNone
			},
		},
		{
			name:      "ptah online_ddl_tool",
			directive: "-- +ptah online_ddl_tool=ghost",
			honored:   directiveplacement.BeforeTheStatement(),
			observe: func(_ *qt.C, sql string) bool {
				return migrator.ParseFileDirectives(sql)["online_ddl_tool"] == "ghost"
			},
		},
		{
			name:      "ptah online_ddl_fallback",
			directive: "-- +ptah online_ddl_fallback=plain",
			honored:   directiveplacement.BeforeTheStatement(),
			observe: func(_ *qt.C, sql string) bool {
				return migrator.ParseFileDirectives(sql)["online_ddl_fallback"] == "plain"
			},
		},
		{
			name:      "atlas txmode none",
			directive: "-- atlas:txmode none",
			honored:   directiveplacement.InsideAtlasHeaderBlock(),
			observe: func(c *qt.C, sql string) bool {
				return upTxMode(c.TB, sql) == migrator.MigrationFileTxModeNone
			},
		},
		{
			// A check is position-insensitive BY DESIGN, and documented that
			// way: multiple `-- +ptah check` lines run in file order, all of
			// them before the first body statement. Its position therefore
			// never decided which statements ran, so narrowing it would remove
			// a documented capability to no safety gain. The row is here so the
			// exemption is measured rather than assumed.
			name:      "ptah check",
			directive: `-- +ptah check name="t_absent" assert="SELECT 1"`,
			honored:   directiveplacement.EveryLineComment(),
			observe: func(c *qt.C, sql string) bool {
				checks, err := migrator.ParseChecks(sql, "")
				c.Assert(err, qt.IsNil)
				return len(checks) == 1
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.honored, qt.HasLen, len(directiveplacement.All), qt.Commentf(
				"every placement needs an answer; a missing key reads as dropped"))

			for _, placement := range directiveplacement.All {
				c.Run(placement.Name, func(c *qt.C) {
					sql := placement.Render(test.directive)

					c.Check(test.observe(c, sql), qt.Equals, test.honored[placement.Name],
						qt.Commentf("source:\n%s", sql))
				})
			}
		})
	}
}

// TestTrailingCommentCarriesNoDirectiveInEitherFamily pins the one position
// both families always agreed on, so the shared rule is not mistaken for "a
// directive anywhere before the end of the last statement".
func TestTrailingCommentCarriesNoDirectiveInEitherFamily(t *testing.T) {
	c := qt.New(t)

	ptah := "CREATE TABLE t (id INTEGER PRIMARY KEY); -- +ptah no_transaction\n"
	atlas := "CREATE TABLE t (id INTEGER PRIMARY KEY); -- atlas:txmode none\n"

	c.Check(upTxMode(c.TB, ptah), qt.Equals, migrator.MigrationFileTxModeUnspecified)
	c.Check(upTxMode(c.TB, atlas), qt.Equals, migrator.MigrationFileTxModeUnspecified)
	c.Check(migrator.ParseFileDirectives(ptah), qt.HasLen, 0)
}
