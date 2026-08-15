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
	//
	// It returns an error rather than taking the checker, so the row carries a
	// predicate and the reporting stays with the test that runs it. Each
	// directive is seen through a different exported call, which is why this
	// is a function at all -- but a function over the SQL, not over the
	// checker. See AGENTS.md, "A Table Row Carries Data, Not A Checker".
	observe func(sql string) (bool, error)
}

// upTxMode is the same reading for a test that wants the mode itself rather
// than a predicate, and reports a parse failure through the checker because it
// is called from a test body rather than held in a row.
func upTxMode(c *qt.C, sql string) migrator.MigrationFileTxMode {
	c.Helper()
	parsed, err := migrator.ParseMigrationUp("1_x.sql", sql)
	c.Assert(err, qt.IsNil)
	return parsed.TxMode
}

func upTxModeIsNone(sql string) (bool, error) {
	parsed, err := migrator.ParseMigrationUp("1_x.sql", sql)
	if err != nil {
		return false, err
	}
	return parsed.TxMode == migrator.MigrationFileTxModeNone, nil
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
			observe:   upTxModeIsNone,
		},
		{
			name:      "ptah online_ddl_tool",
			directive: "-- +ptah online_ddl_tool=ghost",
			honored:   directiveplacement.BeforeTheStatement(),
			observe: func(sql string) (bool, error) {
				return migrator.ParseFileDirectives(sql)["online_ddl_tool"] == "ghost", nil
			},
		},
		{
			name:      "ptah online_ddl_fallback",
			directive: "-- +ptah online_ddl_fallback=plain",
			honored:   directiveplacement.BeforeTheStatement(),
			observe: func(sql string) (bool, error) {
				return migrator.ParseFileDirectives(sql)["online_ddl_fallback"] == "plain", nil
			},
		},
		{
			name:      "atlas txmode none",
			directive: "-- atlas:txmode none",
			honored:   directiveplacement.InsideAtlasHeaderBlock(),
			observe:   upTxModeIsNone,
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
			observe: func(sql string) (bool, error) {
				checks, err := migrator.ParseChecks(sql, "")
				return len(checks) == 1, err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.honored, qt.HasLen, len(directiveplacement.All), qt.Commentf(
				"every placement needs an answer; a missing key reads as dropped"))

			for _, placement := range directiveplacement.All {
				t.Run(placement.Name, func(t *testing.T) {
					c := qt.New(t)
					sql := placement.Render(test.directive)

					honored, err := test.observe(sql)

					c.Assert(err, qt.IsNil, qt.Commentf("source:\n%s", sql))
					c.Check(honored, qt.Equals, test.honored[placement.Name],
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

	c.Check(upTxMode(c, ptah), qt.Equals, migrator.MigrationFileTxModeUnspecified)
	c.Check(upTxMode(c, atlas), qt.Equals, migrator.MigrationFileTxModeUnspecified)
	c.Check(migrator.ParseFileDirectives(ptah), qt.HasLen, 0)
}
