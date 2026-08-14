package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// TestCompareWithDatabaseInfoRefusesAForeignDefinerReplacement is the
// regression for the unanswered review on stokaro/ptah#1461. MySQL and
// MariaDB do not support CREATE OR REPLACE FUNCTION, so a modification is a
// DROP followed by CREATE. Recreating another account's SQL SECURITY DEFINER
// routine as the connected account silently changes the principal under which
// its body executes.
func TestCompareWithDatabaseInfoRefusesAForeignDefinerReplacement(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "mysql", dialect: "mysql"},
		{name: "mariadb", dialect: "mariadb"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDatabaseInfo(
				mysqlDefinerDesired("RETURN 2", "DEFINER"),
				mysqlDefinerCurrent("RETURN 1", "DEFINER", "owner_a@%", "migrator_a@%"),
				mysqlDefinerInfo(test.dialect),
				nil,
			)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches,
				`.*cannot safely replace.*function "f".*definer "owner_a@%".*connected account "migrator_a@%".*execution principal.*`)
			c.Assert(diff, qt.IsNil)
		})
	}
}

// TestCompareWithDatabaseInfoRefusesAReplacementWithoutOwnershipFacts keeps
// the safety rule fail closed for programmatic callers that build a live-like
// DBFunction without the reader-only ownership fields. Treating missing facts
// as same-owner would recreate the exact silent principal change this guard
// exists to prevent.
func TestCompareWithDatabaseInfoRefusesAReplacementWithoutOwnershipFacts(t *testing.T) {
	c := qt.New(t)

	diff, err := schemadiff.CompareWithDatabaseInfo(
		mysqlDefinerDesired("RETURN 2", "DEFINER"),
		mysqlDefinerCurrent("RETURN 1", "DEFINER", "", ""),
		mysqlDefinerInfo("mysql"),
		nil,
	)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `.*cannot safely replace.*ownership facts are incomplete.*`)
	c.Assert(diff, qt.IsNil)
}

// TestCompareWithDatabaseInfoAllowsAForeignDefinerLanguageThisTargetSkips
// keeps the ownership gate aligned with the planner's executable boundary. A
// MySQL-family target leaves a non-SQL function unchanged and emits only a
// named skip comment, so no drop/create pair exists that could adopt the
// connected account as a new definer.
func TestCompareWithDatabaseInfoAllowsAForeignDefinerLanguageThisTargetSkips(t *testing.T) {
	c := qt.New(t)
	desired := mysqlDefinerDesired("RETURN 2", "DEFINER")
	// An omitted annotation language canonicalizes to plpgsql. Checking after
	// canonicalization is what keeps this on the planner's skip path.
	desired.Functions[0].Language = ""

	diff, err := schemadiff.CompareWithDatabaseInfo(
		desired,
		mysqlDefinerCurrent("RETURN 1", "DEFINER", "owner_a@%", "migrator_a@%"),
		mysqlDefinerInfo("mysql"),
		nil,
	)

	c.Assert(err, qt.IsNil)
	c.Assert(diff.FunctionsModified, qt.HasLen, 1)
}

// TestCompareWithDatabaseInfoAllowsSafeFunctionChanges pins the adjacent
// cases. The guard is about an implicit principal change, not a blanket ban on
// MySQL-family function drift.
func TestCompareWithDatabaseInfoAllowsSafeFunctionChanges(t *testing.T) {
	tests := []struct {
		name            string
		desiredBody     string
		desiredSecurity string
		currentBody     string
		currentSecurity string
		definer         string
		currentAccount  string
		wantModified    int
	}{
		{
			name:        "the connected definer may replace its own routine",
			desiredBody: "RETURN 2", desiredSecurity: "DEFINER",
			currentBody: "RETURN 1", currentSecurity: "DEFINER",
			definer: "migrator_a@%", currentAccount: "migrator_a@%",
			wantModified: 1,
		},
		{
			name:        "an explicit change to invoker rights names the principal change",
			desiredBody: "RETURN 2", desiredSecurity: "INVOKER",
			currentBody: "RETURN 1", currentSecurity: "DEFINER",
			definer: "owner_a@%", currentAccount: "migrator_a@%",
			wantModified: 1,
		},
		{
			name:        "an unchanged foreign definer needs no replacement",
			desiredBody: "RETURN 1", desiredSecurity: "DEFINER",
			currentBody: "RETURN 1", currentSecurity: "DEFINER",
			definer: "owner_a@%", currentAccount: "migrator_a@%",
			wantModified: 0,
		},
		{
			name:        "invoker execution does not use the definer principal",
			desiredBody: "RETURN 2", desiredSecurity: "INVOKER",
			currentBody: "RETURN 1", currentSecurity: "INVOKER",
			definer: "owner_a@%", currentAccount: "migrator_a@%",
			wantModified: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDatabaseInfo(
				mysqlDefinerDesired(test.desiredBody, test.desiredSecurity),
				mysqlDefinerCurrent(
					test.currentBody,
					test.currentSecurity,
					test.definer,
					test.currentAccount,
				),
				mysqlDefinerInfo("mysql"),
				nil,
			)

			c.Assert(err, qt.IsNil)
			c.Assert(diff.FunctionsModified, qt.HasLen, test.wantModified)
		})
	}
}

func mysqlDefinerDesired(body, security string) *goschema.Database {
	return &goschema.Database{Functions: []goschema.Function{{
		Name: "f", Returns: "int", Language: "sql",
		Security: security, Volatility: "IMMUTABLE", Body: body,
	}}}
}

func mysqlDefinerCurrent(body, security, definer, currentAccount string) *types.DBSchema {
	return &types.DBSchema{Functions: []types.DBFunction{{
		Name: "f", Schema: "app", Returns: "int", Language: "sql",
		Security: security, Volatility: "IMMUTABLE", Body: body,
		Definer: definer, CurrentAccount: currentAccount,
	}}}
}

func mysqlDefinerInfo(dialect string) types.DBInfo {
	semantics := identifier.ForDialect(dialect)
	semantics.DefaultSchema = "app"
	return types.DBInfo{
		Dialect:             dialect,
		Schema:              "app",
		IdentifierSemantics: semantics,
	}
}
