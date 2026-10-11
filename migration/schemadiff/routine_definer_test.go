package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

// TestCompareWithDatabaseInfoRefusesAForeignDefinerReplacement is the
// regression for the unanswered review on stokaro/ptah#1461. MySQL and
// MariaDB do not support CREATE OR REPLACE FUNCTION, so a modification is a
// DROP followed by CREATE. Recreating another account's SQL SECURITY DEFINER
// routine as the connected account silently changes the principal under which
// its body executes.
//
// A routine that states no security mode is a definer routine on these
// engines, as one created by hand is, and is refused the same way.
func TestCompareWithDatabaseInfoRefusesAForeignDefinerReplacement(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		security string
	}{
		{name: "mysql", dialect: "mysql", security: "DEFINER"},
		{name: "mariadb", dialect: "mariadb", security: "DEFINER"},
		{name: "mysql, no mode stated", dialect: "mysql", security: ""},
		{name: "mariadb, no mode stated", dialect: "mariadb", security: ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDatabaseInfo(
				t.Context(), mysqlDefinerDesired("RETURN 2", test.security),
				mysqlDefinerCurrent("RETURN 1", "DEFINER", "owner_a@%", "migrator_a@%"),
				mysqlDefinerInfo(test.dialect),
				nil, must.Must(builtin.New()),
			)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches,
				`.*cannot safely replace.*function "f".*definer "owner_a@%".*connected account "migrator_a@%".*execution principal.*`)
			c.Assert(diff, qt.IsNil)
		})
	}
}

// TestCompareWithDatabaseInfoRefusesAReplacementWithoutOwnershipFacts keeps
// the safety rule fail closed when the read did not record a fact: a
// programmatic caller that builds a live-like Function without the reader-only
// fields, a description rather than a live read (neither fact is serialized),
// or a reading account the server did not answer for. Treating missing facts
// as same-owner would recreate the exact silent principal change this guard
// exists to prevent.
func TestCompareWithDatabaseInfoRefusesAReplacementWithoutOwnershipFacts(t *testing.T) {
	tests := []struct {
		name           string
		definer        string
		currentAccount string
	}{
		{name: "neither fact, as a description has", definer: "", currentAccount: ""},
		{name: "a definer and no reading account", definer: "owner_a@%", currentAccount: ""},
		{name: "a reading account and no definer", definer: "", currentAccount: "migrator_a@%"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDatabaseInfo(
				t.Context(), mysqlDefinerDesired("RETURN 2", "DEFINER"),
				mysqlDefinerCurrent("RETURN 1", "DEFINER", test.definer, test.currentAccount),
				mysqlDefinerInfo("mysql"),
				nil, must.Must(builtin.New()),
			)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, `.*cannot safely replace.*ownership facts are incomplete.*`)
			c.Assert(diff, qt.IsNil)
		})
	}
}

// TestCompareWithDatabaseInfoGatesDefinerReplacementOnTheCapability proves
// the gate is the capability, not the dialect: a PostgreSQL comparison whose
// capability set says the plan replaces a routine by DROP and CREATE refuses
// the replacement, and a MySQL one whose set says otherwise allows it.
func TestCompareWithDatabaseInfoGatesDefinerReplacementOnTheCapability(t *testing.T) {
	c := qt.New(t)
	postgres := mysqlDefinerInfo("postgres")
	postgres.Capabilities = capability.Postgres18().With(capability.RoutineReplacementResetsDefiner, true)

	diff, err := schemadiff.CompareWithDatabaseInfo(
		t.Context(), mysqlDefinerDesired("RETURN 2", "DEFINER"),
		mysqlDefinerCurrent("RETURN 1", "DEFINER", "owner_a", "migrator_a"),
		postgres, nil, must.Must(builtin.New()),
	)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `.*cannot safely replace postgres function "f".*definer "owner_a".*connected account "migrator_a".*`)
	c.Assert(diff, qt.IsNil)
}

// TestCompareWithDatabaseInfoAllowsDefinerReplacementWithoutTheCapability is
// the other half: the dialect alone does not fire the check.
func TestCompareWithDatabaseInfoAllowsDefinerReplacementWithoutTheCapability(t *testing.T) {
	tests := []struct {
		name string
		info catalog.ServerInfo
	}{
		{name: "postgres preset", info: mysqlDefinerInfo("postgres")},
		{name: "mysql with the capability off", info: withCapabilities(mysqlDefinerInfo("mysql"),
			capability.MySQL84().With(capability.RoutineReplacementResetsDefiner, false))},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDatabaseInfo(
				t.Context(), mysqlDefinerDesired("RETURN 2", "DEFINER"),
				mysqlDefinerCurrent("RETURN 1", "DEFINER", "owner_a", "migrator_a"),
				test.info, nil, must.Must(builtin.New()),
			)

			c.Assert(err, qt.IsNil)
			c.Assert(diff.FunctionsModified, qt.HasLen, 1)
		})
	}
}

func withCapabilities(info catalog.ServerInfo, caps capability.Capabilities) catalog.ServerInfo {
	info.Capabilities = caps
	return info
}

// TestCompareWithDatabaseInfoAllowsAForeignDefinerLanguageThisTargetSkips
// keeps the ownership gate aligned with the planner's executable boundary. A
// MySQL-family target leaves a non-SQL function unchanged and emits only a
// named skip comment, so no drop/create pair exists that could adopt the
// connected account as a new definer.
func TestCompareWithDatabaseInfoAllowsAForeignDefinerLanguageThisTargetSkips(t *testing.T) {
	c := qt.New(t)
	desired := mysqlDefinerDesired("RETURN 2", "DEFINER")
	// A routine declared in plpgsql, which MySQL does not run. An omitted
	// language is the target's own, SQL, and takes the replacement path.
	desired.Functions[0].Language = "plpgsql"

	diff, err := schemadiff.CompareWithDatabaseInfo(
		t.Context(), desired,
		mysqlDefinerCurrent("RETURN 1", "DEFINER", "owner_a@%", "migrator_a@%"),
		mysqlDefinerInfo("mysql"),
		nil, must.Must(builtin.New()),
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
				t.Context(), mysqlDefinerDesired(test.desiredBody, test.desiredSecurity),
				mysqlDefinerCurrent(
					test.currentBody,
					test.currentSecurity,
					test.definer,
					test.currentAccount,
				),
				mysqlDefinerInfo("mysql"),
				nil, must.Must(builtin.New()),
			)

			c.Assert(err, qt.IsNil)
			c.Assert(diff.FunctionsModified, qt.HasLen, test.wantModified)
		})
	}
}

func mysqlDefinerDesired(body, security string) *schemamodel.Database {
	return &schemamodel.Database{Functions: []schemamodel.Function{{
		Name: "f", Returns: "int", Language: "sql",
		Security: security, Volatility: "IMMUTABLE", Body: body,
	}}}
}

func mysqlDefinerCurrent(body, security, definer, currentAccount string) *catalog.Database {
	return &catalog.Database{CurrentAccount: currentAccount, Functions: []catalog.Function{{
		Name: "f", Schema: "app", Returns: "int", Language: "sql",
		Security: security, Volatility: "IMMUTABLE", Body: body,
		Definer: definer,
	}}}
}

func mysqlDefinerInfo(dialect string) catalog.ServerInfo {
	semantics := identifier.ForDialect(dialect)
	semantics.DefaultSchema = "app"
	return catalog.ServerInfo{
		Dialect:             dialect,
		Schema:              "app",
		IdentifierSemantics: semantics,
	}
}
