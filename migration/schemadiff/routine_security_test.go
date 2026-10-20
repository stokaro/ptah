package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlashcl"
	"ptah.run/migration/schemadiff"
)

// reportedProcedure is the procedure [languageProcedure] declares, as a server
// reports it: in language, running with security.
func reportedProcedure(language, security string) *catalog.Database {
	return &catalog.Database{Functions: []catalog.Function{{
		Name: "touch", Schema: "app", Kind: "procedure", Language: language,
		Security: security, Volatility: "VOLATILE", Body: "BEGIN UPDATE t SET c = 1; END",
	}}}
}

// TestCompareWithDialect_AnUnstatedSecurityModeIsTheEnginesOwn pins that a
// routine whose declaration states no security mode runs with the rights its
// engine gives one.
//
// MySQL, MariaDB and Oracle run such a routine with its definer's rights, and
// PostgreSQL and SQL Server with its caller's. A procedure created on each by
// hand, or by the Atlas CLI from the same HCL, compares equal to the
// declaration; read as INVOKER everywhere, it compares as `security: DEFINER
// -> INVOKER` on the first three and is replaced on every run.
func TestCompareWithDialect_AnUnstatedSecurityModeIsTheEnginesOwn(t *testing.T) {
	for _, test := range []struct {
		dialect  string
		language string
		security string
	}{
		{dialect: "mysql", language: "sql", security: "DEFINER"},
		{dialect: "mariadb", language: "sql", security: "DEFINER"},
		{dialect: "oracle", language: "plsql", security: "DEFINER"},
		{dialect: "sqlserver", language: "sql", security: "INVOKER"},
		{dialect: "postgres", language: "plpgsql", security: "INVOKER"},
	} {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			desired := must.Must(atlashcl.Parse(t.Context(), []byte(languageProcedure), "schema.hcl"))

			diff, err := schemadiff.CompareWithDialect(t.Context(), desired, reportedProcedure(test.language, test.security),
				test.dialect, must.Must(builtin.New()))

			c.Assert(err, qt.IsNil)
			c.Assert(diff.FunctionsModified, qt.HasLen, 0)
		})
	}
}

// TestCompareWithDialect_AnUnstatedSecurityModeReplacesAnInvokerRoutine pins
// what a declaration that states no mode plans against a routine the server
// runs with its caller's rights: SQL SECURITY INVOKER, or AUTHID CURRENT_USER
// on Oracle.
//
// On MySQL, MariaDB and Oracle the declaration means the definer's rights, so
// the plan replaces the routine, and the diff keeps the declaration as
// written: the routine is created with no clause, and the server applies its
// own default. A routine that has to keep its caller's rights states
// security="INVOKER". PostgreSQL and SQL Server are the control below: the
// mode they give is INVOKER.
func TestCompareWithDialect_AnUnstatedSecurityModeReplacesAnInvokerRoutine(t *testing.T) {
	for _, test := range []struct {
		dialect     string
		language    string
		wantChanges map[string]string
	}{
		{dialect: "mysql", language: "sql", wantChanges: map[string]string{"security": "INVOKER -> DEFINER"}},
		{dialect: "mariadb", language: "sql", wantChanges: map[string]string{"security": "INVOKER -> DEFINER"}},
		{dialect: "oracle", language: "plsql", wantChanges: map[string]string{"security": "INVOKER -> DEFINER"}},
	} {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			desired := must.Must(atlashcl.Parse(t.Context(), []byte(languageProcedure), "schema.hcl"))

			diff, err := schemadiff.CompareWithDialect(t.Context(), desired, reportedProcedure(test.language, "INVOKER"),
				test.dialect, must.Must(builtin.New()))

			c.Assert(err, qt.IsNil)
			c.Assert(diff.FunctionsModified, qt.HasLen, 1)
			c.Assert(diff.FunctionsModified[0].Changes, qt.DeepEquals, test.wantChanges)
			c.Assert(diff.FunctionsModified[0].Desired.Security, qt.Equals, "")
		})
	}
}

// The control for the test above: on PostgreSQL and SQL Server an unstated
// mode is INVOKER, so an INVOKER routine on the server compares equal.
func TestCompareWithDialect_AnUnstatedSecurityModeKeepsAnInvokerRoutine(t *testing.T) {
	for _, test := range []struct {
		dialect  string
		language string
	}{
		{dialect: "sqlserver", language: "sql"},
		{dialect: "postgres", language: "plpgsql"},
	} {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			desired := must.Must(atlashcl.Parse(t.Context(), []byte(languageProcedure), "schema.hcl"))

			diff, err := schemadiff.CompareWithDialect(t.Context(), desired, reportedProcedure(test.language, "INVOKER"),
				test.dialect, must.Must(builtin.New()))

			c.Assert(err, qt.IsNil)
			c.Assert(diff.FunctionsModified, qt.HasLen, 0)
		})
	}
}
