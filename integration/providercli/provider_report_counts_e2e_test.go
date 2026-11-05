//go:build integration

package providercli_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestProviderSchemaStats_CountsTheProvidersValues_HappyPath counts the
// fixture's retention values in `schema stats`: the provider declares two
// metrics on its retention models and counts each value, the one retention of
// 30 seconds its read gives the table records. The host counts the common
// objects beside them.
func TestProviderSchemaStats_CountsTheProvidersValues_HappyPath(t *testing.T) {
	c := qt.New(t)
	f := readExportProvider(c, "FIXTURE_REPORT_COUNTS=1")

	stdout, stderr, code := executeProviderCLI(c.Context(), f.arguments("stats"))

	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", stderr))
	c.Assert(stdout, qt.Contains, "ptah_schema_tables{dialect=\"fixture\"} 1\n")
	c.Assert(stdout, qt.Contains, "# HELP ptah_schema_fixture_retained_objects Objects with a captured fixture retention.\n")
	c.Assert(stdout, qt.Contains, "ptah_schema_fixture_retained_objects{dialect=\"fixture\"} 1\n")
	c.Assert(stdout, qt.Contains, "# HELP ptah_schema_fixture_retention_seconds Seconds of fixture retention across objects.\n")
	c.Assert(stdout, qt.Contains, "ptah_schema_fixture_retention_seconds{dialect=\"fixture\"} 30\n")
	c.Assert(stdout, qt.Matches, `(?s).*# EOF\n`)
}

// TestProviderSchemaStats_ProviderWithoutReportCounts is the control: the same
// read through a provider that declares no metric reaches the scrape with the
// common counts only.
func TestProviderSchemaStats_ProviderWithoutReportCounts(t *testing.T) {
	c := qt.New(t)
	f := readExportProvider(c)

	stdout, stderr, code := executeProviderCLI(c.Context(), f.arguments("stats"))

	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", stderr))
	c.Assert(stdout, qt.Contains, "ptah_schema_tables{dialect=\"fixture\"} 1\n")
	c.Assert(stdout, qt.Not(qt.Contains), "fixture_retained_objects")
	c.Assert(stdout, qt.Not(qt.Contains), "fixture_retention_seconds")
}
