package migrator_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
)

// The `--tx-mode all` exclusions were explained in three places and in none of
// them completely, so a user adopting the mode met them one migration at a
// time. There is one page now, and these hold it to the code
// (stokaro/ptah#1713).
//
// The refusals are matched as the substrings the code formats rather than by
// reaching for the unexported constants: a same-package test could read them
// directly, but the page has to carry the words a user actually sees, and a
// constant compared to itself would pass however the page was worded.

// txModeAllDocumentation reads the page that owns the interaction.
func txModeAllDocumentation(c *qt.C) string {
	c.Helper()
	path := filepath.Join(
		"..", "..", "docs", "site", "src", "content", "docs", "versioned", "apply.md",
	)
	raw, err := os.ReadFile(path)
	c.Assert(err, qt.IsNil)
	return string(raw)
}

func TestTxModeAllDocumentation_CarriesTheSection(t *testing.T) {
	c := qt.New(t)

	page := txModeAllDocumentation(c)

	c.Assert(page, qt.Contains, "### What `--tx-mode all` cannot carry")
}

// TestTxModeAllDocumentation_QuotesTheRefusalsTheCodeProduces is the point:
// each refusal is quoted on the page, so rewording one in the code without
// touching the page fails here rather than leaving a reader holding a message
// that no longer exists.
func TestTxModeAllDocumentation_QuotesTheRefusalsTheCodeProduces(t *testing.T) {
	c := qt.New(t)
	page := txModeAllDocumentation(c)
	quoted := []string{
		"declares pre-migration checks, which cannot run with tx-mode all",
		"declares timeouts, which cannot run with tx-mode all",
		"tx-mode all is not supported for dialect",
		"this target commits schema changes as they run, so a failed migration cannot be rolled back as a unit",
	}

	for _, phrase := range quoted {
		c.Assert(page, qt.Contains, phrase,
			qt.Commentf("the page must quote the refusal the code produces"))
	}
}

// TestTxModeAllDocumentation_NamesTheCapacityItDecidesOn holds the other half
// of the claim: the page says the dialect gate is a capability rather than a
// list, and the gate is.
func TestTxModeAllDocumentation_NamesTheCapabilityItDecidesOn(t *testing.T) {
	c := qt.New(t)

	page := txModeAllDocumentation(c)

	c.Assert(page, qt.Contains, "transactional-DDL capability")
	c.Assert(capability.Postgres16().Has(capability.TransactionalDDL), qt.IsTrue)
	c.Assert(capability.MySQL84().Has(capability.TransactionalDDL), qt.IsFalse)
	c.Assert(capability.MariaDB1011().Has(capability.TransactionalDDL), qt.IsFalse)
	c.Assert(capability.ClickHouse24().Has(capability.TransactionalDDL), qt.IsFalse)
	c.Assert(capability.SpannerPostgres().Has(capability.TransactionalDDL), qt.IsFalse)
}

// TestTxModeAllDocumentation_ListsEveryTargetThatCommitsAsItRuns is the
// converse, and it derives the list rather than quoting it.
//
// A sentence pinned as a literal agrees with itself however the presets move:
// the page named MySQL, MariaDB, ClickHouse and Spanner while CockroachDB and
// Oracle refused the mode too, and an assertion on those words could not see
// it. Here the refused set comes from the presets, so a preset crossing the
// line in either direction fails until the page says so.
func TestTxModeAllDocumentation_ListsEveryTargetThatCommitsAsItRuns(t *testing.T) {
	c := qt.New(t)

	page := txModeAllDocumentation(c)

	for _, target := range txModeAllTargets {
		t.Run(target.dialect, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(
				capability.ForDialect(target.dialect).Has(capability.TransactionalDDL),
				qt.IsFalse,
				qt.Commentf("this table lists the targets the mode refuses"),
			)
			c.Assert(page, qt.Contains, target.pageName,
				qt.Commentf("the page must name every target that refuses --tx-mode all"))
		})
	}
}

// txModeAllTargets are the dialects whose preset refuses `--tx-mode all`, with
// the spelling the page owes each one.
//
// Naming the page spelling beside the dialect is what makes the row readable:
// `platform.SQLServer` is `sqlserver` and no page writes it that way.
var txModeAllTargets = []struct {
	dialect  string
	pageName string
}{
	{dialect: platform.MySQL, pageName: "MySQL"},
	{dialect: platform.MariaDB, pageName: "MariaDB"},
	{dialect: platform.ClickHouse, pageName: "ClickHouse"},
	{dialect: platform.Oracle, pageName: "Oracle"},
	{dialect: platform.Spanner, pageName: "Spanner"},
	{dialect: platform.CockroachDB, pageName: "CockroachDB"},
}

// TestTxModeAllDocumentation_CensusMatchesEveryPreset holds the other end of the
// pair. The table above is hand-written, so a preset losing the capability
// without joining it would leave the page silent about a target that refuses,
// and this test would pass on the rows that remain. The census walks every
// dialect [capability.ForDialect] has a preset for and compares both ways.
func TestTxModeAllDocumentation_CensusMatchesEveryPreset(t *testing.T) {
	c := qt.New(t)

	listed := make(map[string]bool, len(txModeAllTargets))
	for _, target := range txModeAllTargets {
		listed[target.dialect] = true
	}

	dialects := capability.DefaultDialects()
	c.Assert(len(dialects) > len(txModeAllTargets), qt.IsTrue,
		qt.Commentf("some target must accept the mode, or this census compares an empty claim"))

	for _, dialect := range dialects {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			wraps := capability.ForDialect(dialect).Has(capability.TransactionalDDL)
			c.Assert(listed[dialect], qt.Equals, !wraps,
				qt.Commentf("a dialect refusing --tx-mode all belongs in txModeAllTargets, and one accepting it does not"))
		})
	}
}
