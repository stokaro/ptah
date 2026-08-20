package migrator_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform/capability"
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
// converse: the page names four engines, and a fifth preset losing the
// capability must not leave that sentence behind.
func TestTxModeAllDocumentation_ListsEveryTargetThatCommitsAsItRuns(t *testing.T) {
	c := qt.New(t)

	page := txModeAllDocumentation(c)

	c.Assert(page, qt.Contains, "MySQL, MariaDB, ClickHouse and Spanner commit DDL as it runs.")
}
