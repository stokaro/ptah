package risk_test

import (
	"fmt"

	"go.5x5.cz/ptah/migration/risk"
)

// ExampleIsBlocking answers the one question a gate-writer has: which
// severities fail a safety gate by default. Error and Destructive block —
// they are the same rank expressed in different output vocabularies — while
// Safe, Info and Warning pass. Info in particular exists so a report can
// surface a finding that a gate never acts on.
func ExampleIsBlocking() {
	severities := []risk.Severity{risk.Safe, risk.Info, risk.Warning, risk.Error, risk.Destructive}

	for _, severity := range severities {
		fmt.Printf("%s: %t\n", severity, risk.IsBlocking(severity))
	}

	// Output:
	// safe: false
	// info: false
	// warning: false
	// error: true
	// destructive: true
}

// ExampleSARIFLevel prints the complete mapping of Ptah severities onto
// three of SARIF's result levels, for an embedder publishing findings to a
// SARIF consumer. Info maps to "note", the level below "warning": an advisory
// finding a viewer should show and no gate should act on does not ask for the
// review that "warning" implies.
func ExampleSARIFLevel() {
	severities := []risk.Severity{risk.Safe, risk.Info, risk.Warning, risk.Error, risk.Destructive}

	for _, severity := range severities {
		fmt.Printf("%s -> %s\n", severity, risk.SARIFLevel(severity))
	}

	// Output:
	// safe -> warning
	// info -> note
	// warning -> warning
	// error -> error
	// destructive -> error
}
