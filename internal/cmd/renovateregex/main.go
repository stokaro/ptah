// Command renovateregex compiles every custom-manager pattern in renovate.json
// with the engine Renovate evaluates them with.
//
// The rules are internal/renovateregex, where their fixtures sit beside them.
package main

import (
	"fmt"
	"os"

	"ptah.run/internal/renovateregex"
)

func main() {
	// Fixed rather than taken as an argument. This gate is about THIS
	// repository's configuration, the wrapper script chdirs to the repository
	// root before running it, and a path argument would only widen what a gate
	// can be pointed at without making it able to check anything more.
	const path = "renovate.json"

	raw, err := os.ReadFile(path)
	if err != nil {
		failf("%v", err)
	}
	config, err := renovateregex.Parse(raw)
	if err != nil {
		failf("%s: %v", path, err)
	}
	result := renovateregex.Check(config)
	for _, finding := range result.Findings {
		fmt.Fprintf(os.Stderr, "%s: %s\n", path, finding)
	}
	if !result.OK() {
		fmt.Fprintf(os.Stderr, "renovate regex check: %d finding(s) would stop Renovate\n", len(result.Findings))
		os.Exit(1)
	}
	fmt.Printf("renovate regex check: OK (%d pattern(s) compile under RE2)\n", result.Checked)
}

func failf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "renovate regex check: "+format+"\n", args...)
	os.Exit(1)
}
