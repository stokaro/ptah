// Command renovateregex compiles every custom-manager pattern in renovate.json
// with Go's regexp package, which is RE2 -- the engine Renovate evaluates them
// with.
//
// A pattern that RE2 cannot compile does not degrade: Renovate refuses the whole
// configuration and stops opening pull requests until somebody fixes it. That is
// what stokaro/ptah#2339 was, from a `\k<depName>` backreference -- valid in
// JavaScript, absent from RE2, and invisible to every check this repository ran.
//
// It is invisible to `renovate-config-validator` too. That tool loads the `re2`
// native module and falls back to JavaScript's own engine when the module is not
// built; measured on 2026-08-27, it reported "Config validated successfully" for
// the exact file Renovate had already refused. A validator that answers about a
// different engine than the one that will run the pattern is not evidence about
// this, so the compile happens here instead.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"
)

// config is the part of renovate.json this reads. Everything else is the
// validator's business.
type config struct {
	CustomManagers []struct {
		Description  string   `json:"description"`
		MatchStrings []string `json:"matchStrings"`
	} `json:"customManagers"`
}

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
	var cfg config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		failf("%s: %v", path, err)
	}

	// A file that declares no custom manager is not a pass to report. This gate
	// exists because the patterns are unchecked anywhere else, and one that
	// compiled nothing would report the same success on the day the managers
	// were renamed out from under it.
	if len(cfg.CustomManagers) == 0 {
		failf("%s declares no customManagers; refusing to report a vacuous pass", path)
	}

	checked := 0
	failures := 0
	for i, manager := range cfg.CustomManagers {
		if len(manager.MatchStrings) == 0 {
			failf("%s: customManagers[%d] declares no matchStrings", path, i)
		}
		for j, pattern := range manager.MatchStrings {
			checked++
			if _, err := regexp.Compile(re2Spelling(pattern)); err != nil {
				failures++
				fmt.Fprintf(os.Stderr, "%s: customManagers[%d].matchStrings[%d] does not compile under RE2: %v\n",
					path, i, j, err)
				fmt.Fprintf(os.Stderr, "  %s\n", pattern)
			}
		}
	}
	if failures > 0 {
		fmt.Fprintf(os.Stderr, "renovate regex check: %d of %d pattern(s) would stop Renovate\n", failures, checked)
		os.Exit(1)
	}
	fmt.Printf("renovate regex check: OK (%d pattern(s) compile under RE2)\n", checked)
}

// re2Spelling converts a named group from the spelling Renovate's patterns use
// to the one Go's parser wants. They are the same construct: RE2 accepts
// `(?P<name>`, and the `re2` binding Renovate uses accepts `(?<name>` for it.
// Rewriting is what lets this compile the file's own bytes rather than a
// transcription of them.
func re2Spelling(pattern string) string {
	return strings.ReplaceAll(pattern, "(?<", "(?P<")
}

func failf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "renovate regex check: "+format+"\n", args...)
	os.Exit(1)
}
