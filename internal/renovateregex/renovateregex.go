// Package renovateregex compiles Renovate's custom-manager patterns with the
// engine Renovate evaluates them with.
//
// A pattern RE2 cannot compile does not degrade to a missed bump: Renovate
// refuses the whole configuration and stops opening pull requests until
// somebody fixes it. That is what stokaro/ptah#2339 was, from a `\k<depName>`
// backreference -- valid in JavaScript, absent from RE2, and invisible to every
// check this repository ran.
//
// It is invisible to `renovate-config-validator` too. That tool loads the `re2`
// native module and falls back to JavaScript's own engine when the module is
// not built; measured on 2026-08-27, it reported "Config validated
// successfully" for the exact file Renovate had already refused. A validator
// that answers about a different engine than the one that will run the pattern
// is not evidence about this.
//
// The rules live here rather than in the command so their fixtures can sit
// beside them, which is where a known-bad fixture belongs
// (stokaro/ptah#2509).
package renovateregex

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

// Config is the part of renovate.json this reads. Everything else is the
// validator's business.
type Config struct {
	CustomManagers []Manager `json:"customManagers"`
}

// Manager is one custom manager and the patterns it matches with.
type Manager struct {
	Description  string   `json:"description"`
	MatchStrings []string `json:"matchStrings"`
}

// Parse reads a renovate.json.
func Parse(raw []byte) (Config, error) {
	var config Config
	if err := json.Unmarshal(raw, &config); err != nil {
		return Config{}, err
	}
	return config, nil
}

// Result is what one run found.
type Result struct {
	// Checked is how many patterns compiled successfully or not; a run that
	// checked none is refused rather than reported.
	Checked int
	// Findings are the patterns RE2 rejects, and the refusals that stop the
	// run before any pattern is reached.
	Findings []string
}

// OK reports whether Renovate would accept the configuration.
func (r Result) OK() bool { return len(r.Findings) == 0 }

// Check compiles every pattern.
//
// A file that declares no custom manager is not a pass to report. This exists
// because the patterns are unchecked anywhere else, and a run that compiled
// nothing would report the same success on the day the managers were renamed
// out from under it.
func Check(config Config) Result {
	if len(config.CustomManagers) == 0 {
		return Result{Findings: []string{"renovate.json declares no customManagers; refusing to report a vacuous pass"}}
	}
	result := Result{}
	for i, manager := range config.CustomManagers {
		if len(manager.MatchStrings) == 0 {
			result.Findings = append(result.Findings, fmt.Sprintf(
				"customManagers[%d] declares no matchStrings", i))
			continue
		}
		for j, pattern := range manager.MatchStrings {
			result.Checked++
			if _, err := regexp.Compile(RE2Spelling(pattern)); err != nil {
				result.Findings = append(result.Findings, fmt.Sprintf(
					"customManagers[%d].matchStrings[%d] does not compile under RE2: %v\n  %s",
					i, j, err, pattern))
			}
		}
	}
	return result
}

// RE2Spelling converts a named group from the spelling Renovate's patterns use
// to the one Go's parser wants.
//
// They are the same construct: RE2 accepts `(?P<name>`, and the `re2` binding
// Renovate uses accepts `(?<name>` for it. Rewriting is what lets this compile
// the file's own bytes rather than a transcription of them.
func RE2Spelling(pattern string) string {
	return strings.ReplaceAll(pattern, "(?<", "(?P<")
}
