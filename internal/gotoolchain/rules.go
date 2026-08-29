package gotoolchain

import (
	"fmt"
	"regexp"
	"strings"
)

// forwardedInput matches a step that hands one of its own action's inputs to
// setup-go: `${{ inputs.go-version }}`.
//
// A `${{ }}` expression is opaque -- it shows that a value is derived, never
// from what -- so it is accepted in exactly one place: a composite action
// manifest forwarding its own input, because such an action runs in the
// CALLER's workspace and must not be pinned to this repository's go.mod. What
// the forward resolves to is pinned separately, by the input's default.
var forwardedInput = regexp.MustCompile(`^\$\{\{\s*inputs\.([A-Za-z0-9_-]+)\s*\}\}$`)

// guardedForward is the second accepted shape, and the only one: hand the file
// input over ONLY when the version input is empty, so a caller that named a
// version is not overridden by the default module.
//
//	${{ inputs.go-version == '' && inputs.go-version-file || '' }}
//
// Two shapes rather than an expression parser. GitHub expressions are a
// language, and the point of this rewrite is to stop hand-rolling one; naming
// the two forms this repository writes -- and refusing every other -- is a
// policy a reader can check, where a partial parser is a guess that widens
// every time somebody writes an equivalent spelling.
var guardedForward = regexp.MustCompile(
	`^\$\{\{\s*inputs\.([A-Za-z0-9_-]+)\s*==\s*''\s*&&\s*inputs\.([A-Za-z0-9_-]+)\s*\|\|\s*''\s*\}\}$`)

// forwardedName returns the input a value forwards, under either shape. The
// guarded shape forwards its SECOND reference: the first is the condition.
func forwardedName(value string) (string, bool) {
	if plain := forwardedInput.FindStringSubmatch(value); plain != nil {
		return plain[1], true
	}
	if guarded := guardedForward.FindStringSubmatch(value); guarded != nil {
		return guarded[2], true
	}
	return "", false
}

// rootModules are the spellings that name the module carrying the toolchain.
var rootModules = map[string]bool{"go.mod": true, "./go.mod": true}

// Report is everything one run found.
type Report struct {
	Findings []Finding
	// Steps, Deriving and Forwarding are the census the anti-vacuity floor
	// reads: a scan that matched almost nothing reports zero violations, which
	// is indistinguishable from a clean tree right up until it is not.
	Steps, Deriving, Forwarding int
	Toolchain                   string
}

// OK reports whether the tree satisfies the policy.
func (r Report) OK() bool { return len(r.Findings) == 0 }

// CheckModule is D0: the source exists and NAMES a version.
//
// Counting the directive is not enough, because `toolchain default` is valid Go
// and means "no pin": it is the deletion spelled as a declaration. Measured on
// this module, everything else unchanged, `toolchain go1.26.6` gives
// GOVERSION=go1.26.6 and `toolchain default` gives the `go` directive instead --
// the drift this gate exists to stop, reached without removing a line.
func CheckModule(module Module) []Finding {
	if module.Toolchains != 1 {
		return []Finding{{File: "go.mod", Line: 1, Message: fmt.Sprintf(
			"go.mod declares %d `toolchain` directives; it must declare exactly 1",
			module.Toolchains)}}
	}
	if !strings.HasPrefix(module.Toolchain, "go1.") {
		return []Finding{{File: "go.mod", Line: module.ToolchainLine, Message: fmt.Sprintf(
			"go.mod declares `toolchain %s`, which names no toolchain: setup-go falls back to the `go` directive and CI builds with the compatibility floor. Name a release, as `toolchain go1.x.y`.",
			module.Toolchain)}}
	}
	return nil
}

// CheckSteps is D1: every setup-go step derives its version, and derives it
// from THE source.
//
// Naming a file is not enough on its own. A `go-version-file` pointed at a
// nested module's go.mod derives honestly and still selects the wrong
// toolchain, because such a module carries a compatibility floor and no
// `toolchain` directive -- so setup-go falls back to its `go` line and the job
// quietly builds a patch release behind.
func CheckSteps(manifest Manifest, forwarded map[string][]string) []Finding {
	var findings []Finding
	for _, step := range manifest.Steps {
		if !step.HasVersion && !step.HasVersionFile {
			findings = append(findings, Finding{File: step.File, Line: step.Line, Message: "this setup-go step names no Go version source. Add `go-version-file: go.mod`, which is where the toolchain is declared."})
			continue
		}
		if step.HasVersion {
			findings = append(findings, checkVersionKey(manifest, step, forwarded)...)
		}
		if step.HasVersionFile {
			findings = append(findings, checkVersionFileKey(manifest, step, forwarded)...)
		}
	}
	return findings
}

// checkVersionKey judges `go-version`, which may only be a forwarded input.
func checkVersionKey(manifest Manifest, step SetupGoStep, forwarded map[string][]string) []Finding {
	if name, forwards := forwardedName(step.Version); forwards {
		if !isActionManifest(manifest.Path) {
			return []Finding{{File: step.File, Line: step.VersionLine, Message: fmt.Sprintf(
				"go-version is %s, and only a composite action manifest may forward an input: a workflow has a module to read. Use `go-version-file: go.mod`.",
				step.Version)}}
		}
		forwarded["version"] = append(forwarded["version"], manifest.Path+":"+name)
		return nil
	}
	return []Finding{{File: step.File, Line: step.VersionLine, Message: fmt.Sprintf(
		"go-version is %q. The toolchain is declared once, in the root go.mod; derive it with `go-version-file: go.mod` rather than restating it.",
		step.Version)}}
}

// checkVersionFileKey judges `go-version-file`, which must name the root module.
func checkVersionFileKey(manifest Manifest, step SetupGoStep, forwarded map[string][]string) []Finding {
	if name, forwards := forwardedName(step.VersionFile); forwards {
		if !isActionManifest(manifest.Path) {
			return []Finding{{File: step.File, Line: step.VersionFileLine, Message: fmt.Sprintf(
				"go-version-file is %s, and only a composite action manifest may forward an input: a workflow has a module to read. Name go.mod directly.",
				step.VersionFile)}}
		}
		forwarded["file"] = append(forwarded["file"], manifest.Path+":"+name)
		return nil
	}
	if !rootModules[step.VersionFile] {
		return []Finding{{File: step.File, Line: step.VersionFileLine, Message: fmt.Sprintf(
			"go-version-file names %q, which is not the module that declares the toolchain. Only the root go.mod carries it; a nested module has a compatibility floor and no toolchain directive, so setup-go would build a patch release behind.",
			step.VersionFile)}}
	}
	return nil
}

// CheckForwardedDefaults is D1c: each forwarded input's default is judged by
// the ROLE the forward gives it.
//
//	the input handed to setup-go as the version -> must default to EMPTY
//	the input that names the module             -> must default to the root go.mod
//
// "Empty", not "not a version": `stable`, `oldstable` and `1.x` are all valid
// setup-go selectors and none looks numeric, and any of them non-empty makes
// setup-go prefer go-version and ignore go-version-file entirely.
func CheckForwardedDefaults(manifests map[string]Manifest, forwarded map[string][]string) []Finding {
	var findings []Finding
	for role, references := range forwarded {
		for _, reference := range references {
			path, name, _ := strings.Cut(reference, ":")
			input, declared := manifests[path].Inputs[name]
			if !declared || !input.HasDefault {
				if role == "file" {
					findings = append(findings, Finding{File: path, Line: 1, Message: fmt.Sprintf(
						"input %q names the module setup-go reads and declares no default. An absent default arrives as the empty string, and an empty go-version-file names no module; default it to 'go.mod'.",
						name)})
				}
				continue
			}
			findings = append(findings, checkDefault(path, name, role, input)...)
		}
	}
	return findings
}

func checkDefault(path, name, role string, input Input) []Finding {
	if role == "file" {
		if !rootModules[input.Default] {
			return []Finding{{File: path, Line: input.DefaultLine, Message: fmt.Sprintf(
				"input %q names the module setup-go reads and defaults to %q, which is not the module that declares the toolchain. Default it to 'go.mod'.",
				name, input.Default)}}
		}
		return nil
	}
	if input.Default != "" {
		return []Finding{{File: path, Line: input.DefaultLine, Message: fmt.Sprintf(
			"input %q is handed to setup-go as the Go version and defaults to %q. Any non-empty value "+
				"here -- 'stable' and '1.x' as much as '1.25.0' -- makes setup-go prefer it and ignore "+
				"go-version-file, so the toolchain stops being read from go.mod. Default it to the empty string.",
			name, input.Default)}}
	}
	return nil
}

// isActionManifest reports whether the path is a composite action manifest,
// which is the one place a forwarded input is accepted.
func isActionManifest(path string) bool {
	return strings.HasSuffix(path, "/action.yml") || strings.HasSuffix(path, "/action.yaml")
}
