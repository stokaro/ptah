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

// resolvedOutput is the third accepted shape: a setup-go key reading a value
// another step in the same manifest resolved.
//
//	${{ steps.go-toolchain.outputs.version }}
//
// It exists because setup-go fails outright on a `go-version-file` that is not
// there, and a composite action runs in the CALLER's workspace: a caller whose
// module sits in a subdirectory, or who has no module at all, would lose the
// run on the absence of a file it never named. Resolving the two inputs into
// one pair of values before the step is what survives that, and the step
// boundary is then what this gate has to look through.
//
// It looks through it by reading the resolving step rather than by reading
// shell. checkResolvingStep below requires every value that step sees to be one
// of this action's own inputs, requires each role to name the input that fills
// it, and refuses a version literal there as flatly as anywhere else -- so the
// exemption stays what the other two shapes are: the action states no version,
// and the caller's module is what decides.
var resolvedOutput = regexp.MustCompile(`^\$\{\{\s*steps\.([A-Za-z0-9_-]+)\.outputs\.[A-Za-z0-9_-]+\s*\}\}$`)

// resolvedFrom returns the step a value was resolved by.
func resolvedFrom(value string) (string, bool) {
	if resolved := resolvedOutput.FindStringSubmatch(value); resolved != nil {
		return resolved[1], true
	}
	return "", false
}

// derivesFromInput reports whether a value comes from the action's own inputs,
// under any accepted shape.
func derivesFromInput(value string) bool {
	if _, forwards := forwardedName(value); forwards {
		return true
	}
	_, resolves := resolvedFrom(value)
	return resolves
}

// The variables a resolving step declares its sources in, one per role.
//
// Which input fills which role cannot be read out of a script, so it is
// declared where YAML carries it: one variable per role. The INPUT names stay
// free -- each is read from the forward rather than assumed, which is what a
// rename put outside the old detector's reach -- and only the two slots are
// fixed.
const (
	versionVariable = "GO_VERSION"
	moduleVariable  = "GO_VERSION_FILE"
)

// goVersionLiteral matches a value that names a Go release. A resolving step
// may pass a version through and may fall back to a selector that names no
// release, but naming one is the drift this gate exists to stop, wherever it is
// written.
var goVersionLiteral = regexp.MustCompile(`\b(?:go)?\d+\.(?:\d+|x)\b`)

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
	// judged records the resolving steps this manifest has already reported on.
	// Both keys of one setup-go step usually read the same step, and a second
	// copy of every finding would read as a second defect.
	judged := make(map[string]bool)
	for _, step := range manifest.Steps {
		if !step.HasVersion && !step.HasVersionFile {
			findings = append(findings, Finding{File: step.File, Line: step.Line, Message: "this setup-go step names no Go version source. Add `go-version-file: go.mod`, which is where the toolchain is declared."})
			continue
		}
		if step.HasVersion {
			findings = append(findings, checkVersionKey(manifest, step, forwarded, judged)...)
		}
		if step.HasVersionFile {
			findings = append(findings, checkVersionFileKey(manifest, step, forwarded, judged)...)
		}
	}
	return findings
}

// checkVersionKey judges `go-version`, which may only be an input the action
// forwards or resolved.
func checkVersionKey(manifest Manifest, step SetupGoStep, forwarded map[string][]string, judged map[string]bool) []Finding {
	if _, resolves := resolvedFrom(step.Version); resolves {
		return checkResolved(manifest, step, resolvedKey{
			Key:      "go-version",
			Value:    step.Version,
			Role:     "version",
			Variable: versionVariable,
			Remedy:   "Use `go-version-file: go.mod`.",
			Line:     step.VersionLine,
		}, forwarded, judged)
	}
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
func checkVersionFileKey(manifest Manifest, step SetupGoStep, forwarded map[string][]string, judged map[string]bool) []Finding {
	if _, resolves := resolvedFrom(step.VersionFile); resolves {
		return checkResolved(manifest, step, resolvedKey{
			Key:      "go-version-file",
			Value:    step.VersionFile,
			Role:     "file",
			Variable: moduleVariable,
			Remedy:   "Name go.mod directly.",
			Line:     step.VersionFileLine,
		}, forwarded, judged)
	}
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

// resolvedKey is one setup-go key whose value another step resolved, with the
// role that key gives it.
type resolvedKey struct {
	Key, Value, Role, Variable, Remedy string
	Line                               int
}

// checkResolved judges one key that reads a resolving step's output, and
// records the input behind it under the role the key gives it -- so a forward
// that travels through a step is judged by CheckForwardedDefaults exactly as a
// direct one is. The step boundary moves where the value is assembled, never
// what the action is contracted to read.
func checkResolved(manifest Manifest, step SetupGoStep, key resolvedKey, forwarded map[string][]string, judged map[string]bool) []Finding {
	if !isActionManifest(manifest.Path) {
		return []Finding{{File: step.File, Line: key.Line, Message: fmt.Sprintf(
			"%s is %s, and only a composite action manifest may read a resolving step's output: a workflow has a module to read, and a step output there hides which toolchain the job builds with. %s",
			key.Key, key.Value, key.Remedy)}}
	}
	id, _ := resolvedFrom(key.Value)
	findings := checkResolvingStep(manifest, step.File, key.Line, id, judged)
	source, declared := resolvedSource(manifest.Resolvers[id], key.Variable)
	if !declared {
		return findings
	}
	name, forwards := forwardedName(source)
	if !forwards {
		return findings
	}
	forwarded[key.Role] = append(forwarded[key.Role], manifest.Path+":"+name)
	return findings
}

// checkResolvingStep judges the step a setup-go key reads its value from, once
// per manifest reference.
//
// What none of these rules can check is which branch the script takes, so the
// script is held to passing values through: it may hand over what an input
// named, and it may fall back to a selector that names no release, but it may
// not introduce a version, read anything the env block did not declare, or
// leave a declared input unread.
func checkResolvingStep(manifest Manifest, file string, line int, id string, judged map[string]bool) []Finding {
	if judged[id] {
		return nil
	}
	judged[id] = true
	resolver, declared := manifest.Resolvers[id]
	if !declared {
		return []Finding{{File: file, Line: line, Message: fmt.Sprintf(
			"the Go version is resolved by step %q, which this manifest does not declare. A step output is judged by reading the step that writes it, so it has to be here to be read.",
			id)}}
	}

	var findings []Finding
	for _, entry := range resolver.Env {
		if _, forwards := forwardedName(entry.Value); !forwards {
			findings = append(findings, Finding{File: resolver.File, Line: entry.Line, Message: fmt.Sprintf(
				"step %q resolves the Go version and reads %s from %q, which is not one of this action's inputs. Such a step derives from the action's own inputs and from nothing else: what reaches setup-go is then what the caller asked for.",
				id, entry.Name, entry.Value)})
		}
		if !reads(resolver.Run, entry.Name) {
			findings = append(findings, Finding{File: resolver.File, Line: entry.Line, Message: fmt.Sprintf(
				"step %q declares %s and its script never reads it, so the input behind it decides nothing. Read it or stop declaring it: an input the resolver ignores reads as a source and is not one.",
				id, entry.Name)})
		}
	}
	for _, variable := range []string{versionVariable, moduleVariable} {
		if _, present := resolvedSource(resolver, variable); !present {
			findings = append(findings, Finding{File: resolver.File, Line: resolver.Line, Message: fmt.Sprintf(
				"step %q resolves the Go version and declares no %s. A resolving step names the input "+
					"filling each role in one variable -- %s for the version handed to setup-go, %s for the "+
					"module it reads -- because that is the only place the mapping can be read, and each "+
					"input's default is judged by the role it fills.",
				id, variable, versionVariable, moduleVariable)})
		}
	}
	return append(findings, checkResolvingScript(resolver, id)...)
}

// checkResolvingScript reads the step's body for what it must not carry.
func checkResolvingScript(resolver ResolverStep, id string) []Finding {
	if !resolver.HasRun {
		return []Finding{{File: resolver.File, Line: resolver.Line, Message: fmt.Sprintf(
			"step %q resolves the Go version and runs no script of its own, so what it writes to its outputs comes from somewhere this gate cannot read. A resolving step is a script over the env block beside it.",
			id)}}
	}
	var findings []Finding
	if strings.Contains(resolver.Run, "${{") {
		findings = append(findings, Finding{File: resolver.File, Line: resolver.RunLine, Message: fmt.Sprintf(
			"step %q interpolates an expression into its script. Everything it derives from is declared in env, where it can be read; an expression in the body is a second source, and it is the one nothing judges.",
			id)})
	}
	if literal := goVersionLiteral.FindString(resolver.Run); literal != "" {
		findings = append(findings, Finding{File: resolver.File, Line: resolver.RunLine, Message: fmt.Sprintf(
			"step %q resolves the Go version and its script carries the version literal %q. The toolchain is declared once, in the root go.mod; a step that resolves a version passes one through and never names one.",
			id, literal)})
	}
	return findings
}

// resolvedSource returns what a resolving step declares for one role.
func resolvedSource(resolver ResolverStep, variable string) (string, bool) {
	for _, entry := range resolver.Env {
		if entry.Name == variable {
			return entry.Value, true
		}
	}
	return "", false
}

// reads reports whether a script reads one variable, as a whole word: a body
// naming GO_VERSION_FILE does not read GO_VERSION.
func reads(script, name string) bool {
	return regexp.MustCompile(`\b` + regexp.QuoteMeta(name) + `\b`).MatchString(script)
}
