package featureinventory

import (
	"fmt"
	"slices"
	"sort"
	"strings"
)

// Finding is one defect a check found.
type Finding struct {
	// File and Line locate it. A finding with no line belongs to the file as a
	// whole.
	File string
	Line int
	// Message says what is wrong and, where there is one, what to do.
	Message string
}

func (f Finding) String() string {
	if f.Line > 0 {
		return fmt.Sprintf("%s:%d: %s", f.File, f.Line, f.Message)
	}
	return fmt.Sprintf("%s: %s", f.File, f.Message)
}

// sortFindings orders findings by file and line so a diff of two runs is
// readable.
func sortFindings(findings []Finding) []Finding {
	sort.Slice(findings, func(i, j int) bool {
		if findings[i].File != findings[j].File {
			return findings[i].File < findings[j].File
		}
		if findings[i].Line != findings[j].Line {
			return findings[i].Line < findings[j].Line
		}
		return findings[i].Message < findings[j].Message
	})
	return findings
}

// CheckInventoryCommands compares the command trees with the inventory, in both
// directions.
//
// Both directions, because each catches a different mistake and neither implies
// the other: a command added without a row is an undocumented feature, and a row
// naming a command that no longer exists is an inventory that has outlived its
// subject. A check written in one direction only reports success on half the
// ways this file can be wrong.
func CheckInventoryCommands(census *Census, inventory *Inventory) []Finding {
	var findings []Finding

	claimed := make(map[string]bool)
	for _, row := range inventory.Rows {
		for _, token := range row.Surface {
			if token.Kind != KindCommand {
				continue
			}
			claimed[string(token.Tree)+"\x00"+token.Path] = true
			if _, exists := census.Lookup(token.Tree, token.Path); !exists {
				findings = append(findings, Finding{
					File: inventory.Path, Line: row.Line,
					Message: fmt.Sprintf("row %q claims the command `%s`, which the %s tree does not register", row.ID, token.Raw, token.Tree),
				})
			}
		}
	}

	// Coverage, not exclusivity. A path may be claimed by more than one row on
	// purpose: `ptah mcp` has its row in the native command tree and another in
	// the section about the agent surfaces, and requiring a single owner would
	// report that cross-reference as a defect.
	for _, tree := range []Tree{TreeNative, TreeCompat} {
		for _, cmd := range census.OfTree(tree) {
			if claimed[string(tree)+"\x00"+cmd.Path] {
				continue
			}
			findings = append(findings, Finding{
				File: inventory.Path,
				Message: fmt.Sprintf("the %s tree registers `%s` and no inventory row claims it; add a row whose Public surface names it",
					tree, cmd.Qualified()),
			})
		}
	}
	return sortFindings(findings)
}

// CheckInventorySurfaces is CheckInventoryCommands for everything that is not a
// command: the public Go packages, the runnable programs, and every value of
// every enumerated format set.
func CheckInventorySurfaces(surfaces *Surfaces, inventory *Inventory) []Finding {
	packages := make(map[string]bool)
	programs := make(map[string]bool)
	for _, pkg := range surfaces.Packages {
		packages[pkg] = false
	}
	for _, program := range surfaces.Programs {
		if program.Installable() {
			programs[program.Dir] = false
		}
	}

	findings := markClaims(inventory, packages, programs)
	findings = append(findings, unclaimed(inventory, packages,
		"docs/public_api.md lists `%s` and no inventory row claims it")...)
	findings = append(findings, unclaimed(inventory, programs,
		"`%s` is an installable `main` package and no inventory row claims it; a contributor tool may be marked as one, but it may not be absent")...)
	findings = append(findings, unclaimedFormats(surfaces, inventory)...)
	return sortFindings(findings)
}

// markClaims records which discovered surfaces the inventory claims, and reports
// a claim that names none of them.
func markClaims(inventory *Inventory, packages, programs map[string]bool) []Finding {
	var findings []Finding
	for _, row := range inventory.Rows {
		for _, token := range row.Surface {
			claimed, message := claimTarget(token, packages, programs)
			if claimed == nil {
				continue
			}
			if _, known := claimed[token.Value]; !known {
				findings = append(findings, Finding{
					File: inventory.Path, Line: row.Line,
					Message: fmt.Sprintf(message, row.ID, token.Value),
				})
				continue
			}
			claimed[token.Value] = true
		}
	}
	return findings
}

// claimTarget picks the discovery set one token claims from, and the diagnostic
// for a token that names nothing in it.
func claimTarget(token Token, packages, programs map[string]bool) (map[string]bool, string) {
	switch token.Kind {
	case KindPackage:
		return packages, "row %q claims the package `%s`, which docs/public_api.md does not list"
	case KindProgram:
		return programs, "row %q claims the program `%s`, which is not an installable `main` package under cmd/"
	case KindCommand, KindFormat, KindValue:
		return nil, ""
	}
	return nil, ""
}

// unclaimed reports every discovered surface no row named.
func unclaimed(inventory *Inventory, discovered map[string]bool, message string) []Finding {
	var findings []Finding
	for name, claimed := range discovered {
		if claimed {
			continue
		}
		findings = append(findings, Finding{
			File:    inventory.Path,
			Message: fmt.Sprintf(message, name),
		})
	}
	return findings
}

// unclaimedFormats reports every value the code accepts that no row names.
//
// One direction only, and the reason is in the register rather than here: its
// rows record values that no single declaration carries. `atlas-hcl` is an
// accepted export target deliberately left out of the advertised slice, `svg` is
// a viz format that renders `dot` through Graphviz from a case in another
// package, and one row's value is the empty string because that is the default
// of `migrations generate --report`. A check that also refused a value it could
// not find in code would be refusing the document for being more complete than
// the list it is compared against.
func unclaimedFormats(surfaces *Surfaces, inventory *Inventory) []Finding {
	claimed := make(map[string]bool)
	for _, row := range inventory.Rows {
		for _, token := range row.Surface {
			if token.Kind == KindFormat {
				claimed[token.List+"/"+token.Value] = true
			}
		}
	}
	var findings []Finding
	for _, list := range surfaces.Formats {
		for _, value := range list.Values {
			if claimed[list.Name+"/"+value] {
				continue
			}
			findings = append(findings, Finding{
				File: inventory.Path,
				Message: fmt.Sprintf("`format:%s/%s` is a value the code accepts (%s) and no inventory row names it",
					list.Name, value, list.Source),
			})
		}
	}
	return findings
}

// Exemption is one documented invocation that names a command on purpose
// because the command does not exist.
//
// The list is small and it polices itself: an exemption whose path the tree
// DOES register is reported as a finding of its own. An allowlist that could
// quietly cover a real command is the failure this shape exists to avoid.
type Exemption struct {
	File string
	Tree Tree
	// Word is the first word the tree refuses.
	Word string
	// Reason says why the document is right and the tree is not missing
	// anything.
	Reason string
}

// docCommandExemptions are the deliberate negative examples in the tracked
// documents.
//
// There is one, and the structure of the scan is why there are not twenty. A
// scan of prose finds a hundred sentences that name a command in order to say
// it does not exist -- AGENTS.md says outright that there is no `ptah generate`
// -- and every one of them would need an entry here. Reading only fenced blocks
// tagged as shell leaves the transcripts, and a transcript of a refusal is a
// legitimate thing for a compatibility page to print.
var docCommandExemptions = []Exemption{
	{
		File: "docs/conformance.md",
		Tree: TreeCompat,
		Word: "cloud",
		Reason: "the page prints the refusal itself: `ptah-compat cloud` answers `unknown command \"cloud\" for \"atlas\"` at exit 1, " +
			"byte for byte with the pinned community binary, and the transcript is the evidence for that claim",
	},
}

// CheckDocCommandReferences reports documented invocations naming a command the
// tree does not have.
func CheckDocCommandReferences(census *Census, references []Reference, exemptions []Exemption) []Finding {
	var findings []Finding

	for _, exemption := range exemptions {
		if _, exists := census.Lookup(exemption.Tree, exemption.Word); exists {
			findings = append(findings, Finding{
				File: exemption.File,
				Message: fmt.Sprintf("this file is exempted for naming `%s %s`, and the %s tree now registers it; remove the exemption rather than keeping a hole where a real command is",
					exemption.Tree.Launcher(), exemption.Word, exemption.Tree),
			})
		}
	}

	for _, reference := range references {
		if reference.Launcher.Tree == "" {
			continue
		}
		cmd, consumed, refused := census.Resolve(reference.Launcher.Tree, reference.Words)
		if !refused {
			continue
		}
		word := reference.Words[consumed]
		if slices.ContainsFunc(exemptions, func(e Exemption) bool {
			return e.File == reference.File && e.Tree == reference.Launcher.Tree && e.Word == word
		}) {
			continue
		}
		parent := reference.Launcher.Tree.Launcher()
		if cmd.Path != "" {
			parent += " " + cmd.Path
		}
		findings = append(findings, Finding{
			File: reference.File, Line: reference.Line,
			Message: fmt.Sprintf("`%s` names no command of `%s`; the %s in this %s is stale\n      %s",
				word, parent, reference.Launcher.Prefix, reference.Source, reference.Text),
		})
	}
	return sortFindings(findings)
}

// CheckDocFlagReferences reports a documented invocation carrying a flag its own
// command does not register.
//
// Scoped to the command, never to the whole tree. Over the tracked documents,
// 247 `--flag` mentions name a flag no tree registers and every one of them is
// correct and about another program: `--profile` and `--rm` belong to docker,
// `--scenarios` to the integration runner, `--selftest` to the check scripts
// themselves. docs/site/scripts/check-matrix-flag-names.mjs states the same
// limit from the other side -- it compares against the whole tree, so a
// plausible wrong name that is a real flag elsewhere passes it
// (stokaro/ptah#1924). Scoping is what removes that, and it was measured to cost
// nothing: 532 documented invocations resolve to a command, and none of them
// carries a flag its own command lacks.
//
// A hidden flag counts as existing. It is reachable, it parses, and a document
// that names one is right; what a hidden flag is exempt from is the opposite
// direction, being required to appear in documentation at all.
func CheckDocFlagReferences(census *Census, references []Reference) []Finding {
	var findings []Finding
	for _, reference := range references {
		if reference.Launcher.Tree == "" || reference.Source != "fenced code block" {
			continue
		}
		cmd, _, refused := census.Resolve(reference.Launcher.Tree, reference.Words)
		if refused {
			// The command reference gate owns this line. Reporting its flags
			// too would attribute a second defect to one mistake.
			continue
		}
		for _, word := range reference.Words {
			// Everything after the end-of-flags marker is a positional
			// argument, whatever it looks like. `ptah schema render --
			// --not-a-flag` passes a literal string, and reporting it would be
			// reporting the shell's grammar as the document's mistake.
			if word == "--" {
				break
			}
			name, ok := FlagNameOf(word)
			if !ok {
				continue
			}
			if _, registered := cmd.Flags[name]; registered {
				continue
			}
			findings = append(findings, Finding{
				File: reference.File, Line: reference.Line,
				Message: fmt.Sprintf("`--%s` is not a flag of `%s`\n      %s", name, cmd.Qualified(), reference.Text),
			})
		}
	}
	return sortFindings(findings)
}

// CheckCommandReference compares the generated block in a document with the one
// the trees produce.
func CheckCommandReference(census *Census, body string) []Finding {
	generated := census.ReferenceBlock()
	if strings.TrimSpace(generated) == "" {
		return []Finding{{
			File:    CommandReferencePath,
			Message: "the command trees rendered no reference block at all; refusing to compare a document against an empty table",
		}}
	}
	extracted, err := ExtractBlock(body)
	if err != nil {
		return []Finding{{File: CommandReferencePath, Message: err.Error()}}
	}
	if extracted == generated {
		return nil
	}

	var findings []Finding
	have := make(map[string]bool)
	for line := range strings.SplitSeq(strings.TrimRight(extracted, "\n"), "\n") {
		have[line] = true
	}
	want := make(map[string]bool)
	for line := range strings.SplitSeq(strings.TrimRight(generated, "\n"), "\n") {
		want[line] = true
		if !have[line] {
			findings = append(findings, Finding{
				File:    CommandReferencePath,
				Message: fmt.Sprintf("the generated command reference is missing a row the trees produce: %s", line),
			})
		}
	}
	for line := range have {
		if !want[line] {
			findings = append(findings, Finding{
				File:    CommandReferencePath,
				Message: fmt.Sprintf("the generated command reference carries a row no tree produces: %s", line),
			})
		}
	}
	if len(findings) == 0 {
		findings = append(findings, Finding{
			File:    CommandReferencePath,
			Message: "the generated command reference differs from the trees in row order only",
		})
	}
	return sortFindings(findings)
}

// DocCommandExemptions returns the deliberate negative examples.
func DocCommandExemptions() []Exemption { return slices.Clone(docCommandExemptions) }
