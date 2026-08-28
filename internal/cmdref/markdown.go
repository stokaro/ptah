package cmdref

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"go.5x5.cz/ptah/internal/agentsurface"
)

// Surface is one binary's command tree with the name it ships under.
type Surface struct {
	// Program is the name the binary calls itself, taken from the root
	// command rather than written down: `ptah` and `ptah-compat`.
	Program string
	Nodes   []agentsurface.Node
	// Classified says that every LEAF of this tree carries a classification in
	// internal/agentsurface, so its purpose cell comes from there rather than
	// from cobra's Short. Only the native tree does; see [Commands].
	Classified bool
}

// Commands renders one surface's every command path as a table.
//
// Groups are rows too. `ptah schema` is a path a user types, cobra runs it,
// and it has a description of its own; a table of leaves would leave a reader
// looking up a spelling the binary answers to and finding nothing.
//
// # Leaves are classified, groups describe themselves
//
// On a Classified surface a LEAF's purpose comes from internal/agentsurface's
// classification and a GROUP's from cobra's Short, and neither is a fallback
// for the other. A group's Short IS its description -- "Manage migration
// plans, files, and revision state" -- while a Reason for a group would be a
// sentence about a help printer, which is why the classification excludes
// groups in the first place. The Notes column carries `group`, so a reader can
// see which of the two a cell came from without being told.
//
// Measured on the native tree: 103 paths, 92 leaves and 11 groups; 92
// classified verbs, none with an empty Reason. The eleven without one are
// exactly the eleven groups, so there is no partial coverage to police, and
// TestClassification_NamesEveryVerbTheBinaryHas guards that bidirectionally --
// a new leaf without a Reason fails the build before this renderer sees it.
// The refusal below is the second line of that defense rather than the first.
//
// Reason is the right cell because Short cannot be made to carry a reference
// sentence: Short is printed in the parent's `Available Commands` listing,
// where a long string wraps on an 80-column terminal. Measured against the
// hand-written table this block replaced, Short averages 46 characters and the
// page's purpose averaged 106; Reason averages 99.
//
// The compatibility tree has no classification and stays on Short for every
// row. That asymmetry is deliberate rather than an oversight: its page carries
// 38 per-verb prose sections holding the depth the native page has to fit in a
// cell.
func Commands(surface Surface) (string, error) {
	if len(surface.Nodes) == 0 {
		return "", errEmpty("the command tree of " + surface.Program)
	}
	var out strings.Builder
	out.WriteString("| Command | What it does | Notes |\n| --- | --- | --- |\n")
	for _, node := range surface.Nodes {
		purpose, err := purposeOf(surface, node)
		if err != nil {
			return "", err
		}
		fmt.Fprintf(&out, "| `%s %s` | %s | %s |\n",
			surface.Program, node.Name, purpose, notes(commandNotes(node)))
	}
	return out.String(), nil
}

// purposeOf answers the rule above for one row.
//
// The capital is this renderer's, not the registry's. A classification reads
// as a predicate of the verb it belongs to -- "introspects the database and
// prints what it found" -- and a reference table's cell is a sentence of its
// own; a column mixing 92 predicates with 11 sentences reads as a mistake.
// Nothing else about the string is touched.
func purposeOf(surface Surface, node agentsurface.Node) (string, error) {
	if !surface.Classified || !node.Leaf {
		return cell(node.Summary), nil
	}
	verb, classified := agentsurface.Lookup(node.Name)
	if !classified {
		return "", fmt.Errorf(
			"cmdref: %s %s is a leaf with no classification; refusing to render a reference "+
				"whose rows come from two depths without saying which", surface.Program, node.Name)
	}
	return capitalized(escape(verb.Reason)), nil
}

// capitalized raises the first rune, leaving the rest of the string alone.
func capitalized(sentence string) string {
	first, width := utf8.DecodeRuneInString(sentence)
	if width == 0 {
		return sentence
	}
	return string(unicode.ToUpper(first)) + sentence[width:]
}

// Flags renders every flag every command of every surface registers, one row
// per command and flag. Rows are grouped below their exact command path rather
// than repeating that path in a sixth column. Besides fitting the documentation
// column, the grouping makes one command's accepted inputs readable as a unit.
//
// One row per pair is the only shape that fits, and the limit is mechanical:
// docs/site/scripts/check-style.mjs refuses a table cell over 350 characters.
// Measured on this tree, a cell per command listing its flags puts four of the
// 103 native rows over that, `migrations up` at 555; inverted, a cell per flag
// listing its commands puts six over, `--db-url` at 695 across 38 commands.
//
// # There is no column carrying a flag's own words, and the measurement is why
//
// agentsurface.Flag.Usage holds the sentence `--help` prints, and rendering it
// here would put the meaning of every flag on one page. The same 350-character
// limit refuses it: 6 of the 937 usage strings are over, all of them
// `--verify-sum`, from 409 characters on `migrations up` to 512 on
// `migrations show`. Truncating a measured value would make the reference lie
// about text the binary prints in full, and there is no third rendering. A
// further 46 carry a bare `--flag`, which the same checker refuses because it
// renders as an en dash, so the column would also need the renderer to rewrite
// measured text on every row.
//
// Shortening those six registrations is a change to `--help` output and
// belongs in its own commit; the column becomes a five-line addition the day
// it lands. Until then `ptah <command> --help` is where a flag's own words
// are, and the page says so rather than pointing at a verb page for them.
func Flags(surfaces []Surface) (string, error) {
	var out strings.Builder
	for _, surface := range surfaces {
		if len(surface.Nodes) == 0 {
			return "", errEmpty("the command tree of " + surface.Program)
		}
		fmt.Fprintf(&out, "## `%s`\n\n", surface.Program)
		rows := 0
		for _, node := range surface.Nodes {
			if len(node.Flags) == 0 {
				continue
			}
			fmt.Fprintf(&out, "**`%s %s`**\n\n", surface.Program, node.Name)
			out.WriteString("| Flag | Type | Default | Environment variable | Notes |\n")
			out.WriteString("| --- | --- | --- | --- | --- |\n")
			for _, flag := range node.Flags {
				fmt.Fprintf(&out, "| `--%s`%s | `%s` | %s | %s | %s |\n",
					flag.Name, shorthand(flag),
					flag.Type, code(flag.Default), code(flag.Environment),
					notes(flagNotes(flag)))
				rows++
			}
			out.WriteString("\n")
		}
		if rows == 0 {
			return "", errEmpty("the flag set of " + surface.Program)
		}
	}
	return strings.TrimRight(out.String(), "\n") + "\n", nil
}

// FlagsPage renders the flag reference as a whole page rather than a block,
// because the page is entirely measurement: there is no sentence on it a person
// would write. Everything a reader needs beyond the rows -- when to reach for a
// verb, what a flag means -- is on the two command pages, which stay
// hand-written around their generated tables.
//
// The preamble is here rather than in the generator binary so that what it
// CLAIMS is testable. It is prose about the rows, and prose about measured rows
// goes stale the way any hand-written sentence does: its approval paragraph
// said no `--auto-approve` anywhere reads a variable, which was false for the
// `ptah-compat schema plan` verbs on the same page.
// TestFlagsPage_NoNativeApprovalFlagReadsAVariable and
// TestFlagsPage_TheCompatibilityCounterexampleIsReal measure both halves of the
// replacement, and they can only do that from a package a test may import.
//
// The paragraph names the compatibility COMMAND and lets its row name the
// variable, which is not squeamishness about repetition. A `PTAH_*` name in a
// string literal is a name cmd/internal/envboolguard requires to be classified,
// and the two classifications it offers are "declared through
// internal/envbool" and "carries something other than a boolean".
// `PTAH_AUTO_APPROVE` is neither: it is a boolean that reaches its flag through
// cmdflags, which parses it with envbool.Parse without any package declaring
// it. An entry in either list would be a false statement made to quiet a guard,
// and the row below the paragraph already owns the name.
func FlagsPage(surfaces []Surface) (string, error) {
	tables, err := Flags(surfaces)
	if err != nil {
		return "", err
	}
	return flagsPreamble + tables, nil
}

// flagsPreamble is written with an explicit newline join rather than a raw
// string literal, because half its lines carry code spans and a backtick ends a
// raw literal.
var flagsPreamble = strings.Join([]string{
	"---",
	"title: Command flags",
	"description: Every flag both Ptah binaries register, with its type, default and environment variable.",
	"---",
	"",
	"<!-- Generated by internal/cmd/cmdref. Do not edit by hand: run `scripts/check-command-reference.sh --write`. -->",
	"",
	"This page is measured from the command trees the two binaries ship, one row",
	"per command and flag. It is the inventory: which spellings a command accepts,",
	"what each holds when nothing sets it, and which variable it reads. For a",
	"flag's own words run `ptah <command> --help`, which prints the same usage",
	"string this page indexes by name. What each VERB is for is on",
	"[Native commands](../native-commands/) and",
	"[Atlas-compatible commands](../atlas-commands/).",
	"",
	"Each flag table sits under the exact command path that accepts it. The",
	"command path is outside the table so the five measured flag fields fit the",
	"documentation column without hiding the right-hand fields.",
	"",
	"Read the columns as follows.",
	"",
	"- **Default** is the value the flag holds when nothing sets one, in the",
	"  spelling the flag parser reports.",
	"- **Environment variable** is what the flag reads when it is not typed on the",
	"  command line, and an em dash means it reads none. Approval is where the",
	"  column earns its place: no `--auto-approve` and no",
	"  `--allow-database-inspect` the native binary registers reads a variable, so",
	"  nothing a script exports can approve a `ptah` run. That is a rule about the",
	"  native surface rather than about the spelling — the compatibility",
	"  surface's `ptah-compat schema plan` verbs do bind one — so read the row",
	"  rather than the flag name.",
	"- **Notes** carries `hidden` for a flag `--help` does not list and still",
	"  parses, and `inherited by subcommands` for one declared on a group.",
	"",
	"",
}, "\n")

// StrictCompat renders what PTAH_ATLAS_STRICT_COMPAT=1 takes away.
//
// Only the paths it removes have rows. What remains is on the command table
// already, and a second listing of it would be a copy that could disagree.
func StrictCompat(program string, paths []Path) (string, error) {
	removed := Removed(paths)
	if len(removed) == 0 {
		return "", errEmpty("the set of paths strict compatibility mode removes")
	}
	var out strings.Builder
	out.WriteString("| Command | Under `PTAH_ATLAS_STRICT_COMPAT=1` | Exit | Stream | The answer names |\n")
	out.WriteString("| --- | --- | --- | --- | --- |\n")
	for _, path := range removed {
		fmt.Fprintf(&out, "| `%s %s` | %s | `%d` | %s | `%s %s` |\n",
			program, path.Name, path.Availability, path.Availability.Exit(),
			path.Availability.Stream(), program, path.Answers)
	}
	return out.String(), nil
}

// commandNotes says what a reader cannot see from the path alone.
func commandNotes(node agentsurface.Node) []string {
	var found []string
	if !node.Leaf {
		found = append(found, "group")
	}
	if node.Hidden {
		found = append(found, "hidden")
	}
	return found
}

// flagNotes says the same for a flag. "inherited" is the fact that decides
// whether a spelling works on a subcommand, and nothing else in the row shows
// it.
func flagNotes(flag agentsurface.Flag) []string {
	var found []string
	if flag.Persistent {
		found = append(found, "inherited by subcommands")
	}
	if flag.Hidden {
		found = append(found, "hidden")
	}
	return found
}

func notes(found []string) string {
	if len(found) == 0 {
		return "—"
	}
	return strings.Join(found, ", ")
}

// code wraps a measured value in a span, and answers an em dash for an empty
// one. An empty default and an empty variable name are different facts from a
// value that happens to look empty, and a bare pair of backticks renders as
// nothing at all.
func code(value string) string {
	if value == "" {
		return "—"
	}
	return "`" + escape(value) + "`"
}

func cell(value string) string {
	if value == "" {
		return "—"
	}
	return escape(value)
}

func shorthand(flag agentsurface.Flag) string {
	if flag.Shorthand == "" {
		return ""
	}
	return ", `-" + flag.Shorthand + "`"
}

// escape protects the one character a Markdown table row cannot carry. A cell
// holding an unescaped pipe silently splits into two, and check-style.mjs
// reports the row as having more cells than the header — which is how the
// defect is noticed, one release after it shipped.
func escape(value string) string {
	return strings.ReplaceAll(value, "|", `\|`)
}
