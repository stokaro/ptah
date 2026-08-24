package agentsurface

import (
	"fmt"
	"sort"
	"strings"
)

// Markdown renders the classification as a table, one row per verb.
//
// The measured half and the written half are in the same row on purpose: a
// reader checking a classification should not have to go and find which flags
// the verb registers, and a row whose class and flags disagree is the thing the
// guards refuse.
func Markdown(leaves []Leaf) string {
	var out strings.Builder
	out.WriteString("| verb | target | second database | flags | what it does |\n")
	out.WriteString("| --- | --- | --- | --- | --- |\n")
	for _, leaf := range leaves {
		verb, known := Lookup(leaf.Name)
		if !known {
			continue
		}
		fmt.Fprintf(&out, "| `%s` | %s | %s | %s | %s |\n",
			leaf.Name,
			targetCell(verb.Target),
			scratchCell(verb.Scratch),
			flagCell(leaf),
			verb.Reason,
		)
	}
	return out.String()
}

// DatabaseSafeMarkdown renders the verbs that cannot change or destroy a
// database, which is the shortlist an agent-exposure decision starts from.
//
// It is a shortlist and not a verdict: see [Verb.DatabaseSafe] for what it does
// not answer.
func DatabaseSafeMarkdown(leaves []Leaf) string {
	var names []string
	for _, leaf := range leaves {
		if verb, known := Lookup(leaf.Name); known && verb.DatabaseSafe() {
			names = append(names, leaf.Name)
		}
	}
	sort.Strings(names)

	var out strings.Builder
	out.WriteString("| verb | why no database is at risk |\n")
	out.WriteString("| --- | --- |\n")
	for _, name := range names {
		verb, _ := Lookup(name)
		fmt.Fprintf(&out, "| `%s` | %s |\n", name, verb.Reason)
	}
	return out.String()
}

func targetCell(target Target) string {
	switch target {
	case TargetNone:
		return "none"
	case TargetReads:
		return "reads"
	case TargetWrites:
		return "**writes**"
	default:
		return string(target)
	}
}

func scratchCell(scratch Scratch) string {
	switch scratch {
	case ScratchNone:
		return "none"
	case ScratchProbes:
		return "probes"
	case ScratchRewrites:
		return "**rewrites**"
	default:
		return string(scratch)
	}
}

// flagCell names the database flags the command registers, which is the part a
// reader can check against `--help` in one step.
func flagCell(leaf Leaf) string {
	flags := make([]string, 0, len(leaf.TargetFlags)+len(leaf.ScratchFlags))
	for _, flag := range leaf.TargetFlags {
		flags = append(flags, "`--"+flag+"`")
	}
	for _, flag := range leaf.ScratchFlags {
		flags = append(flags, "`--"+flag+"`")
	}
	if len(flags) == 0 {
		return "—"
	}
	return strings.Join(flags, ", ")
}
