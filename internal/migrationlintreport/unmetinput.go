package migrationlintreport

import (
	"fmt"
	"io"
	"sort"
	"strings"

	"ptah.run/migration/lint"
)

// WriteUnmetInputNotice reports, on w, every rule that asked for an analyzer
// input the run did not supply, and every statement an engine owner declined
// to judge on a run that names no dialect ([lint.Analysis.UnjudgedStatements]).
//
// It writes to STDERR on both surfaces, and that placement is the whole design.
// A rule that needs the replayed dev schema and does not get it resolves
// nothing and reports less, while the run exits 0 and prints a clean report --
// the hardest kind of gap to notice from CI (stokaro/ptah#1632). Saying so on
// stdout would change bytes a compat consumer parses and a native `--format
// json` consumer decodes; saying it on stderr leaves both alone and still puts
// the fact in front of whoever reads the log.
//
// It writes nothing when every rule got what it asked for, which is every run
// with a dev database it could read and every run with nothing to resolve, and
// every owner judged every statement.
func WriteUnmetInputNotice(w io.Writer, report Report) error {
	if err := writeUnmetInputs(w, report.Analysis.UnmetInputs()); err != nil {
		return err
	}
	return writeUnjudged(w, report.Analysis.UnjudgedStatements())
}

// writeUnmetInputs writes one line per input some rule went without.
func writeUnmetInputs(w io.Writer, unmet []lint.UnmetInput) error {
	if len(unmet) == 0 {
		return nil
	}
	byInput := make(map[string]map[string]struct{})
	for _, entry := range unmet {
		input := entry.Input.String()
		if byInput[input] == nil {
			byInput[input] = make(map[string]struct{})
		}
		byInput[input][entry.Rule] = struct{}{}
	}
	inputs := make([]string, 0, len(byInput))
	for input := range byInput {
		inputs = append(inputs, input)
	}
	sort.Strings(inputs)
	for _, input := range inputs {
		rules := make([]string, 0, len(byInput[input]))
		for rule := range byInput[input] {
			rules = append(rules, rule)
		}
		sort.Strings(rules)
		if _, err := fmt.Fprintf(w,
			"warning: %s ran without the %s it reads, so this analysis is thinner than the same "+
				"directory would get against a dev database the run can read\n",
			strings.Join(rules, ", "), input,
		); err != nil {
			return err
		}
	}
	return nil
}

// writeUnjudged writes one line per owner that declined a statement. A run
// that names no dialect reads every file with a scanner that knows none, and
// asks each owner for what only the owner recognizes. An owner whose own
// lexing splits a statement's text differently declines it, so the rules that
// read those facts did not check it, and the clean report says nothing about
// it.
func writeUnjudged(w io.Writer, unjudged []lint.UnjudgedStatement) error {
	byTarget := make(map[string][]string)
	var targets []string
	for _, statement := range unjudged {
		if _, seen := byTarget[statement.Target]; !seen {
			targets = append(targets, statement.Target)
		}
		byTarget[statement.Target] = append(byTarget[statement.Target], fmt.Sprintf("%s:%d", statement.File, statement.Line))
	}
	sort.Strings(targets)
	for _, target := range targets {
		sites := byTarget[target]
		pronoun := "it"
		if len(sites) > 1 {
			pronoun = "them"
		}
		if _, err := fmt.Fprintf(w,
			"warning: the %s owner did not judge %s: its own lexing splits the text differently than the "+
				"dialect-blind scanner this run used, so the rules that read what only %s recognizes did not check %s; "+
				"name the dialect to read the text one way\n",
			target, strings.Join(sites, ", "), target, pronoun,
		); err != nil {
			return err
		}
	}
	return nil
}
