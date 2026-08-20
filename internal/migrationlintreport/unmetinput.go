package migrationlintreport

import (
	"fmt"
	"io"
	"sort"
	"strings"
)

// WriteUnmetInputNotice reports, on w, every rule that asked for an analyzer
// input the run did not supply.
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
// with a dev database it could read and every run with nothing to resolve.
func WriteUnmetInputNotice(w io.Writer, report Report) error {
	unmet := report.Analysis.UnmetInputs()
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
