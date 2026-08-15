package lintcatalog

import (
	"fmt"
	"io"
	"slices"
	"strings"
)

// WriteMarkdown renders the whole enumeration: the families, both rule tables,
// what the compatibility surface prints where it differs, every Atlas analyzer
// check with Ptah's status on it, and the identifiers that predate the naming
// convention.
//
// It validates before it renders. A generator that emitted a table it could not
// stand behind would turn a broken catalog into documentation instead of into a
// failure.
func WriteMarkdown(w io.Writer) error {
	entries, err := Entries()
	if err != nil {
		return err
	}
	if err := Validate(entries); err != nil {
		return err
	}

	sorted := slices.Clone(entries)
	slices.SortStableFunc(sorted, func(a, b Entry) int { return strings.Compare(a.Code, b.Code) })

	sections := []func(io.Writer, []Entry) error{
		writeFamilies,
		writeMigrationRules,
		writeSQLRules,
		writeSeverities,
		writeCompatIdentities,
		writeAtlasChecks,
		writePreConvention,
	}
	for _, section := range sections {
		if err := section(w, sorted); err != nil {
			return err
		}
	}
	return nil
}

func writeFamilies(w io.Writer, _ []Entry) error {
	sorted := Families()
	slices.SortStableFunc(sorted, func(a, b Family) int { return strings.Compare(a.Prefix, b.Prefix) })

	var out strings.Builder
	out.WriteString("## Identifier families\n\n")
	out.WriteString("An identifier's prefix says whose namespace it lives in. Atlas owns a prefix when the Atlas analyzer documentation uses it; the rest are Ptah's.\n\n")
	out.WriteString("| Prefix | Namespace | What the family covers |\n| --- | --- | --- |\n")
	for _, family := range sorted {
		fmt.Fprintf(&out, "| `%s` | %s | %s |\n", family.Prefix, family.Origin, family.Summary)
	}
	out.WriteString("\n")
	_, err := io.WriteString(w, out.String())
	return err
}

func writeMigrationRules(w io.Writer, entries []Entry) error {
	rules := entriesOfKind(entries, KindMigration)

	var out strings.Builder
	out.WriteString("## Migration lint rules\n\n")
	fmt.Fprintf(&out,
		"%d rules, registered in `migration/lint`. Native `ptah migrations lint` and "+
			"`ptah migrations up` read this registry, and so do `ptah-compat migrate lint` and "+
			"`ptah-compat schema apply`. They are grouped by the dialects each rule applies to, "+
			"which is why the tables carry no dialect column.\n\n",
		len(rules))
	for _, group := range groupByDialects(rules) {
		fmt.Fprintf(&out, "### %s\n\n", group.heading)
		out.WriteString(ruleTable(group.rules))
	}
	_, err := io.WriteString(w, out.String())
	return err
}

// dialectSortKey puts the unrestricted group first and orders the rest by
// label. The prefix rather than a special case in the comparator keeps the
// ordering a total one, which slices.SortStableFunc requires.
func dialectSortKey(label string) string {
	if label == everyDialect {
		return "0"
	}
	return "1" + label
}

// dialectGroup is one dialect restriction and the rules that carry it.
type dialectGroup struct {
	heading string
	rules   []Entry
}

// groupByDialects splits rules by their dialect restriction, unrestricted rules
// first and the rest in label order. The heading is the restriction itself
// rather than a prettier name looked up in a table, so a dialect added to a
// rule cannot land in a group that has no heading.
func groupByDialects(rules []Entry) []dialectGroup {
	order := []string{}
	byLabel := map[string][]Entry{}
	for _, rule := range rules {
		label := rule.DialectsLabel()
		if _, seen := byLabel[label]; !seen {
			order = append(order, label)
		}
		byLabel[label] = append(byLabel[label], rule)
	}
	slices.SortStableFunc(order, func(a, b string) int {
		return strings.Compare(dialectSortKey(a), dialectSortKey(b))
	})
	groups := make([]dialectGroup, 0, len(order))
	for _, label := range order {
		heading := label
		if label == everyDialect {
			heading = "Every dialect"
		}
		groups = append(groups, dialectGroup{heading: heading, rules: byLabel[label]})
	}
	return groups
}

func writeSQLRules(w io.Writer, entries []Entry) error {
	rules := entriesOfKind(entries, KindSQL)

	var out strings.Builder
	out.WriteString("## SQL lint rules\n\n")
	fmt.Fprintf(&out,
		"%d rules, reported by `ptah sql lint` over standalone SQL files, on every dialect. "+
			"The compatibility surface has no verb that reaches them.\n\n",
		len(rules))
	out.WriteString(ruleTable(rules))
	_, err := io.WriteString(w, out.String())
	return err
}

// ruleTable renders one group of rules.
//
// Four columns, not six. The documentation site refuses a table wider than its
// reading column, and a six-column table of short unbreakable tokens overflowed
// it by 57px: the dialect restriction moved into the group heading and the
// default severity into [writeSeverities], both still generated.
func ruleTable(rules []Entry) string {
	var out strings.Builder
	out.WriteString("| Rule | Meaning | Surface | Origin |\n| --- | --- | --- | --- |\n")
	for _, rule := range rules {
		fmt.Fprintf(&out, "| `%s` | %s | %s | %s |\n",
			rule.Code, rule.Summary, rule.SurfaceLabel(), rule.Origin())
	}
	out.WriteString("\n")
	return out.String()
}

// writeSeverities states the default severities as a sentence, generated from
// the same entries, because the column that carried them did not fit.
func writeSeverities(w io.Writer, entries []Entry) error {
	var errors []string
	warnings := 0
	for _, entry := range entries {
		if entry.Severity == severityError {
			errors = append(errors, "`"+entry.Code+"`")
			continue
		}
		warnings++
	}

	var out strings.Builder
	out.WriteString("## Default severities\n\n")
	fmt.Fprintf(&out,
		"%d rules report at error severity by default: %s. The other %d default to warning. "+
			"A committed `.ptah-lint.yaml` replaces either, per rule or per family.\n\n",
		len(errors), strings.Join(errors, ", "), warnings)
	_, err := io.WriteString(w, out.String())
	return err
}

// writeCompatIdentities lists the rules the compatibility surface prints under
// another name. A reader holding a `ptah-compat` diagnostic needs the row in
// this direction to find the rule in the table above.
func writeCompatIdentities(w io.Writer, entries []Entry) error {
	var out strings.Builder
	out.WriteString("## What ptah-compat prints\n\n")
	out.WriteString("Every migration lint finding reports under an analyzer name and a code on the compatibility surface. Rules not listed here keep their own code under the `ptah` analyzer.\n\n")
	out.WriteString("| Native rule | Analyzer | Code |\n| --- | --- | --- |\n")
	rows := 0
	for _, rule := range entriesOfKind(entries, KindMigration) {
		if !rule.Compat || rule.CompatCode == rule.Code {
			continue
		}
		rows++
		fmt.Fprintf(&out, "| `%s` | `%s` | `%s` |\n", rule.Code, rule.CompatAnalyzer, rule.CompatCode)
	}
	if rows == 0 {
		out.WriteString("| none | | |\n")
	}
	out.WriteString("\n")
	_, err := io.WriteString(w, out.String())
	return err
}

func writeAtlasChecks(w io.Writer, _ []Entry) error {
	checks := AtlasChecks()
	counts := AtlasCounts()

	var out strings.Builder
	out.WriteString("## Atlas analyzer checks\n\n")
	fmt.Fprintf(&out,
		"Every check code the [Atlas analyzer documentation](https://atlasgo.io/lint/analyzers) carries, "+
			"and what Ptah does about it: %d covered, %d partial, %d not implemented, %d waived, of %d. "+
			"A code Atlas marks as an Atlas Pro feature is marked here too, and the ones Ptah implements "+
			"are reported through both surfaces.\n\n",
		counts[StatusCovered], counts[StatusPartial], counts[StatusAbsent], counts[StatusWaived], len(checks))
	out.WriteString("| Atlas check | Meaning | Pro | Ptah rule | Status |\n| --- | --- | --- | --- | --- |\n")
	for _, check := range checks {
		status := string(check.Status)
		if check.Note != "" {
			status += " — " + check.Note
		}
		fmt.Fprintf(&out, "| `%s` | %s | %s | %s | %s |\n",
			check.Code, check.Meaning, proLabel(check), ruleList(check.PtahRules), status)
	}
	out.WriteString("\n")
	_, err := io.WriteString(w, out.String())
	return err
}

func writePreConvention(w io.Writer, entries []Entry) error {
	nonConforming := NonConforming(entries)

	var out strings.Builder
	out.WriteString("## Identifiers that predate the convention\n\n")
	fmt.Fprintf(&out,
		"%d identifiers were chosen before the convention above existed. Renaming one changes what "+
			"`ptah-compat` prints, what a `.ptah-lint.yaml` selector matches, and what a SARIF consumer "+
			"keys on, so they are recorded rather than rewritten. The list is pinned in "+
			"`internal/lintcatalog`: a rule added from now on that does not follow the convention fails "+
			"the check instead of joining it.\n\n",
		len(nonConforming))
	out.WriteString("| Rule | Why it does not follow the convention |\n| --- | --- |\n")
	for _, entry := range nonConforming {
		fmt.Fprintf(&out, "| `%s` | %s |\n", entry.Code, entry.ConventionNote())
	}
	out.WriteString("\n")
	_, err := io.WriteString(w, out.String())
	return err
}

// proLabel renders whether the Atlas documentation marks a check as an Atlas
// Pro feature. It takes the check rather than the flag so the column can never
// be filled in from a boolean that belongs to another row.
func proLabel(check AtlasCheck) string {
	if check.Pro {
		return "yes"
	}
	return "no"
}

func ruleList(codes []string) string {
	if len(codes) == 0 {
		return "—"
	}
	quoted := make([]string, 0, len(codes))
	for _, code := range codes {
		quoted = append(quoted, "`"+code+"`")
	}
	return strings.Join(quoted, ", ")
}

func entriesOfKind(entries []Entry, kind Kind) []Entry {
	var out []Entry
	for _, entry := range entries {
		if entry.Kind == kind {
			out = append(out, entry)
		}
	}
	return out
}
