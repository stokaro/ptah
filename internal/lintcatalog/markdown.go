package lintcatalog

import (
	"fmt"
	"io"
	"slices"
	"strings"

	"ptah.run/core/lintext"
	"ptah.run/internal/atlascodes"
	"ptah.run/internal/lintnames"
	"ptah.run/internal/migrationlintgate"
	"ptah.run/internal/sqllint"
)

// WriteMarkdown renders the whole enumeration: the rule-name categories, the
// families, both rule tables with each rule's code and Ptah name,
// what the compatibility surface prints where it differs, every Atlas analyzer
// check with Ptah's status on it, and the identifiers that predate the naming
// convention.
//
// It validates before it renders. A generator that emitted a table it could not
// stand behind would turn a broken catalog into documentation instead of into a
// failure.
func WriteMarkdown(w io.Writer) error {
	runtime, err := documented()
	if err != nil {
		return err
	}
	return WriteMarkdownFrom(w, runtime)
}

// WriteMarkdownFrom is [WriteMarkdown] with runtime as the source of the rules
// engine owners publish; see [MigrationEntriesFrom].
func WriteMarkdownFrom(w io.Writer, runtime lintext.Runtime) error {
	entries, err := EntriesFrom(runtime)
	if err != nil {
		return err
	}
	if err := Validate(entries); err != nil {
		return err
	}

	sorted := slices.Clone(entries)
	slices.SortStableFunc(sorted, func(a, b Entry) int { return strings.Compare(a.Name, b.Name) })

	sections := []func(io.Writer, []Entry) error{
		writeCategories,
		writeMigrationRules,
		writeYDBVerdicts,
		writeSQLRules,
		writeSeverities,
		writeAtlasIdentities,
		writeAtlasChecks,
	}
	for _, section := range sections {
		if err := section(w, sorted); err != nil {
			return err
		}
	}
	return nil
}

// writeCategories lists the categories a rule name begins with, each with the
// number of rules named under it, since a category is a selector and the
// count is how much it selects.
func writeCategories(w io.Writer, entries []Entry) error {
	var out strings.Builder
	out.WriteString("## Rule names and categories\n\n")
	out.WriteString("Every rule has a Ptah name, `category/name`, and no numeric code. A selector in `--disable`, " +
		"`disabled-rules`, a `rules:` key, the `gate` section or a `ptah:nolint` comment is a rule name, or a " +
		"category written as `locking` or `locking/`, which selects every rule named under it. A name or a " +
		"category matches exactly, never as a prefix, and a name or category no rule has is refused, as is a " +
		"word written as a code, such as `DS102`. One name may cover a hazard checked on several engines, each " +
		"by its own owner, or read by both linters.\n\n")
	out.WriteString("| Category | Rules | What it covers |\n| --- | --- | --- |\n")
	for _, category := range lintnames.Categories() {
		count := 0
		for _, entry := range entries {
			if entry.Category() == category.Name {
				count++
			}
		}
		fmt.Fprintf(&out, "| `%s` | %d | %s |\n", category.Name, count, category.Summary)
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
		"%d rules, registered in `migration/lint`. `ptah migrations lint` reports the whole "+
			"registry%s. Neither apply gate reports even that much, so a rule listed below is "+
			"not by itself a check that stands between an apply and a database: "+
			"`ptah migrations up` disables %d advisory rules (migration-file form, backward "+
			"compatibility, and the locking and rewrite rules of PostgreSQL, MySQL and MariaDB) and refuses "+
			"only on blocking findings of %s, unless the policy's `gate` section names more rules or categories, "+
			"and `ptah-compat schema apply` runs only the rules an `atlas.hcl` `lint` "+
			"block names, which means a project without such a block gets no lint pass there at "+
			"all. The tables are grouped by the dialects each rule applies to, which is why they "+
			"carry no dialect column.\n\n",
		len(rules),
		compatRegistryNote(entries),
		len(migrationlintgate.DisabledRules()),
		codeList(migrationlintgate.ReportedRules()))
	for _, group := range groupByDialects(rules) {
		fmt.Fprintf(&out, "### %s\n\n", group.heading)
		out.WriteString(ruleTable(group.rules, atlascodes.Identities()))
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
	order := make([]string, 0)
	byLabel := make(map[string][]Entry)
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

// writeYDBVerdicts says what each rule that runs on every dialect does on
// YDB, from the verdicts the catalog declares.
func writeYDBVerdicts(w io.Writer, entries []Entry) error {
	var out strings.Builder
	out.WriteString("### What the rules for every dialect do on YDB\n\n")
	out.WriteString("A YDB run reads migrations as YQL. The `YD` family above is YDB's own, and every rule with no " +
		"dialect restriction runs there too. This table says what each of those does on YDB: it applies, the statement " +
		"it reads does not exist in YQL, a `YD` rule replaces it, its subject is read by the provider rather than Ptah, " +
		"or it needs a dev database, and a run without `--dev-url` names it as unmet.\n\n")
	out.WriteString("| Rule | On YDB | What it rests on |\n| --- | --- | --- |\n")
	for _, entry := range entries {
		if entry.YDB == "" {
			continue
		}
		fmt.Fprintf(&out, "| `%s` | %s | %s |\n", entry.Name, entry.YDB, entry.YDBNote)
	}
	out.WriteString("\n")
	_, err := io.WriteString(w, out.String())
	return err
}

func writeSQLRules(w io.Writer, entries []Entry) error {
	rules := entriesOfKind(entries, KindSQL)

	var out strings.Builder
	out.WriteString("## SQL lint rules\n\n")
	fmt.Fprintf(&out,
		"%d rules, reported by `ptah sql lint` over standalone SQL files, on every dialect. "+
			"The compatibility surface has no verb that reaches them. A file of an executable provider's engine, such "+
			"as YDB, is read from the statement facts of the provider's split and can report %s only.\n\n",
		len(rules), codeList(sqllint.OwnerCatalogIDs()))
	out.WriteString(ruleTable(rules, nil))
	_, err := io.WriteString(w, out.String())
	return err
}

// ruleTable renders one group of rules.
//
// Four columns. The documentation site caps a cell at eight lines, and a rule
// name is one unbreakable token of up to 47 characters, so the dialect
// restriction lives in the group heading and the default severity in
// [writeSeverities], both still generated. The wrapper keeps a phone from
// scrolling the page, since the name sets the cell's minimum width. The Atlas
// column lists the Atlas checks the rule reports, from identities.
func ruleTable(rules []Entry, identities []atlascodes.Identity) string {
	var out strings.Builder
	out.WriteString(wideTableOpen)
	out.WriteString("| Rule | Meaning | Surface | Atlas check |\n| --- | --- | --- | --- |\n")
	for _, rule := range rules {
		fmt.Fprintf(&out, "| `%s` | %s | %s | %s |\n", rule.Name, rule.Summary, rule.SurfaceLabel(), atlasCell(rule, identities))
	}
	out.WriteString(wideTableClose)
	return out.String()
}

// atlasCell lists the Atlas check codes a rule reports, or a dash.
func atlasCell(rule Entry, identities []atlascodes.Identity) string {
	var codes []string
	for _, identity := range identities {
		if identity.Name == rule.Name && !slices.Contains(codes, identity.Code) {
			codes = append(codes, identity.Code)
		}
	}
	if len(codes) == 0 {
		return "—"
	}
	return "`" + strings.Join(codes, "`, `") + "`"
}

// writeSeverities states the default severities as a sentence, generated from
// the same entries, because the column that carried them did not fit.
func writeSeverities(w io.Writer, entries []Entry) error {
	var errors []string
	warnings := 0
	for _, entry := range entries {
		if entry.Severity == severityError {
			errors = append(errors, "`"+entry.Name+"`")
			continue
		}
		warnings++
	}

	var out strings.Builder
	out.WriteString("## Default severities\n\n")
	fmt.Fprintf(&out,
		"%d rules report at error severity by default: %s. The other %d default to warning or info. "+
			"A committed `.ptah-lint.yaml` replaces either, per rule or per category. `ptah sql lint` "+
			"reads the `rules:` severities the same file sets for %s, so the severities above are the defaults. "+
			"`--disable` refuses a selector covering `correctness/syntax-error` or `coverage/unsupported-statement`: those report that the "+
			"file could not be analyzed, and a run that analyzed nothing must not report clean.\n\n",
		len(errors), strings.Join(errors, ", "), warnings,
		codeList(namesOfKind(entries, KindSQL)))
	_, err := io.WriteString(w, out.String())
	return err
}

// writeAtlasIdentities lists the Atlas check each Ptah rule reports, from the
// Atlas compatibility table. A reader holding an Atlas code needs the row in
// this direction to find the rule in the tables above.
func writeAtlasIdentities(w io.Writer, entries []Entry) error {
	var out strings.Builder
	out.WriteString("## Atlas check codes\n\n")
	out.WriteString("Ptah rules have no numeric code. The compatibility layer maps each rule that reports an Atlas " +
		"check to that check's code, so `ptah-compat` can print Atlas's own vocabulary. A rule not listed here " +
		"has no Atlas check and keeps its name.\n\n")
	out.WriteString("| Ptah rule | Atlas check | Dialects |\n| --- | --- | --- |\n")
	rows := 0
	for _, identity := range atlascodes.Identities() {
		dialects := everyDialect
		if len(identity.Dialects) > 0 {
			dialects = strings.Join(identity.Dialects, ", ")
		}
		rows++
		fmt.Fprintf(&out, "| `%s` | `%s` | %s |\n", identity.Name, identity.Code, dialects)
	}
	if rows == 0 {
		out.WriteString("| none | | |\n")
	}
	out.WriteString("\n")
	_, err := io.WriteString(w, out.String())
	return err
}

// wideTableOpen and wideTableClose wrap a table wide enough that a phone
// viewport shrinks it. The class is styled in docs/site/src/styles/global.css,
// where the scrolling lives on the wrapper and the desktop width on the table:
// putting the width on the table alone scrolls the PAGE, because Starlight
// makes every table its own scroller.
const (
	wideTableOpen  = "<div class=\"ptah-wide-table\">\n\n"
	wideTableClose = "\n</div>\n\n"
)

func writeAtlasChecks(w io.Writer, entries []Entry) error {
	checks := atlascodes.Checks()
	counts := atlascodes.Counts()

	var out strings.Builder
	out.WriteString("## Atlas analyzer checks\n\n")
	fmt.Fprintf(&out,
		"Every check code in the reviewed snapshot of the "+
			"[Atlas analyzer documentation](https://atlasgo.io/lint/analyzers) "+
			"(`internal/atlascodes/reference.txt`, reviewed %s), and what Ptah does about it: "+
			"%d covered, %d partial, %d not implemented, %d waived, of %d. "+
			"That page presents its table as highlights rather than an inventory -- it says Atlas "+
			"\"runs and reports dozens of additional checks\" beyond them -- so a complete row here is "+
			"completeness against the snapshot, not full behavioral parity with the analyzer. "+
			"A code Atlas marks as an Atlas Pro feature is marked here too%s.\n\n",
		atlascodes.ReferenceReviewedOn,
		counts[atlascodes.StatusCovered], counts[atlascodes.StatusPartial], counts[atlascodes.StatusAbsent], counts[atlascodes.StatusWaived], len(checks),
		atlasSurfaceNote(entries, checks))
	// Five columns, and the widest carries a sentence, so at a phone width auto
	// layout shrinks the table and the status column stacks into cells fifteen
	// lines tall. The wrapper scrolls and the table keeps its desktop width
	// inside it -- the same fix the feature matrix uses, and the reason
	// check-responsive.mjs exempted this route until now: the table is emitted
	// here rather than by a docs script (stokaro/ptah#946).
	//
	// The blank lines around the table are load-bearing: without them the
	// markdown inside the div is not parsed as a table.
	out.WriteString(wideTableOpen)
	out.WriteString("| Atlas check | Meaning | Pro | Ptah rule | Status |\n| --- | --- | --- | --- | --- |\n")
	for _, check := range checks {
		status := string(check.Status)
		if check.Note != "" {
			status += " — " + check.Note
		}
		fmt.Fprintf(&out, "| `%s` | %s | %s | %s | %s |\n",
			check.Code, check.Meaning, proLabel(check), ruleList(check.PtahRules), status)
	}
	out.WriteString(wideTableClose)
	_, err := io.WriteString(w, out.String())
	return err
}

// atlasSurfaceNote says through which surfaces the Atlas checks Ptah implements
// are reported.
//
// Claiming "both surfaces" unconditionally contradicts the catalog itself: a
// check whose Ptah rule is native only is reported through one. Deriving the
// exception from the same Compat flag the Surface column
// renders is what keeps the two from disagreeing.
func atlasSurfaceNote(entries []Entry, checks []atlascodes.Check) string {
	compat := make(map[string]bool)
	for _, entry := range entries {
		compat[entry.Name] = entry.Compat
	}

	var nativeOnly []string
	for _, check := range checks {
		if len(check.PtahRules) > 0 && !everyRuleReachesCompat(check.PtahRules, compat) {
			nativeOnly = append(nativeOnly, check.Code)
		}
	}
	if len(nativeOnly) == 0 {
		return ", and the ones Ptah implements are reported through both surfaces"
	}
	return fmt.Sprintf(
		", and the ones Ptah implements are reported through both surfaces except %s, whose Ptah rule "+
			"the compatibility surface does not report",
		codeList(nativeOnly))
}

// everyRuleReachesCompat reports whether every rule a check names is one the
// compatibility surface can emit.
func everyRuleReachesCompat(codes []string, compat map[string]bool) bool {
	for _, code := range codes {
		if !compat[code] {
			return false
		}
	}
	return true
}

// compatRegistryNote says how much of the migration registry the compatibility
// surface reports.
//
// Both lint commands read one registry, which is why it is tempting to write
// that both report all of it. They do not: the compatibility profile classifies
// a rename as a destructive change and never emits rollout/rename, so the sentence has
// to carry whatever the Surface column carries, derived from the same flag.
func compatRegistryNote(entries []Entry) string {
	nativeOnly := nativeOnlyNames(entries, KindMigration)
	if len(nativeOnly) == 0 {
		return ", and so does `ptah-compat migrate lint`"
	}
	return fmt.Sprintf(
		", and `ptah-compat migrate lint` reports all of it but %s, which only native `ptah` emits",
		codeList(nativeOnly))
}

// namesOfKind returns the rule names one linter registers, in catalog order.
func namesOfKind(entries []Entry, kind Kind) []string {
	names := make([]string, 0, len(entries))
	for _, entry := range entriesOfKind(entries, kind) {
		names = append(names, entry.Name)
	}
	return names
}

// nativeOnlyNames returns the rule names of one kind that the compatibility
// surface never reports. It is what keeps a sentence about "the whole registry"
// from outrunning the Surface column two tables below it.
func nativeOnlyNames(entries []Entry, kind Kind) []string {
	var codes []string
	for _, entry := range entriesOfKind(entries, kind) {
		if !entry.Compat {
			codes = append(codes, entry.Name)
		}
	}
	return codes
}

// codeList renders rule names as prose: "`a/b`, `c/d` and `e/f`".
func codeList(codes []string) string {
	quoted := make([]string, 0, len(codes))
	for _, code := range codes {
		quoted = append(quoted, "`"+code+"`")
	}
	if len(quoted) < 2 {
		return strings.Join(quoted, "")
	}
	return strings.Join(quoted[:len(quoted)-1], ", ") + " and " + quoted[len(quoted)-1]
}

// proLabel renders whether the Atlas documentation marks a check as an Atlas
// Pro feature. It takes the check rather than the flag so the column can never
// be filled in from a boolean that belongs to another row.
func proLabel(check atlascodes.Check) string {
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
