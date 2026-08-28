package terminologyguard_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"go.5x5.cz/ptah/cmd/root"
)

// registryPath is the one registry, read here and by
// docs/site/scripts/check-terminology.mjs.
const registryPath = "docs/site/scripts/data/terminology.json"

// styleGuidePath carries the generated rendering of that registry. This package
// compares against it rather than against its own parse, so a Go half that
// stopped reading the JSON disagrees with an artifact it did not produce.
const styleGuidePath = "docs/STYLE_GUIDE.md"

type registry struct {
	Markers struct {
		File  string `json:"file"`
		Begin string `json:"begin"`
		End   string `json:"end"`
	} `json:"markers"`
	Terms []term `json:"terms"`
}

type term struct {
	ID      string `json:"id"`
	Term    string `json:"term"`
	Meaning string `json:"meaning"`
	Rule    string `json:"rule"`
	Bans    []ban  `json:"bans"`
}

type ban struct {
	ID            string    `json:"id"`
	Stems         []string  `json:"stems"`
	BannedHeads   []string  `json:"bannedHeads"`
	AllowedHeads  []string  `json:"allowedHeads"`
	OnUnknownHead string    `json:"onUnknownHead"`
	Message       string    `json:"message"`
	Enforced      *bool     `json:"enforced"`
	HelpText      *helpText `json:"helpText"`
}

type helpText struct {
	Tree   string `json:"tree"`
	Reason string `json:"reason"`
}

// TestNativeHelpTextRespectsTheTerminologyRegistry is the guard.
//
// It reads the assembled command tree rather than the source that builds it, so
// a sentence composed at run time is covered the same as a literal.
func TestNativeHelpTextRespectsTheTerminologyRegistry(t *testing.T) {
	c := qt.New(t)

	loaded := loadRegistry(c)
	bans := nativeHelpBans(loaded)
	c.Assert(bans, qt.Not(qt.HasLen), 0,
		qt.Commentf("no ban declares helpText.tree = native; this guard would report a clean tree forever"))

	surfaces := helpSurfaces(root.NewRootCommand())
	c.Assert(surfaces, qt.Not(qt.HasLen), 0,
		qt.Commentf("the command tree yielded no help text; refusing to report OK on an empty corpus"))

	c.Assert(strings.Join(findings(surfaces, bans), "\n"), qt.Equals, "",
		qt.Commentf("see section 7 of %s; the registry is %s", styleGuidePath, registryPath))
}

// TestTheRegistryReachesBothReaders is what says the Go half is still reading
// the registry at all.
//
// A second reader stops reading silently. The comparison is against the block
// docs/site/scripts/check-terminology.mjs generates into docs/STYLE_GUIDE.md,
// which is an artifact this package does not write: a Go reader that parsed
// nothing produces no rows and disagrees with a table that has twenty, instead
// of agreeing with its own empty parse.
func TestTheRegistryReachesBothReaders(t *testing.T) {
	c := qt.New(t)

	loaded := loadRegistry(c)
	c.Assert(loaded.Markers.File, qt.Equals, styleGuidePath)

	rendered := generatedRows(c, loaded)
	c.Assert(rendered, qt.HasLen, len(loaded.Terms),
		qt.Commentf("%s carries %d generated rows and %s declares %d terms",
			styleGuidePath, len(rendered), registryPath, len(loaded.Terms)))

	names := make([]string, 0, len(loaded.Terms))
	for _, entry := range loaded.Terms {
		names = append(names, entry.Term)
	}
	c.Assert(names, qt.DeepEquals, rendered,
		qt.Commentf("run: node docs/site/scripts/check-terminology.mjs --write"))
}

// TestEveryBanIsWellFormed refuses a registry entry any reader would read as
// governing nothing. A ban with no stems matches nothing and reports nothing,
// which is indistinguishable from a ban that is satisfied.
//
// It reads EVERY ban, not the subset this package scans. The subset is the
// hazard: a prose-only ban -- a shape the registry documents and supports --
// carries no helpText, so filtering to helpText.tree == "native" would leave
// the one kind of malformed ban this test cannot see. Measured before the
// widening: a ban with an empty `stems` was accepted by all three readers,
// counted as coverage in the OK line, and reported nothing at all.
func TestEveryBanIsWellFormed(t *testing.T) {
	c := qt.New(t)

	entries := allBans(loadRegistry(c))
	c.Assert(entries, qt.Not(qt.HasLen), 0,
		qt.Commentf("%s declares no bans; this test would pass on a registry that governs nothing", registryPath))

	for _, entry := range entries {
		t.Run(entry.ID, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(entry.Stems, qt.Not(qt.HasLen), 0,
				qt.Commentf("a ban with no stems matches nothing and reports nothing"))
			c.Assert(entry.Message, qt.Not(qt.Equals), "")
			c.Assert(len(entry.BannedHeads)+len(entry.AllowedHeads), qt.Not(qt.Equals), 0)
			c.Assert(len(entry.BannedHeads) > 0 || entry.OnUnknownHead == "report", qt.IsTrue,
				qt.Commentf("a ban that names no banned head and allows every head it does not know matches nothing"))
		})
	}
}

// TestEveryHelpTextBanNamesTheTreeItGoverns is the half that applies only to
// the bans this package reads. Which command tree a ban governs is a product
// decision, and a decision without its reasoning is a decision nobody can
// argue with.
func TestEveryHelpTextBanNamesTheTreeItGoverns(t *testing.T) {
	c := qt.New(t)

	entries := nativeHelpBans(loadRegistry(c))
	c.Assert(entries, qt.Not(qt.HasLen), 0)

	for _, entry := range entries {
		t.Run(entry.ID, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(entry.HelpText.Reason, qt.Not(qt.Equals), "")
		})
	}
}

// TestTheGuardCatchesTheShapesItClaimsTo drives the same matcher the guard runs
// over fixtures that must produce a finding.
//
// Without it, a matcher that stopped matching would report the clean tree above
// and look exactly like a matcher that found nothing to report. The wrapped
// fixture is the one that matters most: the sentence it reproduces is the one
// this guard was written for, and `grep -n 'declarative schema changes'` over
// the file that held it answered nothing, because the phrase wraps.
func TestTheGuardCatchesTheShapesItClaimsTo(t *testing.T) {
	tests := []struct {
		name string
		help string
	}{
		{
			name: "the sentence ptah --help opened with",
			help: "Use versioned migrations, declarative schema changes, or both, across supported databases.",
		},
		{
			name: "the same sentence wrapped mid-phrase, where a line-based grep answers nothing",
			help: "Use versioned migrations, declarative schema\nchanges, or both, across supported databases.",
		},
		{name: "a plan verb's Short", help: "Save a fingerprinted declarative apply plan"},
		{name: "a validate verb's Long", help: "the same way every other declarative verb takes it"},
		{name: "the label shortened", help: "Run the declarative changes quick start first."},
		{name: "a head the registry classifies neither way", help: "Ptah supports declarative frobnication."},
		{
			name: "a quoted sentence that uses the label, where the quote runs on past the noun phrase",
			help: `The help text says "declarative workflow steps are printed here".`,
		},
		{
			name: "a value whose quote wraps the label, which is text a reader meets rather than a quotation",
			help: `title="Declarative changes"`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			bans := nativeHelpBans(loadRegistry(c))
			found := findings([]surface{{where: "fixture", text: test.help}}, bans)
			c.Assert(found, qt.Not(qt.HasLen), 0)
		})
	}
}

// TestTheGuardIsSilentOnTheSensesTheRegistryAllows is the other half, and it is
// not optional. A matcher that reported everything would pass the test above
// while making the guard useless: the census measured 142 occurrences of the
// word against 19 violations, and a gate that fires on the other 123 is a gate
// the next person turns off rather than obeys.
func TestTheGuardIsSilentOnTheSensesTheRegistryAllows(t *testing.T) {
	tests := []struct {
		name string
		help string
	}{
		{
			name: "a capability name",
			help: "Generate a reversible data migration from declarative reference-data drift.",
		},
		{
			name: "Atlas's own name for its test format",
			help: "Run declarative YAML cases with migrate/apply-schema/seed/SQL/assert steps.",
		},
		{
			name: "a capability name in a flag usage",
			help: "Run declarative test cases against a throwaway database",
		},
		{name: "an ordinary compound", help: "Render declarative foreign keys for every dialect."},
		{name: "an adjective with no head noun", help: "The cases are declarative."},
		{name: "the word quoted as itself", help: `Ptah does not use "declarative" as the name of a workflow.`},
		{
			name: "the whole retired spelling quoted as itself",
			help: `Ptah retired the phrase "declarative schema changes" before v1.`,
		},
		{
			name: "a quotation a colon introduces mid-sentence",
			help: `Section 7 says: "declarative schema changes" is retired.`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			bans := nativeHelpBans(loadRegistry(c))
			c.Assert(findings([]surface{{where: "fixture", text: test.help}}, bans), qt.HasLen, 0)
		})
	}
}

// --------------------------------------------------------------------------
// The registry, and the tree it governs
// --------------------------------------------------------------------------

func repositoryRoot(c *qt.C) string {
	c.Helper()
	dir, err := filepath.Abs(filepath.Join("..", "..", ".."))
	c.Assert(err, qt.IsNil)
	return dir
}

func loadRegistry(c *qt.C) registry {
	c.Helper()
	raw, err := os.ReadFile(filepath.Join(repositoryRoot(c), filepath.FromSlash(registryPath)))
	c.Assert(err, qt.IsNil)

	var loaded registry
	c.Assert(json.Unmarshal(raw, &loaded), qt.IsNil)
	c.Assert(loaded.Terms, qt.Not(qt.HasLen), 0,
		qt.Commentf("%s declares no terms; refusing to check a command tree against an empty registry", registryPath))
	return loaded
}

// allBans is every ban the registry declares, whatever corpus it governs.
func allBans(loaded registry) []ban {
	var selected []ban
	for _, entry := range loaded.Terms {
		selected = append(selected, entry.Bans...)
	}
	return selected
}

// nativeHelpBans is every enforced ban whose helpText names the native tree.
func nativeHelpBans(loaded registry) []ban {
	var selected []ban
	for _, entry := range loaded.Terms {
		for _, candidate := range entry.Bans {
			if candidate.HelpText == nil || candidate.HelpText.Tree != "native" {
				continue
			}
			if candidate.Enforced != nil && !*candidate.Enforced {
				continue
			}
			selected = append(selected, candidate)
		}
	}
	return selected
}

// generatedRows reads the first cell of every row inside the generated block of
// the style guide, in order.
func generatedRows(c *qt.C, loaded registry) []string {
	c.Helper()
	raw, err := os.ReadFile(filepath.Join(repositoryRoot(c), filepath.FromSlash(loaded.Markers.File)))
	c.Assert(err, qt.IsNil)

	document := string(raw)
	begin := strings.Index(document, loaded.Markers.Begin)
	end := strings.Index(document, loaded.Markers.End)
	c.Assert(begin >= 0 && end > begin, qt.IsTrue,
		qt.Commentf("%s carries no %s / %s markers", loaded.Markers.File, loaded.Markers.Begin, loaded.Markers.End))

	block := document[begin+len(loaded.Markers.Begin) : end]
	var rows []string
	for line := range strings.SplitSeq(block, "\n") {
		cells := strings.Split(strings.Trim(strings.TrimSpace(line), "|"), "|")
		first := strings.TrimSpace(cells[0])
		skip := first == "" || first == "Term" || strings.Trim(first, "-: ") == ""
		if skip {
			continue
		}
		rows = append(rows, strings.ReplaceAll(first, `\|`, "|"))
	}
	return rows
}

// surface is one piece of help text and the place a reader meets it.
type surface struct {
	where string
	text  string
}

// helpSurfaces walks the command tree and returns every string a `--help` run
// can print: the command's own descriptions and every flag usage under it.
func helpSurfaces(cmd *cobra.Command) []surface {
	path := cmd.CommandPath()
	surfaces := []surface{
		{where: path + " Short", text: cmd.Short},
		{where: path + " Long", text: cmd.Long},
		{where: path + " Example", text: cmd.Example},
	}
	collect := func(kind string, flags *pflag.FlagSet) {
		flags.VisitAll(func(flag *pflag.Flag) {
			surfaces = append(surfaces, surface{where: fmt.Sprintf("%s --%s (%s)", path, flag.Name, kind), text: flag.Usage})
		})
	}
	collect("flag", cmd.Flags())
	collect("persistent flag", cmd.PersistentFlags())

	for _, child := range cmd.Commands() {
		surfaces = append(surfaces, helpSurfaces(child)...)
	}
	return surfaces
}

// findings runs every ban over every surface.
func findings(surfaces []surface, bans []ban) []string {
	var found []string
	for _, entry := range bans {
		for _, target := range surfaces {
			for _, hit := range scan(target.text, entry) {
				found = append(found, fmt.Sprintf("%s: %s", target.where, hit))
			}
		}
	}
	sort.Strings(found)
	return found
}

// --------------------------------------------------------------------------
// The matcher
//
// It mirrors headAfter/classifyHead in docs/site/scripts/check-terminology.mjs.
// Two implementations of one rule is a hazard this repository names -- so the
// fixtures above and the ones in that script's --selftest are deliberately the
// same sentences, and the shared registry is what they both read the vocabulary
// from. What cannot be shared is the corpus: one reads Markdown, the other
// reads a cobra tree.
// --------------------------------------------------------------------------

var urlPattern = regexp.MustCompile(`[a-zA-Z][a-zA-Z0-9+.-]*://[^\s)\]<>"'` + "`" + `]+`)

func scan(text string, entry ban) []string {
	prose := urlPattern.ReplaceAllStringFunc(text, func(match string) string {
		return strings.Repeat(" ", len(match))
	})

	var hits []string
	for _, stem := range entry.Stems {
		pattern := regexp.MustCompile(`(?i)(^|[^A-Za-z0-9_-])(` + regexp.QuoteMeta(stem) + `)([^A-Za-z]|$)`)
		for _, match := range pattern.FindAllStringSubmatchIndex(prose, -1) {
			start, stop := match[4], match[5]
			if isMention(prose, start, stop) {
				continue
			}
			words := headAfter(prose[stop:])
			if len(words) == 0 {
				continue
			}
			verdict, head := classifyHead(words, entry)
			if verdict == "allowed" {
				continue
			}
			spelling := strings.ToLower(prose[start:stop]) + " " + head
			if verdict == "banned" {
				hits = append(hits, fmt.Sprintf("retired spelling %q: %s", spelling, entry.Message))
				continue
			}
			hits = append(hits, fmt.Sprintf(
				"%q heads a noun the registry classifies neither way; add it to allowedHeads or bannedHeads of %q in %s",
				spelling, entry.ID, registryPath))
		}
	}
	return hits
}

// isMention reports whether the stem is wrapped in a matching quote pair that
// closes around the TERM -- at the stem, or at the end of one of the head words
// after it -- which makes the sentence a mention of the label rather than a use
// of it.
//
// A quote that runs on past the noun phrase is quoting a sentence, and a
// sentence that uses the label uses it. A quote that opens a VALUE is not a
// quotation at all.
func isMention(prose string, start, stop int) bool {
	if start == 0 {
		return false
	}
	pairs := map[byte]byte{'"': '"', '\'': '\'', '`': '`'}
	closing, paired := pairs[prose[start-1]]
	if !paired || isValueQuote(prose, start-1) {
		return false
	}

	index := stop
	for words := 0; words <= headWords; {
		if index < len(prose) && prose[index] == closing {
			return true
		}
		if index >= len(prose) {
			return false
		}
		if isHeadSkip(prose[index]) {
			index++
			continue
		}
		if !isLetter(prose[index]) {
			return false
		}
		for index < len(prose) && isLetter(prose[index]) {
			index++
		}
		words++
	}
	return false
}

// isValueQuote reports whether the quote at quoteAt opens a value rather than a
// quotation: it follows `=`, or a key that begins its own line.
//
// The key has to begin its line, or `Section 7 says: "declarative schema
// changes" is retired` would read as a value and be reported -- prose about the
// rule, reported by the rule.
func isValueQuote(prose string, quoteAt int) bool {
	index := quoteAt - 1
	for index >= 0 && (prose[index] == ' ' || prose[index] == '\t' || prose[index] == '{') {
		index--
	}
	if index < 0 {
		return false
	}
	if prose[index] == '=' {
		return true
	}
	if prose[index] != ':' {
		return false
	}
	index--
	for index >= 0 && (prose[index] == ' ' || prose[index] == '\t') {
		index--
	}
	keyEnd := index
	for index >= 0 && (isLetter(prose[index]) || isKeyPunctuation(prose[index])) {
		index--
	}
	if index == keyEnd {
		return false
	}
	for index >= 0 && (prose[index] == ' ' || prose[index] == '\t') {
		index--
	}
	return index < 0 || prose[index] == '\n'
}

func isKeyPunctuation(char byte) bool {
	return char == '_' || char == '-' || (char >= '0' && char <= '9')
}

const headWords = 4

func isHeadSkip(char byte) bool {
	return char == ' ' || char == '\t' || char == '\n' || char == '\r' || char == '-' || char == '*' || char == '_' || char == '~'
}

func isHeadStop(char byte) bool {
	return strings.IndexByte(`.,;:!?()"'|[]{}<>@#=+\/`, char) >= 0
}

func isLetter(char byte) bool {
	return (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z')
}

// headAfter takes up to four words, treating whitespace, the hyphen and
// emphasis markers as separators and stopping at clause punctuation.
func headAfter(text string) []string {
	var words []string
	index := 0
	for len(words) < headWords {
		for index < len(text) && isHeadSkip(text[index]) {
			index++
		}
		if index >= len(text) || isHeadStop(text[index]) {
			break
		}
		start := index
		for index < len(text) && isLetter(text[index]) {
			index++
		}
		if index == start {
			break
		}
		words = append(words, strings.ToLower(text[start:index]))
		if index >= len(text) || !isHeadSkip(text[index]) {
			break
		}
	}
	return words
}

// classifyHead takes the longest head either list names. `schema apply` is an
// Atlas page title and `schema changes` is the retired workflow label; both
// begin with `schema`, so the shorter match must never win.
func classifyHead(words []string, entry ban) (verdict, head string) {
	banned := lowered(entry.BannedHeads)
	allowed := lowered(entry.AllowedHeads)
	for length := min(len(words), headWords); length >= 1; length-- {
		head := strings.Join(words[:length], " ")
		if banned[head] {
			return "banned", head
		}
		if allowed[head] {
			return "allowed", head
		}
	}
	if entry.OnUnknownHead == "report" {
		return "unknown", strings.Join(words, " ")
	}
	return "allowed", strings.Join(words, " ")
}

func lowered(values []string) map[string]bool {
	set := make(map[string]bool, len(values))
	for _, value := range values {
		set[strings.ToLower(value)] = true
	}
	return set
}
