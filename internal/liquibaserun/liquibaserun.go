// Package liquibaserun recognizes the attributes of a Liquibase changeset and
// says what each asks of an import: nothing, a no-transaction migration, a
// changeset Liquibase never runs, or a refusal. The refusals cover what decides
// whether, or how often, Liquibase runs a changeset -- context, labels, dbms,
// preconditions, runAlways, runOnChange -- and the attributes Ptah has no form
// for, such as failOnError="false".
//
// It also reads what both formatted-SQL readers must read alike: which lines of
// a file Liquibase reads at all ([FormattedLines]), and the property references
// Liquibase fills in when it runs ([PropertyReferences]).
//
// Every Ptah reader of a Liquibase changelog asks this package. The changeset
// parser in migration/importer splits a changelog into one migration per
// changeset, and the one-file conversion in internal/atlasmigrateimport copies a
// numbered formatted-SQL file whole. A list per reader drops, in the reader that
// was not told, whatever the other learned to read (stokaro/ptah#3713,
// stokaro/ptah#3714, stokaro/ptah#3727).
package liquibaserun

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// Effect says what one changeset attribute, with its value, asks of an import.
type Effect int

const (
	// Unknown is an attribute Ptah does not read. Liquibase ignores a name it
	// does not know as well, but a name Ptah does not know may be one Liquibase
	// does, so an import refuses it rather than guess.
	Unknown Effect = iota
	// NoEffect decides nothing a migration has to carry: an identifier, a
	// note, or a value that spells out Liquibase's default.
	NoEffect
	// Selector decides WHETHER the changeset runs.
	Selector
	// Repeat makes Liquibase run the changeset again after its first run.
	Repeat
	// NoTransaction runs the changeset, and its rollback, outside a
	// transaction.
	NoTransaction
	// Skip means Liquibase never runs the changeset and never records it.
	Skip
	// Unsupported is a value that changes how Liquibase runs the changeset in a
	// way a Ptah migration has no form for.
	Unsupported
)

// unsupportedReasons says, per attribute, what Liquibase does that a Ptah
// migration cannot. Keys are lower case.
var unsupportedReasons = map[string]string{
	"failonerror":           "Liquibase records the changeset as run when it fails, and a failed Ptah migration stops the apply",
	"runorder":              "Liquibase moves the changeset to the start or the end of the update",
	"runwith":               "Liquibase runs the SQL through that native client rather than the database driver",
	"runwithspoolfile":      "Liquibase runs the SQL through a native client and writes its output to that file",
	"objectquotingstrategy": "it changes how Liquibase quotes the names that typed changes create",
	"enddelimiter":          "Ptah does not know the delimiter, so it would stay in the SQL",
	"rollbackenddelimiter":  "Ptah does not know the delimiter, so it would stay in the rollback SQL",
}

// Attribute is the one place that recognizes a changeset attribute, and says
// what its value asks of an import. Names match in any case, as Liquibase
// matches them, and an empty value is Liquibase's default.
//
// A boolean attribute whose value is not a boolean is an error, because what
// the author meant by it is unknown.
func Attribute(key, value string) (Effect, error) {
	value = strings.TrimSpace(value)
	name := strings.ToLower(key)
	switch name {
	case "id", "author", "created", "logicalfilepath", "changelogid", "onvalidationfail",
		"comment", "validchecksum", "validchecksums",
		// A Ptah migration file is split into statements by Ptah, and its
		// comments do not execute, so neither setting changes what runs.
		"splitstatements", "rollbacksplitstatements", "stripcomments":
		return NoEffect, nil
	// contextFilter is the newer spelling of context, and Liquibase reads
	// either.
	case "context", "contextfilter", "contexts", "labels", "dbms":
		return presence(value, Selector), nil
	// A precondition is a selector whatever it holds; Liquibase accepts both
	// spellings of the element.
	case "preconditions":
		return Selector, nil
	// alwaysRun is the older spelling of runAlways, and Liquibase reads either.
	case "runalways", "alwaysrun", "runonchange":
		return whenTrue(key, value, Repeat)
	case "ignore":
		return whenTrue(key, value, Skip)
	case "runintransaction":
		return whenFalse(key, value, NoTransaction)
	case "failonerror":
		return whenFalse(key, value, Unsupported)
	case "runorder", "runwithspoolfile":
		return presence(value, Unsupported), nil
	case "runwith":
		if strings.EqualFold(value, "jdbc") {
			return NoEffect, nil
		}
		return presence(value, Unsupported), nil
	case "objectquotingstrategy":
		if strings.EqualFold(value, "LEGACY") {
			return NoEffect, nil
		}
		return presence(value, Unsupported), nil
	case "enddelimiter", "rollbackenddelimiter":
		if value == ";" {
			return NoEffect, nil
		}
		return presence(value, Unsupported), nil
	default:
		return Unknown, nil
	}
}

// presence is effect for a value that is set and NoEffect for an empty one.
func presence(value string, effect Effect) Effect {
	if value == "" {
		return NoEffect
	}
	return effect
}

// whenTrue is effect for a boolean attribute set to true.
func whenTrue(key, value string, effect Effect) (Effect, error) {
	if value == "" {
		return NoEffect, nil
	}
	parsed, ok := ParseBool(value)
	if !ok {
		return NoEffect, fmt.Errorf("%s %q is not true or false", key, value)
	}
	if parsed {
		return effect, nil
	}
	return NoEffect, nil
}

// whenFalse is effect for a boolean attribute, true by default, set to false.
func whenFalse(key, value string, effect Effect) (Effect, error) {
	if value == "" {
		return NoEffect, nil
	}
	parsed, ok := ParseBool(value)
	if !ok {
		return NoEffect, fmt.Errorf("%s %q is not true or false", key, value)
	}
	if parsed {
		return NoEffect, nil
	}
	return effect, nil
}

// ParseBool is the one grammar for a Liquibase boolean, and reports false for a
// value that is not one.
func ParseBool(value string) (parsed, ok bool) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "true":
		return true, true
	case "false":
		return false, true
	default:
		return false, false
	}
}

// Changeset collects what the attributes of one changeset ask of an import.
// The zero value asks nothing.
type Changeset struct {
	selectors     []string
	repeats       []string
	unsupported   []string
	unknown       []string
	noTransaction bool
	skip          bool
	err           error
	// dbms holds each changeset-level dbms value as written, so it can be
	// matched against a database name the caller states.
	dbms []string
}

// Note records one attribute written where only attributes can stand. A name
// [Attribute] does not recognize is recorded as one Ptah does not read.
func (c *Changeset) Note(key, value string) {
	if !c.NoteKnown(key, value) && !slices.Contains(c.unknown, key) {
		c.unknown = append(c.unknown, key)
	}
}

// NoteKnown records one attribute and reports whether [Attribute] recognizes
// its name. A name it does not recognize is left to the caller, which is how a
// reader tells an attribute written as a nested XML element from a change. A
// name is recorded once: formatted SQL writes one `--precondition-<type>` line
// per precondition.
func (c *Changeset) NoteKnown(key, value string) bool {
	effect, err := Attribute(key, value)
	switch {
	case effect == Unknown:
		return false
	case err != nil:
		if c.err == nil {
			c.err = err
		}
	case effect == Selector:
		c.AddSelector(key)
		if strings.EqualFold(key, "dbms") {
			c.dbms = append(c.dbms, value)
		}
	case effect == Repeat && !slices.Contains(c.repeats, key):
		c.repeats = append(c.repeats, key)
	case effect == Unsupported:
		c.unsupported = append(c.unsupported, fmt.Sprintf("%s=%s, which Ptah cannot carry because %s",
			key, strings.TrimSpace(value), unsupportedReasons[strings.ToLower(key)]))
	case effect == NoTransaction:
		c.noTransaction = true
	case effect == Skip:
		c.skip = true
	}
	return true
}

// AddSelector records a selector a reader recognized itself, such as the `dbms`
// of one change inside the changeset. A name is recorded once.
func (c *Changeset) AddSelector(name string) {
	if !slices.Contains(c.selectors, name) {
		c.selectors = append(c.selectors, name)
	}
}

// Clone returns a copy that shares nothing with c, so a changelog's own
// attributes can be the start of each changeset in it.
func (c Changeset) Clone() Changeset {
	c.selectors = slices.Clone(c.selectors)
	c.repeats = slices.Clone(c.repeats)
	c.unsupported = slices.Clone(c.unsupported)
	c.unknown = slices.Clone(c.unknown)
	c.dbms = slices.Clone(c.dbms)
	return c
}

// DBMS is the changeset's dbms as written, several joined by a comma and a
// space, or empty when it has none.
func (c Changeset) DBMS() string {
	return strings.Join(c.dbms, ", ")
}

// ResolveDBMS matches the changeset's dbms against the short name of the
// database the history ran on, and reports whether Liquibase runs the
// changeset there. A changeset with no dbms runs everywhere. Once resolved,
// dbms no longer refuses the changeset as a selector; the caller keeps or
// leaves out the changeset by the answer.
//
// Liquibase lowercases a changeset's dbms before matching it, so this does too.
// A name in the list that Liquibase does not know is an error, as it is to
// Liquibase's validation.
func (c *Changeset) ResolveDBMS(shortName string) (bool, error) {
	runs := true
	for _, definition := range c.dbms {
		matches, err := MatchDBMS(strings.ToLower(definition), shortName)
		if err != nil {
			return false, err
		}
		runs = runs && matches
	}
	c.selectors = slices.DeleteFunc(c.selectors, func(name string) bool {
		return strings.EqualFold(name, "dbms")
	})
	c.dbms = nil
	return runs, nil
}

// Skipped reports that Liquibase never runs the changeset: its `ignore` is
// true. A reader leaves such a changeset out and says so, which is what
// Liquibase does with it, whatever else the changeset carries.
func (c Changeset) Skipped() bool {
	return c.skip
}

// NoTransaction reports that the changeset and its rollback run outside a
// transaction: its `runInTransaction` is false.
func (c Changeset) NoTransaction() bool {
	return c.noTransaction
}

// Err refuses a changeset whose attributes ask for something a migration
// directory cannot carry, naming every attribute that does, and returns nil for
// one it can. name identifies the changeset and file the changelog holding it.
//
// A selector is named first and decides the remedy: a changeset that runs on
// some databases or in some environments has to be split in Liquibase, and
// removing any other attribute does not change that.
func (c Changeset) Err(name, file string) error {
	switch {
	case c.err != nil:
		return fmt.Errorf("liquibase changeset %s in %q: %w", name, file, c.err)
	case len(c.selectors) > 0:
		message := fmt.Sprintf(
			"liquibase changeset %s in %q is conditional on %s; a migration directory has no "+
				"equivalent, so importing it would turn a conditional history into an "+
				"unconditional one -- split the changelog or import it by hand",
			name, file, strings.Join(c.selectors, ", "))
		if len(c.repeats) > 0 {
			message += fmt.Sprintf("; it also sets %s, which a migration directory cannot express either",
				strings.Join(c.repeats, ", "))
		}
		return errors.New(message)
	case len(c.repeats) > 0:
		return fmt.Errorf(
			"liquibase changeset %s in %q sets %s, so Liquibase can run it again on a later update; "+
				"a Ptah migration runs once, so importing it would turn a repeated changeset into a "+
				"one-time one -- import it by hand",
			name, file, strings.Join(c.repeats, ", "))
	case len(c.unsupported) > 0 || len(c.unknown) > 0:
		clauses := slices.Clone(c.unsupported)
		for _, key := range c.unknown {
			clauses = append(clauses, key+", which Ptah does not read")
		}
		return fmt.Errorf("liquibase changeset %s in %q sets %s -- import it by hand",
			name, file, strings.Join(clauses, "; "))
	default:
		return nil
	}
}

// knownDBMS are the database short names Ptah knows Liquibase to accept: the
// ones Liquibase ships (liquibase-standard and its snowflake module), the ones
// its bigquery and redshift extensions add, and the ones the extensions for
// Ptah's own targets add -- cloudspanner, clickhouse and yugabytedb. Liquibase
// refuses a dbms naming a database it has no implementation for, so a name
// outside this list is refused here too; a changelog written for another
// extension is refused rather than guessed at.
var knownDBMS = []string{
	"asany", "bigquery", "clickhouse", "cloudspanner", "cockroachdb", "db2", "db2z", "derby", "edb",
	"firebird", "h2", "hsqldb", "informix", "ingres", "mariadb", "mssql", "mysql", "oracle",
	"postgresql", "redshift", "snowflake", "sqlite", "sybase", "yugabytedb",
}

// KnownDBMS reports whether name is a database short name Liquibase accepts in
// a dbms attribute. Names match in lower case, as Liquibase's validation
// matches them.
func KnownDBMS(name string) bool {
	return slices.Contains(knownDBMS, strings.ToLower(strings.TrimSpace(name)))
}

// KnownDBMSNames lists the names [KnownDBMS] accepts, in order.
func KnownDBMSNames() []string {
	return slices.Clone(knownDBMS)
}

// MatchDBMS reports whether a dbms definition selects the database whose
// short name is shortName, by Liquibase's rule (DatabaseList.definitionMatches):
// a comma-separated list; an empty one matches every database; `all` matches
// and outranks the rest, then `none` matches nothing, then `!name` excludes
// that database, and otherwise a list naming no database without `!` matches
// every one it does not exclude, and a list naming some matches only those.
//
// Names compare exactly. Liquibase lowercases a changeset's dbms and leaves a
// change's as written, so a caller passes each as Liquibase would compare it.
// A name without `!` that Liquibase does not know is an error, as it is to
// Liquibase's validation, which lowercases before it looks the name up.
func MatchDBMS(definition, shortName string) (bool, error) {
	var entries []string
	for entry := range strings.SplitSeq(definition, ",") {
		if entry = strings.TrimSpace(entry); entry != "" {
			entries = append(entries, entry)
		}
	}
	for _, entry := range entries {
		if entry != "all" && entry != "none" && !strings.HasPrefix(entry, "!") && !KnownDBMS(entry) {
			return false, fmt.Errorf("dbms %q names %q, which is not a database Liquibase knows", definition, entry)
		}
	}
	switch {
	case len(entries) == 0, slices.Contains(entries, "all"):
		return true, nil
	case slices.Contains(entries, "none"), slices.Contains(entries, "!"+shortName):
		return false, nil
	}
	supported := slices.DeleteFunc(slices.Clone(entries), func(entry string) bool {
		return strings.HasPrefix(entry, "!")
	})
	return len(supported) == 0 || slices.Contains(supported, shortName), nil
}

var (
	// changesetRE matches on the `--changeset` keyword alone (not the
	// argument), so that a marker missing its author:id is still a marker.
	// `\b` keeps `--changesetlike` from matching.
	changesetRE = regexp.MustCompile(`(?i)^\s*--\s*changeset\b(.*)$`)
	// attributeRE reads one `name:value` attribute after the author:id of a
	// `--changeset` line. The value may be quoted, as Liquibase allows for
	// context and labels, and may follow the colon after blanks, which
	// Liquibase's own dbms pattern accepts.
	attributeRE = regexp.MustCompile(`([A-Za-z]+):[ \t]*("[^"]*"|\S*)`)
	// preconditionsRE matches the `--preconditions` line and each
	// `--precondition-<type>` line that follows it, the formatted-SQL spelling
	// of a changeset's preConditions.
	preconditionsRE = regexp.MustCompile(`(?i)^\s*--\s*precondition(?:s|-)`)
	// ignoreLinesRE is Liquibase's `--ignoreLines:<start|end|count>` directive.
	// Like Liquibase's own pattern it must fill the line: no blank before the
	// dashes, nothing after the word.
	ignoreLinesRE = regexp.MustCompile(`(?i)^--\s*ignoreLines:(\w+)$`)
	// ignoreLinesOneDashRE and ignoreRE are the spellings Liquibase refuses
	// rather than reads as a comment: `-ignoreLines:` with one dash, and
	// `--ignore:` for `--ignoreLines:`. Inside a skipped block Liquibase refuses
	// only the first.
	ignoreLinesOneDashRE = regexp.MustCompile(`(?i)^-\s*ignoreLines:\w+.*$`)
	ignoreRE             = regexp.MustCompile(`(?i)^--\s*ignore:\w+$`)
)

// FormattedLines returns the lines of a formatted-SQL changelog that Liquibase
// reads, in order: every line but an `--ignoreLines` directive and the lines it
// skips. Both readers take their lines from here, so a line Liquibase skips
// reaches neither the SQL nor the changeset markers.
//
// It follows Liquibase's formatted-SQL parser. `--ignoreLines:start` skips every
// line up to the next `--ignoreLines:end`, and to the end of the file when there
// is none. `--ignoreLines:<n>` skips the next n lines. The directive must fill
// its line, its name matches in any case, and start and end match only in lower
// case. Any other word, such as `START`, is an error, as it is to Liquibase, and
// so are the spellings Liquibase refuses: `-ignoreLines:` with one dash, and
// `--ignore:`. A line's trailing carriage return is ignored when matching and
// kept in what is returned.
func FormattedLines(file, content string) ([]string, error) {
	lines := strings.Split(content, "\n")
	var kept []string
	for number := 0; number < len(lines); number++ {
		line := strings.TrimSuffix(lines[number], "\r")
		match := ignoreLinesRE.FindStringSubmatch(line)
		switch {
		case match == nil && (ignoreLinesOneDashRE.MatchString(line) || ignoreRE.MatchString(line)):
			return nil, misspelledIgnoreLines(file, number, line)
		case match == nil:
			kept = append(kept, lines[number])
		case match[1] == "start":
			end, misspelled := ignoreLinesBlockEnd(lines, number)
			if misspelled >= 0 {
				return nil, misspelledIgnoreLines(file, misspelled, lines[misspelled])
			}
			number = end
		default:
			count, err := strconv.ParseInt(match[1], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("liquibase changelog %q line %d: %q names neither start nor a number of "+
					"lines, so Liquibase refuses it", file, number+1, line)
			}
			number += int(min(count, int64(len(lines))))
		}
	}
	return kept, nil
}

// ignoreLinesBlockEnd returns the index of the `--ignoreLines:end` that closes
// the block opened at start, or the last index when nothing closes it: an
// unterminated block runs to the end of the file, as it does in Liquibase. It
// also returns the index of a one-dash `-ignoreLines:` inside the block, which
// Liquibase refuses there too, or -1.
func ignoreLinesBlockEnd(lines []string, start int) (end, misspelled int) {
	for number := start + 1; number < len(lines); number++ {
		line := strings.TrimSuffix(lines[number], "\r")
		match := ignoreLinesRE.FindStringSubmatch(line)
		switch {
		case match != nil && match[1] == "end":
			return number, -1
		case match == nil && ignoreLinesOneDashRE.MatchString(line):
			return number, number
		}
	}
	return len(lines) - 1, -1
}

// misspelledIgnoreLines refuses a spelling of the directive Liquibase refuses.
func misspelledIgnoreLines(file string, number int, line string) error {
	return fmt.Errorf("liquibase changelog %q line %d: %q is not a directive Liquibase reads, and Liquibase "+
		"refuses it -- write --ignoreLines:<count|start|end>", file, number+1, strings.TrimSuffix(line, "\r"))
}

// PropertyReferences returns the property references Liquibase fills in when it
// runs -- `${name}` -- in the order they appear in text, each once and as
// written. A `${` with no closing brace is text to Liquibase and is not
// returned; a reference inside another, `${a${b}}`, is returned as the outer
// one.
func PropertyReferences(text string) []string {
	var references []string
	for offset := 0; ; {
		start := strings.Index(text[offset:], "${")
		if start < 0 {
			return references
		}
		start += offset
		end := propertyReferenceEnd(text, start+2)
		if end < 0 {
			return references
		}
		if reference := text[start : end+1]; !slices.Contains(references, reference) {
			references = append(references, reference)
		}
		offset = end + 1
	}
}

// propertyReferenceEnd returns the index of the brace that closes a reference
// whose name starts at from, counting the references nested in it, or -1.
func propertyReferenceEnd(text string, from int) int {
	depth := 1
	for index := from; index < len(text); index++ {
		switch {
		case strings.HasPrefix(text[index:], "${"):
			depth++
			index++
		case text[index] == '}':
			depth--
			if depth == 0 {
				return index
			}
		}
	}
	return -1
}

// PropertyReferencesErr refuses texts holding a property reference, naming
// each one, and returns nil when they hold none. The message continues a
// sentence whose subject the caller names: "<sql> " + err.Error().
//
// A reference is refused rather than filled in because the changelog does not
// record its value. Liquibase takes it, in order, from an environment variable
// or Java system property of that name, matched in any case, from a
// command-line or liquibase.properties parameter, and only then from a property
// the changelog defines, each filtered by its own context, labels and dbms.
// Measured with Liquibase 5.0.4: a changelog property tbl was overridden by the
// environment variable TBL and by the parameter -Dtbl.
func PropertyReferencesErr(texts ...string) error {
	var references []string
	for _, text := range texts {
		for _, reference := range PropertyReferences(text) {
			if !slices.Contains(references, reference) {
				references = append(references, reference)
			}
		}
	}
	if len(references) == 0 {
		return nil
	}
	return fmt.Errorf("uses the property reference %s; Liquibase fills it in when it runs, and an environment "+
		"variable, a Java system property or a command-line parameter of that name wins over any property the "+
		"changelog defines, so the changelog does not record the value that ran -- write the value in, or "+
		"import the changeset by hand", strings.Join(references, ", "))
}

// ChangesetArgs reports whether line is a formatted-SQL `--changeset` marker,
// and returns what follows the keyword: the author:id and the attributes.
func ChangesetArgs(line string) (string, bool) {
	match := changesetRE.FindStringSubmatch(line)
	if match == nil {
		return "", false
	}
	return match[1], true
}

// IsPreconditions reports whether line is one of the formatted-SQL lines that
// declare a changeset's preconditions. To the database it is a comment; to
// Liquibase it is a condition.
func IsPreconditions(line string) bool {
	return preconditionsRE.MatchString(line)
}

// Attributes reads the `name:value` attributes that follow the author:id on a
// `--changeset` line, such as `dbms:mysql` or `context:"prod or staging"`, in
// the order they are written. A quoted value loses its quotes.
func Attributes(args string) [][2]string {
	fields := strings.Fields(args)
	if len(fields) < 2 {
		return nil
	}
	// The first field is author:id, which the attribute pattern would read as
	// an attribute named for the author.
	rest := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(args), fields[0]))
	var attributes [][2]string
	for _, match := range attributeRE.FindAllStringSubmatch(rest, -1) {
		attributes = append(attributes, [2]string{match[1], strings.Trim(match[2], `"`)})
	}
	return attributes
}

// FormattedCopy is what a formatted-SQL file copied whole carries, once
// [ScanFormattedSQL] has read it.
type FormattedCopy struct {
	// Lines are the lines Liquibase reads, from [FormattedLines]: the copy is
	// made of these and nothing else.
	Lines []string
	// NoTransaction reports that every changeset in the file sets
	// runInTransaction to false, so the copy runs outside a transaction.
	NoTransaction bool
}

// ScanFormattedSQL reads a formatted-SQL file that a reader copies whole rather
// than splits, and refuses the file when one of its changesets asks for
// something a copy cannot carry. The refusal names the first such changeset by
// its author:id.
//
// It reads the lines [FormattedLines] keeps, so a changeset inside an
// `--ignoreLines` block is not read at all, as Liquibase does not read it. A
// changeset whose attributes refuse it, or whose SQL holds a property reference
// ([PropertyReferencesErr]), refuses the file. A copy is one migration, so the
// changesets in it share one transaction mode: every changeset setting
// runInTransaction to false makes the copy a no-transaction migration, and a
// file mixing the two modes is refused, because either mode would run one of
// its changesets differently from Liquibase. A changeset Liquibase never runs
// (ignore="true") is refused as well, because a copy cannot leave one changeset
// out.
//
// It reads nothing else, so a file the changeset parser in migration/importer
// would refuse for its layout -- a header with no changeset, SQL before the
// first one -- passes here. A copy loses nothing in those shapes.
func ScanFormattedSQL(file, content string) (FormattedCopy, error) {
	lines, err := FormattedLines(file, content)
	if err != nil {
		return FormattedCopy{}, err
	}
	var current *Changeset
	var body []string
	name := ""
	changesets, noTransactionChangesets := 0, 0
	finish := func() error {
		if current == nil {
			return nil
		}
		if err := current.Err(name, file); err != nil {
			return err
		}
		if current.Skipped() {
			return fmt.Errorf("liquibase changeset %s in %q sets ignore, so Liquibase never runs it, and "+
				"a file copied whole cannot leave one changeset out -- remove the changeset or import it by hand",
				name, file)
		}
		if err := PropertyReferencesErr(body...); err != nil {
			return fmt.Errorf("liquibase changeset %s in %q %w", name, file, err)
		}
		changesets++
		if current.NoTransaction() {
			noTransactionChangesets++
		}
		return nil
	}
	for _, line := range lines {
		if args, ok := ChangesetArgs(line); ok {
			if err := finish(); err != nil {
				return FormattedCopy{}, err
			}
			current, body = &Changeset{}, nil
			name = changesetName(args)
			for _, attribute := range Attributes(args) {
				current.Note(attribute[0], attribute[1])
			}
			continue
		}
		if current == nil {
			continue
		}
		if IsPreconditions(line) {
			current.NoteKnown("preconditions", "")
		}
		body = append(body, line)
	}
	if err := finish(); err != nil {
		return FormattedCopy{}, err
	}
	if noTransactionChangesets > 0 && noTransactionChangesets < changesets {
		return FormattedCopy{}, fmt.Errorf("liquibase file %q sets runInTransaction to false on some changesets "+
			"and not on others, and a file copied whole runs as one migration in one mode -- give each mode a "+
			"file of its own", file)
	}
	return FormattedCopy{Lines: lines, NoTransaction: noTransactionChangesets > 0}, nil
}

// changesetName is the author:id a `--changeset` marker names, as the author
// wrote it.
func changesetName(args string) string {
	fields := strings.Fields(args)
	if len(fields) == 0 {
		return "without author:id"
	}
	return fields[0]
}
