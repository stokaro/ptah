// Package liquibaserun recognizes the Liquibase changeset attributes that decide
// whether, or how often, Liquibase runs a changeset: context, labels, dbms,
// preconditions, runAlways and runOnChange.
//
// A migration directory has no equivalent for any of them, so every Ptah reader
// of a Liquibase changelog refuses a changeset that carries one. The changeset
// parser in migration/importer splits a changelog into one migration per
// changeset, and the one-file conversion in internal/atlasmigrateimport copies a
// numbered formatted-SQL file whole. Both ask this package, because a list per
// reader drops, in the reader that was not told, whatever the other learned to
// refuse (stokaro/ptah#3713).
package liquibaserun

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
)

// Kind says what a changeset attribute decides about running the changeset.
type Kind int

const (
	// None is an attribute that decides nothing about running the changeset.
	None Kind = iota
	// Selector decides WHETHER the changeset runs.
	Selector
	// Repeat decides whether Liquibase runs the changeset again after its
	// first run.
	Repeat
)

// Condition is the one place that recognizes what decides whether, or how
// often, Liquibase runs a changeset.
//
// kind is reported for every name the function recognizes, and binds reports
// whether the value makes the condition hold: an empty value is the default, so
// an empty selector selects nothing, and `runAlways="false"` spells out the
// default. A repeat whose value is not a boolean is an error, because what the
// author meant by it is unknown. Names match in any case, as Liquibase matches
// them.
func Condition(key, value string) (kind Kind, binds bool, err error) {
	switch strings.ToLower(key) {
	// contextFilter is the newer spelling of context, and Liquibase reads
	// either.
	case "context", "contextfilter", "contexts", "labels", "dbms":
		return Selector, strings.TrimSpace(value) != "", nil
	// A precondition is a selector whatever it holds; Liquibase accepts both
	// spellings of the element.
	case "preconditions":
		return Selector, true, nil
	// alwaysRun is the older spelling of runAlways, and Liquibase reads either.
	case "runalways", "alwaysrun", "runonchange":
		if strings.TrimSpace(value) == "" {
			return Repeat, false, nil
		}
		repeat, ok := ParseBool(value)
		if !ok {
			return Repeat, false, fmt.Errorf("%s %q is not true or false", key, value)
		}
		return Repeat, repeat, nil
	default:
		return None, false, nil
	}
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

// Conditions collects the run conditions one changeset carries. The zero value
// holds none.
type Conditions struct {
	selectors []string
	repeats   []string
	err       error
}

// Note records key when it names a run condition that holds, and reports
// whether key names a run condition at all, so a reader can tell one from a
// change. A name is recorded once: formatted SQL writes one
// `--precondition-<type>` line per precondition.
func (c *Conditions) Note(key, value string) bool {
	kind, binds, err := Condition(key, value)
	switch {
	case kind == None:
		return false
	case err != nil:
		if c.err == nil {
			c.err = err
		}
	case binds && kind == Selector:
		c.AddSelector(key)
	case binds && kind == Repeat && !slices.Contains(c.repeats, key):
		c.repeats = append(c.repeats, key)
	}
	return true
}

// AddSelector records a selector a reader recognized itself, such as the `dbms`
// of one change inside the changeset. A name is recorded once.
func (c *Conditions) AddSelector(name string) {
	if !slices.Contains(c.selectors, name) {
		c.selectors = append(c.selectors, name)
	}
}

// Err refuses a changeset that Liquibase runs conditionally or more than once,
// naming every attribute that makes it so, and returns nil for one that runs
// unconditionally, once. name identifies the changeset and file the changelog
// holding it.
//
// A selector is named first and decides the remedy: a changeset that runs on
// some databases or in some environments has to be split in Liquibase, and
// removing a repeat attribute does not change that.
func (c Conditions) Err(name, file string) error {
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
	default:
		return nil
	}
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
)

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

// ScanFormattedSQL refuses a formatted-SQL file holding a changeset that
// Liquibase runs conditionally or more than once, and returns nil otherwise.
// The refusal names the first such changeset by its author:id.
//
// It reads the run conditions and nothing else, so a file the changeset parser
// in migration/importer would refuse for its layout -- a header with no
// changeset, SQL before the first one -- passes here. It is for a reader that
// copies the file whole rather than splitting it, where the layout loses
// nothing and a run condition is the one thing dropped.
func ScanFormattedSQL(file, content string) error {
	var current *Conditions
	name := ""
	for line := range strings.SplitSeq(content, "\n") {
		if args, ok := ChangesetArgs(line); ok {
			if current != nil {
				if err := current.Err(name, file); err != nil {
					return err
				}
			}
			current = &Conditions{}
			name = changesetName(args)
			for _, attribute := range Attributes(args) {
				current.Note(attribute[0], attribute[1])
			}
			continue
		}
		if current != nil && IsPreconditions(line) {
			current.Note("preconditions", "")
		}
	}
	if current == nil {
		return nil
	}
	return current.Err(name, file)
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
