package chrefresh

import (
	"errors"
	"fmt"
	"strings"

	"ptah.run/dialect/clickhouse/chschema"
)

// The refresh clause, as ClickHouse prints it between REFRESH and the column
// list of a stored CREATE MATERIALIZED VIEW, measured on 24.10.4.191 and
// 26.9.14.10:
//
//	EVERY|AFTER <interval> [OFFSET <interval>] [RANDOMIZE FOR <interval>]
//	[DEPENDS ON <view> [, <view> ...]] [SETTINGS <name> = <value> [, ...]]
//	[APPEND] [TO <table>]
//
// Each clause appears at most once and in that order. TO names where a view
// writes its rows, which is not part of its schedule.

// errUnmodeled marks a stored clause this reader recognizes and does not
// model, such as refresh SETTINGS. A schedule read without it would plan a
// change that drops it.
var errUnmodeled = errors.New("not modeled")

// ParseClause reads a declared refresh schedule, such as `every 1 hour offset
// 5 minute`, keywords in either case. Every clause of the schedule grammar is
// accepted once and in the server's order, and each one needs its operand; a
// clause written twice, out of order or without its operand is refused, as
// are SETTINGS and TO, which a declaration cannot state. Intervals keep the
// spelling given; [Canonical] reads them.
func ParseClause(clause string) (*chschema.Schedule, error) {
	schedule, err := parse(clause, false)
	if errors.Is(err, errUnmodeled) {
		return nil, fmt.Errorf("refresh %q: %w", strings.TrimSpace(clause), err)
	}
	return schedule, err
}

// ParseCreateQuery reads the schedule out of a stored create_table_query.
//
// The schedule survives nowhere else: system.tables.as_select is byte-identical
// for a plain view and a refreshable one, and system.view_refreshes carries the
// refresh STATE without the rules. So the statement text is the only source.
//
// It returns nil when the statement carries no schedule, and nil as well when
// the clause cannot be read completely, such as one with refresh SETTINGS. A
// caller tells the two apart through system.view_refreshes and must report the
// second as a schedule it could not read, never as a plain view: those are
// different objects, and only one of them can be altered into the other.
func ParseCreateQuery(createQuery string) *chschema.Schedule {
	const marker = " REFRESH "
	_, rest, found := strings.Cut(createQuery, marker)
	if !found {
		return nil
	}
	// What follows the clause: the column list the server prints, or, before
	// it, the storage and security clauses of a statement printed without one.
	end := len(rest)
	for _, next := range []string{" (", " ENGINE ", " EMPTY", " DEFINER ", " SQL SECURITY ", " AS "} {
		if at := strings.Index(rest, next); at >= 0 && at < end {
			end = at
		}
	}
	schedule, err := parse(rest[:end], true)
	if err != nil {
		return nil
	}
	return schedule
}

// clauseStage orders the optional clauses; each must come after the previous.
type clauseStage int

const (
	stageInterval clauseStage = iota
	stageOffset
	stageRandomize
	stageDepends
	stageSettings
	stageAppend
	stageTo
)

type clauseParser struct {
	tokens []string
	stored bool
	stage  clauseStage
}

func parse(clause string, stored bool) (*chschema.Schedule, error) {
	tokens, err := tokenize(clause)
	if err != nil {
		return nil, err
	}
	if len(tokens) == 0 {
		return nil, fmt.Errorf("refresh clause is empty; expected %s or %s followed by an interval", chschema.RefreshEvery, chschema.RefreshAfter)
	}
	mode := strings.ToUpper(tokens[0])
	if mode != chschema.RefreshEvery && mode != chschema.RefreshAfter {
		return nil, fmt.Errorf("refresh clause starts with %q; expected %s or %s", tokens[0], chschema.RefreshEvery, chschema.RefreshAfter)
	}
	p := &clauseParser{tokens: tokens[1:], stored: stored}
	schedule := &chschema.Schedule{Mode: mode}
	if schedule.Interval, err = p.interval(mode); err != nil {
		return nil, err
	}
	for len(p.tokens) > 0 {
		if err := p.clause(schedule); err != nil {
			return nil, err
		}
	}
	return schedule, nil
}

// clause reads the next optional clause into schedule.
func (p *clauseParser) clause(schedule *chschema.Schedule) error {
	var err error
	switch {
	case p.keyword("OFFSET"):
		if err = p.enter(stageOffset, "OFFSET"); err == nil {
			schedule.Offset, err = p.interval("OFFSET")
		}
	case p.keyword("RANDOMIZE", "FOR"):
		if err = p.enter(stageRandomize, "RANDOMIZE FOR"); err == nil {
			schedule.Randomize, err = p.interval("RANDOMIZE FOR")
		}
	case p.keyword("DEPENDS", "ON"):
		if err = p.enter(stageDepends, "DEPENDS ON"); err == nil {
			schedule.DependsOn, err = p.names()
		}
	case p.keyword("SETTINGS"):
		if err = p.enter(stageSettings, "SETTINGS"); err == nil {
			err = p.settings()
		}
	case p.keyword("APPEND"):
		err = p.enter(stageAppend, "APPEND")
		schedule.Append = true
	case p.keyword("TO"):
		if err = p.enter(stageTo, "TO"); err == nil {
			err = p.target()
		}
	default:
		err = fmt.Errorf("refresh clause has %q where a clause was expected", p.tokens[0])
	}
	return err
}

// keyword consumes words when the next tokens are exactly them.
func (p *clauseParser) keyword(words ...string) bool {
	if len(p.tokens) < len(words) {
		return false
	}
	for i, word := range words {
		if !strings.EqualFold(p.tokens[i], word) {
			return false
		}
	}
	p.tokens = p.tokens[len(words):]
	return true
}

// enter moves to stage, refusing a clause repeated or out of the server's
// order.
func (p *clauseParser) enter(stage clauseStage, name string) error {
	if stage <= p.stage {
		return fmt.Errorf("refresh %s is repeated or out of order; the order is OFFSET, RANDOMIZE FOR, DEPENDS ON, SETTINGS, APPEND", name)
	}
	p.stage = stage
	return nil
}

// interval consumes the `<count> <unit>` terms of one interval, which needs at
// least one.
func (p *clauseParser) interval(clause string) (string, error) {
	var terms []string
	for len(p.tokens) >= 2 && isCount(p.tokens[0]) {
		if _, ok := lookupUnit(p.tokens[1]); !ok {
			break
		}
		terms = append(terms, p.tokens[0], strings.ToUpper(p.tokens[1]))
		p.tokens = p.tokens[2:]
	}
	if len(terms) == 0 {
		return "", fmt.Errorf("refresh %s needs an interval such as `1 HOUR`", clause)
	}
	return strings.Join(terms, " "), nil
}

// names consumes the comma-separated views DEPENDS ON lists, which needs at
// least one.
func (p *clauseParser) names() ([]string, error) {
	var names []string
	for {
		if len(p.tokens) == 0 || !isName(p.tokens[0]) {
			return nil, fmt.Errorf("refresh DEPENDS ON needs a view name")
		}
		names = append(names, p.tokens[0])
		p.tokens = p.tokens[1:]
		if len(p.tokens) == 0 || p.tokens[0] != "," {
			return names, nil
		}
		p.tokens = p.tokens[1:]
	}
}

// settings consumes `name = value` pairs. A stored schedule with them is one
// this reader does not model, and a declaration cannot state them.
func (p *clauseParser) settings() error {
	for {
		if len(p.tokens) < 3 || !isName(p.tokens[0]) || p.tokens[1] != "=" || p.tokens[2] == "," {
			return fmt.Errorf("refresh SETTINGS needs `name = value` pairs")
		}
		p.tokens = p.tokens[3:]
		if len(p.tokens) == 0 || p.tokens[0] != "," {
			return fmt.Errorf("refresh SETTINGS is %w", errUnmodeled)
		}
		p.tokens = p.tokens[1:]
	}
}

// target consumes the table TO names, which only a stored statement carries.
func (p *clauseParser) target() error {
	if !p.stored {
		return fmt.Errorf("refresh TO names where a view writes, not when it refreshes")
	}
	if len(p.tokens) != 1 || !isName(p.tokens[0]) {
		return fmt.Errorf("refresh TO needs one table name and nothing after it")
	}
	p.tokens = nil
	return nil
}

// clauseKeywords cannot be view names in a clause, where they open the next
// clause.
var clauseKeywords = map[string]bool{"OFFSET": true, "RANDOMIZE": true, "DEPENDS": true, "SETTINGS": true, "APPEND": true, "TO": true}

func isName(token string) bool {
	return token != "," && token != "=" && !clauseKeywords[strings.ToUpper(token)]
}

func isCount(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// tokenize splits a clause into words, keeping `,` and `=` as tokens of their
// own and a backquoted or single-quoted span, such as db.`a view`, inside the
// word it belongs to.
func tokenize(clause string) ([]string, error) {
	var tokens []string
	var word strings.Builder
	flush := func() {
		if word.Len() > 0 {
			tokens = append(tokens, word.String())
			word.Reset()
		}
	}
	runes := []rune(clause)
	for i := 0; i < len(runes); i++ {
		r := runes[i]
		switch r {
		case '`', '\'':
			end := closingQuote(runes, i)
			if end < 0 {
				return nil, fmt.Errorf("refresh clause has an unterminated %c", r)
			}
			word.WriteString(string(runes[i : end+1]))
			i = end
		case ',', '=':
			flush()
			tokens = append(tokens, string(r))
		case ' ', '\t', '\n', '\r':
			flush()
		default:
			word.WriteRune(r)
		}
	}
	flush()
	return tokens, nil
}

// closingQuote finds the quote closing the one at start; a doubled quote or a
// backslash escapes it.
func closingQuote(runes []rune, start int) int {
	quote := runes[start]
	for i := start + 1; i < len(runes); i++ {
		switch {
		case runes[i] == '\\':
			i++
		case runes[i] == quote && i+1 < len(runes) && runes[i+1] == quote:
			i++
		case runes[i] == quote:
			return i
		}
	}
	return -1
}
