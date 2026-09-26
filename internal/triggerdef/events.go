package triggerdef

import (
	"slices"
	"strings"
)

// eventOrder is the order PostgreSQL reports a trigger's events in, whatever
// order they were declared in: pg_get_triggerdef writes
// `BEFORE DELETE OR UPDATE OF b, a` for a trigger created
// `BEFORE UPDATE OF b, a OR DELETE`, measured on PostgreSQL 18.6.
var eventOrder = map[string]int{"INSERT": 0, "DELETE": 1, "UPDATE": 2, "TRUNCATE": 3}

// Event is one member of a trigger's event list.
type Event struct {
	// Keyword is the member in upper case: INSERT, UPDATE, DELETE or TRUNCATE,
	// or the member's whole text when it is none of them.
	Keyword string
	// Columns are the columns of an UPDATE OF member, each as written, quotes
	// included.
	Columns []string
}

// String spells the member the way a CREATE TRIGGER statement does.
func (e Event) String() string {
	if len(e.Columns) == 0 {
		return e.Keyword
	}
	return e.Keyword + " OF " + strings.Join(e.Columns, ", ")
}

// Events splits an event list such as `update of b, "A" or insert` at each OR
// outside a quoted identifier. A list with no text has no members.
func Events(list string) []Event {
	var events []Event
	var member []string
	for _, token := range tokens(list) {
		if strings.EqualFold(token, "OR") {
			events = append(events, event(member))
			member = nil
			continue
		}
		member = append(member, token)
	}
	if len(member) == 0 && len(events) == 0 {
		return nil
	}
	return append(events, event(member))
}

// Includes reports whether events has a member naming keyword, such as
// TRUNCATE.
func Includes(events []Event, keyword string) bool {
	return slices.ContainsFunc(events, func(e Event) bool { return e.Keyword == keyword })
}

// NamesColumns reports whether a member of events is an UPDATE OF a column
// list.
func NamesColumns(events []Event) bool {
	return slices.ContainsFunc(events, func(e Event) bool { return len(e.Columns) > 0 })
}

// Canonical spells an event list one way: each keyword in upper case, the
// members in the order PostgreSQL reports them and joined by " OR ", and an
// UPDATE's columns joined by ", " in the order declared, which is the order
// the server keeps. Each column passes through column, or stays as written
// when column is nil. A member that names no known event keeps its place
// after the ones that do.
func Canonical(list string, column func(string) string) string {
	events := Events(list)
	members := make([]string, len(events))
	slices.SortStableFunc(events, func(a, b Event) int {
		return rank(a) - rank(b)
	})
	for i, e := range events {
		if column != nil {
			e.Columns = slices.Clone(e.Columns)
			for j, name := range e.Columns {
				e.Columns[j] = column(name)
			}
		}
		members[i] = e.String()
	}
	return strings.Join(members, " OR ")
}

func event(tokens []string) Event {
	if len(tokens) >= 2 && strings.EqualFold(tokens[0], "UPDATE") && strings.EqualFold(tokens[1], "OF") {
		var columns []string
		for _, token := range tokens[2:] {
			if token != "," {
				columns = append(columns, token)
			}
		}
		return Event{Keyword: "UPDATE", Columns: columns}
	}
	var text strings.Builder
	for _, token := range tokens {
		if token != "," && text.Len() > 0 {
			text.WriteByte(' ')
		}
		text.WriteString(token)
	}
	return Event{Keyword: strings.ToUpper(text.String())}
}

func rank(e Event) int {
	if order, known := eventOrder[e.Keyword]; known {
		return order
	}
	return len(eventOrder)
}

// tokens splits text into words and commas. A double-quoted identifier is one
// word, spaces, commas and doubled quotes included.
func tokens(text string) []string {
	var words []string
	var word strings.Builder
	flush := func() {
		if word.Len() > 0 {
			words = append(words, word.String())
			word.Reset()
		}
	}
	inQuote := false
	for i := 0; i < len(text); i++ {
		c := text[i]
		switch {
		case inQuote:
			word.WriteByte(c)
			if c == '"' {
				inQuote = false
			}
		case c == '"':
			word.WriteByte(c)
			inQuote = true
		case c == ',':
			flush()
			words = append(words, ",")
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			flush()
		default:
			word.WriteByte(c)
		}
	}
	flush()
	return words
}
