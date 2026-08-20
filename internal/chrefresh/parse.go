package chrefresh

import (
	"strings"

	"go.5x5.cz/ptah/core/ast"
)

// ParseClause reads a schedule out of the text between REFRESH and the column
// list of a stored CREATE MATERIALIZED VIEW statement.
//
// The input is what the server stored, so the intervals are already canonical
// and the dependencies already schema-qualified; this reads them rather than
// normalizing them again. A clause it cannot read returns nil, which reports
// the view as one with no schedule -- see [ParseCreateQuery] for why that is
// the safe direction.
//
// The clause order is fixed, measured:
//
//	EVERY <interval> [OFFSET <interval>] [RANDOMIZE FOR <interval>] [DEPENDS ON <view>...] [APPEND]
func ParseClause(clause string) *ast.MatViewRefreshSpec {
	fields := strings.Fields(clause)
	if len(fields) < 2 {
		return nil
	}
	mode := strings.ToUpper(fields[0])
	if mode != ModeEvery && mode != ModeAfter {
		return nil
	}

	spec := &ast.MatViewRefreshSpec{Mode: mode}
	rest := fields[1:]

	// APPEND is last, so it is taken off the end before the clauses in the
	// middle are split.
	if len(rest) > 0 && strings.EqualFold(rest[len(rest)-1], "APPEND") {
		spec.Append = true
		rest = rest[:len(rest)-1]
	}

	spec.Interval, rest = takeInterval(rest)
	if spec.Interval == "" {
		return nil
	}
	for len(rest) > 0 {
		switch {
		case strings.EqualFold(rest[0], "OFFSET"):
			spec.Offset, rest = takeInterval(rest[1:])
		case strings.EqualFold(rest[0], "RANDOMIZE") && len(rest) > 1 && strings.EqualFold(rest[1], "FOR"):
			spec.Randomize, rest = takeInterval(rest[2:])
		case strings.EqualFold(rest[0], "DEPENDS") && len(rest) > 1 && strings.EqualFold(rest[1], "ON"):
			spec.DependsOn = splitDependencies(rest[2:])
			rest = nil
		default:
			// A clause this reader does not know. Reporting a partial schedule
			// would be worse than reporting none: a comparison against it would
			// plan a change to make the view match something it already is.
			return nil
		}
	}
	return spec
}

// takeInterval consumes the leading `<count> <unit>` terms and returns them
// with the rest of the fields.
func takeInterval(fields []string) (string, []string) {
	var terms []string
	for len(fields) >= 2 && isCount(fields[0]) {
		if _, ok := lookupUnit(fields[1]); !ok {
			break
		}
		terms = append(terms, fields[0], strings.ToUpper(fields[1]))
		fields = fields[2:]
	}
	return strings.Join(terms, " "), fields
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

// splitDependencies reads the comma-separated view list DEPENDS ON carries.
func splitDependencies(fields []string) []string {
	joined := strings.Join(fields, " ")
	var dependencies []string
	for part := range strings.SplitSeq(joined, ",") {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			dependencies = append(dependencies, trimmed)
		}
	}
	return dependencies
}

// ParseCreateQuery reads the schedule out of a stored create_table_query.
//
// The schedule survives nowhere else: system.tables.as_select is byte-identical
// for a plain view and a refreshable one, and system.view_refreshes carries the
// refresh STATE without the rules. So the statement text is the only source,
// and this reads the REFRESH clause between the view name and the column list
// the server always prints.
//
// It returns nil when the statement carries no schedule, which is the ordinary
// materialized view. A caller must not read that as "a refreshable view with an
// empty schedule": those are different objects, and only one of them can be
// altered into the other.
func ParseCreateQuery(createQuery string) *ast.MatViewRefreshSpec {
	const marker = " REFRESH "
	_, rest, found := strings.Cut(createQuery, marker)
	if !found {
		return nil
	}
	// The column list opens the rest of the statement, and the server always
	// prints one for a materialized view that owns its storage.
	if end := strings.Index(rest, " ("); end >= 0 {
		rest = rest[:end]
	}
	return ParseClause(rest)
}
