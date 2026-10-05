package ydbpool

import (
	"strconv"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/internal/sqlident"
	"ptah.run/internal/ydbtype"
)

// setting is one WITH option of a pool and its value, nil when unset.
type setting struct {
	option   string
	integer  *int32
	fraction *float64
}

// settingsOf lists spec's options in the order a statement names them.
func settingsOf(spec ast.ResourcePoolSpec) []setting {
	return []setting{
		{option: "CONCURRENT_QUERY_LIMIT", integer: spec.ConcurrentQueryLimit},
		{option: "QUEUE_SIZE", integer: spec.QueueSize},
		{option: "DATABASE_LOAD_CPU_THRESHOLD", fraction: spec.DatabaseLoadCPUThreshold},
		{option: "QUERY_MEMORY_LIMIT_PERCENT_PER_NODE", fraction: spec.QueryMemoryLimitPercentPerNode},
		{option: "QUERY_CPU_LIMIT_PERCENT_PER_NODE", fraction: spec.QueryCPULimitPercentPerNode},
		{option: "TOTAL_CPU_LIMIT_PERCENT_PER_NODE", fraction: spec.TotalCPULimitPercentPerNode},
		{option: "RESOURCE_WEIGHT", fraction: spec.ResourceWeight},
	}
}

// set reports whether the setting holds a value.
func (s setting) set() bool { return s.integer != nil || s.fraction != nil }

// literal writes the setting's value as YQL takes it. A WITH option of a pool
// takes an integer or a string literal and nothing else (`value should be a
// string literal or integer`, and a parse error on 25.1.4.7 for `80.5`), and
// the string `"80.5"` is read as the number, measured on every line: so a
// whole number is written bare and a fraction as a string.
func (s setting) literal() string {
	if s.integer != nil {
		return strconv.FormatInt(int64(*s.integer), 10)
	}
	text := strconv.FormatFloat(*s.fraction, 'f', -1, 64)
	if strings.ContainsAny(text, ".eE") {
		return ydbtype.StringLiteral(text)
	}
	return text
}

// equal reports whether two settings of one option hold the same value.
func (s setting) equal(other setting) bool {
	switch {
	case s.set() != other.set():
		return false
	case !s.set():
		return true
	case s.integer != nil:
		return other.integer != nil && *s.integer == *other.integer
	default:
		return other.fraction != nil && *s.fraction == *other.fraction
	}
}

// PoolsEqual reports whether two pool specs hold the same settings.
func PoolsEqual(desired, current ast.ResourcePoolSpec) bool {
	left, right := settingsOf(desired), settingsOf(current)
	for i := range left {
		if !left[i].equal(right[i]) {
			return false
		}
	}
	return true
}

// ClassifiersEqual reports whether two classifier specs are the same.
func ClassifiersEqual(desired, current ast.ResourcePoolClassifierSpec) bool {
	return desired == current
}

// unsetLimit is how a pool with no setting is created. `CREATE RESOURCE POOL`
// needs a WITH clause and an option in it (`mismatched input '<EOF>' expecting
// WITH`), YQL writes no negative number in one (`-1` is a parse error), and
// the string `"-1"` is read as -1, YDB's own spelling of no limit: measured on
// 25.1.4.7 and 26.2.1.14, it reads back as -1 like an option never named.
const unsetLimit = `CONCURRENT_QUERY_LIMIT = "-1"`

// CreatePoolStatement is the CREATE RESOURCE POOL that creates the pool name
// with spec. A setting spec leaves unset is not named.
func CreatePoolStatement(name string, spec ast.ResourcePoolSpec) string {
	var assignments []string
	for _, s := range settingsOf(spec) {
		if s.set() {
			assignments = append(assignments, s.option+" = "+s.literal())
		}
	}
	if len(assignments) == 0 {
		assignments = append(assignments, unsetLimit)
	}
	return "CREATE RESOURCE POOL " + sqlident.Quote(platform.YDB, name) +
		" WITH (" + strings.Join(assignments, ", ") + ");"
}

// AlterPoolStatement is the ALTER RESOURCE POOL that moves the pool name from
// current to desired, or "" when they hold the same settings.
//
// It sets each setting desired holds that differs and resets each one only
// current holds, in one statement: YDB checks the pool a statement leaves
// behind as a whole, so a queue and the limit it waits for change together,
// and it refuses a statement that sets and resets one option (`Duplicate reset
// feature`). Setting one option changes no other: measured on every line from
// 25.1.4.7 to 26.2.1.14, `SET (CONCURRENT_QUERY_LIMIT = 20)` kept the queue,
// the threshold and the percentages, and `RESET (QUEUE_SIZE)` returned the
// queue alone to -1.
func AlterPoolStatement(name string, desired, current ast.ResourcePoolSpec) string {
	wanted, held := settingsOf(desired), settingsOf(current)
	var sets, resets []string
	for i := range wanted {
		switch {
		case wanted[i].equal(held[i]):
		case wanted[i].set():
			sets = append(sets, wanted[i].option+" = "+wanted[i].literal())
		default:
			resets = append(resets, wanted[i].option)
		}
	}
	var clauses []string
	if len(sets) > 0 {
		clauses = append(clauses, "SET ("+strings.Join(sets, ", ")+")")
	}
	if len(resets) > 0 {
		clauses = append(clauses, "RESET ("+strings.Join(resets, ", ")+")")
	}
	if len(clauses) == 0 {
		return ""
	}
	return "ALTER RESOURCE POOL " + sqlident.Quote(platform.YDB, name) + " " + strings.Join(clauses, ", ") + ";"
}

// DropPoolStatement is the DROP RESOURCE POOL that drops the pool name. YDB
// has no IF EXISTS here (`mismatched input 'EXISTS'`).
func DropPoolStatement(name string) string {
	return "DROP RESOURCE POOL " + sqlident.Quote(platform.YDB, name) + ";"
}

// CreateClassifierStatement is the CREATE RESOURCE POOL CLASSIFIER that
// creates the classifier name with spec. The pool and the member are string
// literals, as YDB takes them (`RESOURCE_POOL value should be a string literal
// or integer`), and a classifier without a member names none.
func CreateClassifierStatement(name string, spec ast.ResourcePoolClassifierSpec) string {
	assignments := []string{
		"RESOURCE_POOL = " + ydbtype.StringLiteral(spec.ResourcePool),
		"RANK = " + strconv.FormatInt(spec.Rank, 10),
	}
	if spec.MemberName != "" {
		assignments = append(assignments, "MEMBER_NAME = "+ydbtype.StringLiteral(spec.MemberName))
	}
	return "CREATE RESOURCE POOL CLASSIFIER " + sqlident.Quote(platform.YDB, name) +
		" WITH (" + strings.Join(assignments, ", ") + ");"
}

// AlterClassifierStatement is the ALTER RESOURCE POOL CLASSIFIER that moves
// the classifier name from current to desired, or "" when they are the same.
//
// It names the pool and the rank whether or not they change, and the member,
// or `RESET (MEMBER_NAME)` when desired stops naming one, so the statement
// says the whole classifier. Measured on every line from 25.1.4.7 to 26.2.1.14: a SET
// naming the rank the classifier holds is accepted, while one naming the rank
// of another classifier is refused (`Classifier with rank 20 already exists`),
// which is the planner's to order around; and the pool cannot be reset
// (`Cannot reset required property resource_pool`).
func AlterClassifierStatement(name string, desired, current ast.ResourcePoolClassifierSpec) string {
	if ClassifiersEqual(desired, current) {
		return ""
	}
	sets := []string{
		"RESOURCE_POOL = " + ydbtype.StringLiteral(desired.ResourcePool),
		"RANK = " + strconv.FormatInt(desired.Rank, 10),
	}
	if desired.MemberName != "" {
		sets = append(sets, "MEMBER_NAME = "+ydbtype.StringLiteral(desired.MemberName))
	}
	statement := "ALTER RESOURCE POOL CLASSIFIER " + sqlident.Quote(platform.YDB, name) +
		" SET (" + strings.Join(sets, ", ") + ")"
	if desired.MemberName == "" && current.MemberName != "" {
		statement += ", RESET (MEMBER_NAME)"
	}
	return statement + ";"
}

// DropClassifierStatement is the DROP RESOURCE POOL CLASSIFIER that drops the
// classifier name.
func DropClassifierStatement(name string) string {
	return "DROP RESOURCE POOL CLASSIFIER " + sqlident.Quote(platform.YDB, name) + ";"
}
