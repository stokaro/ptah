// Package directiveplacement carries the one table of positions a migration
// file directive can occupy, so every directive in the class is measured
// against the same seven shapes.
//
// It is a package rather than a fixture repeated per test file because the
// class spans both test packages of migration/migrator: the exported half of
// the class (transaction mode, online-DDL routing, checks) is measured through
// the public API, and the half with no exported observable (the timeout keys,
// `-- atlas:checkpoint`, `-- atlas:assert oneof`) is measured from inside. A
// Go external test package cannot see an internal one's identifiers, so two
// copies of the table would be the only alternative -- and two descriptions of
// one rule drifting apart is the defect this class exists to close.
package directiveplacement

// Statement is the single executable statement every rendered fixture carries.
// One statement is enough: the question is where a directive sits relative to
// executable SQL, not what the SQL does.
const Statement = "CREATE TABLE t (id INTEGER PRIMARY KEY);\n"

// Placement renders one directive line at one position relative to [Statement].
type Placement struct {
	// Name identifies the position and keys the per-directive answer sets.
	Name string
	// Render returns a complete migration file with directive at this position.
	Render func(directive string) string
}

// All is the position table.
//
// The seven shapes are the ones that separate the two directive families'
// rules. The pinned community binary was measured on every one of them with
// `-- atlas:txmode none` under `migrate apply --tx-mode all`: it honors the
// first two and drops the last five, which is what [InsideAtlasHeaderBlock]
// records.
var All = []Placement{
	{
		Name:   "first line",
		Render: func(d string) string { return d + "\n\n" + Statement },
	},
	{
		Name:   "second comment line",
		Render: func(d string) string { return "-- create the table\n" + d + "\n\n" + Statement },
	},
	{
		Name:   "after a leading blank line",
		Render: func(d string) string { return "\n" + d + "\n\n" + Statement },
	},
	{
		Name:   "indented",
		Render: func(d string) string { return "  " + d + "\n\n" + Statement },
	},
	{
		Name:   "after a blank line inside the header",
		Render: func(d string) string { return "-- create the table\n\n" + d + "\n" + Statement },
	},
	{
		Name:   "after the statement",
		Render: func(d string) string { return Statement + d + "\n" },
	},
	{
		Name: "trailing comment on the statement line",
		Render: func(d string) string {
			return "CREATE TABLE t (id INTEGER PRIMARY KEY); " + d + "\n"
		},
	},
}

// Everywhere is the answer set for a directive whose meaning does not depend on
// where it sits.
func Everywhere(honored bool) map[string]bool {
	answers := make(map[string]bool, len(All))
	for _, placement := range All {
		answers[placement.Name] = honored
	}
	return answers
}

// BeforeTheStatement is the rule both families share: a directive is
// significant anywhere in the run of blank lines and line comments that
// precedes the first executable statement, and nowhere after it. A trailing
// comment carries no directive in either family, which is documented rather
// than accidental.
func BeforeTheStatement() map[string]bool {
	answers := Everywhere(true)
	answers["after the statement"] = false
	answers["trailing comment on the statement line"] = false
	return answers
}

// InsideAtlasHeaderBlock is the `atlas:` family's stricter acceptance inside
// that shared region: Atlas reads only the unbroken run of line comments that
// starts at byte 0, each beginning in column 0.
//
// Measured on the pinned community binary with `migrate apply --tx-mode all`
// over one-statement SQLite directories, exit codes read directly from unpiped
// invocations: 1 for the two true rows, 0 for the five false ones.
func InsideAtlasHeaderBlock() map[string]bool {
	answers := Everywhere(false)
	answers["first line"] = true
	answers["second comment line"] = true
	return answers
}

// EveryLineComment is the answer set for a directive whose position never
// decides anything, on every shape that is a directive carrier at all. The
// trailing comment is excluded because neither family reads one as a directive
// -- that is the boundary of "a directive was written here", not a position
// rule.
func EveryLineComment() map[string]bool {
	answers := Everywhere(true)
	answers["trailing comment on the statement line"] = false
	return answers
}

// OnlyTheFirstLine is the `-- atlas:checkpoint` rule: nothing may precede it,
// not even a comment. Leading whitespace on that line is accepted, because the
// directive is compared against the trimmed first line.
func OnlyTheFirstLine() map[string]bool {
	answers := Everywhere(false)
	answers["first line"] = true
	answers["indented"] = true
	return answers
}
