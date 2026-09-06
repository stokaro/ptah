package lint_test

import (
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/lint"
	"go.5x5.cz/ptah/migration/migrationfile"
)

// memberFS is the fixture every case in this file analyzes: a table with one
// list-typed column, then one migration that changes the list. What the list
// was is in file 1, and nothing in file 2 can tell an analyzer that, which is
// why the rules read the schema state the version starts from.
func memberFS(alter string) map[string]string {
	return map[string]string{
		"1_base.sql":   "CREATE TABLE orders (id int NOT NULL, status ENUM('new','paid','shipped'));",
		"2_change.sql": alter,
	}
}

// memberBaseline is the state version 2 starts from, with the column's type
// spelled the way the dev-database read reports it.
func memberBaseline(columnType string) []lint.BaselineColumn {
	return []lint.BaselineColumn{{
		Version:    2,
		Table:      "orders",
		Name:       "status",
		ColumnType: columnType,
	}}
}

func memberOptions(baseline []lint.BaselineColumn) lint.Options {
	return lint.Options{
		Dialect:   "mysql",
		DirFormat: migrationfile.DirFormatAtlas,
		Selection: lint.VersionSelection{Versions: []int64{2}, Restricted: true},
		Baseline:  baseline,
	}
}

func analyzeMemberChange(c *qt.C, alter, columnType string) lint.Analysis {
	c.Helper()
	var baseline []lint.BaselineColumn
	if columnType != "" {
		baseline = memberBaseline(columnType)
	}
	analysis, err := lint.AnalyzeFS(fixture(memberFS(alter)), memberOptions(baseline))
	c.Assert(err, qt.IsNil)
	return analysis
}

// members spells n members the way a generated list would, so a boundary
// case can be written as a count rather than as a wall of literals.
func members(n int) string {
	values := make([]string, 0, n)
	for i := range n {
		values = append(values, fmt.Sprintf("'m%03d'", i))
	}
	return strings.Join(values, ",")
}

// memberCodes keeps the codes of this family, so a quiet case asserts the
// analysis said nothing rather than that the generic rules fell silent: an
// append-only MODIFY is still a MODIFY, and DS103 and MY101 still describe
// the clauses beside the list that this file does not judge.
func memberCodes(codes []string) []string {
	var kept []string
	for _, code := range codes {
		if strings.HasPrefix(code, "MY11") || strings.HasPrefix(code, "MY12") {
			kept = append(kept, code)
		}
	}
	return kept
}

func messageOf(findings []lint.Finding, rule string) string {
	for _, finding := range findings {
		if finding.Rule == rule {
			return finding.Message
		}
	}
	return ""
}

// TestMemberRules_ReportWhatTheMigrationDid pins one finding per fact, each
// naming the member it is about and the cost the server charges. The
// expectations are the measured semantics recorded in members.go, not a
// reading of the manual.
func TestMemberRules_ReportWhatTheMigrationDid(t *testing.T) {
	tests := []struct {
		name       string
		alter      string
		columnType string
		want       []string
		message    string
	}{
		{
			name:       "enum member removed",
			alter:      "ALTER TABLE orders MODIFY COLUMN status ENUM('new','paid');",
			columnType: "enum('new','paid','shipped')",
			want:       []string{"MY110"},
			message:    "removes ENUM member 'shipped' from orders.status, keeping 2 of 3",
		},
		{
			name:       "enum members reordered",
			alter:      "ALTER TABLE orders MODIFY COLUMN status ENUM('paid','new','shipped');",
			columnType: "enum('new','paid','shipped')",
			want:       []string{"MY111"},
			message:    "reorders the ENUM members of orders.status ('new', 'paid', 'shipped' becomes 'paid', 'new', 'shipped')",
		},
		{
			name:       "enum member inserted before the end",
			alter:      "ALTER TABLE orders MODIFY COLUMN status ENUM('new','held','paid','shipped');",
			columnType: "enum('new','paid','shipped')",
			want:       []string{"MY112"},
			message:    "inserts ENUM member 'held' into orders.status ahead of existing members",
		},
		{
			name:       "enum grows across 255 members",
			alter:      "ALTER TABLE orders MODIFY COLUMN status ENUM(" + members(256) + ");",
			columnType: "enum(" + members(255) + ")",
			want:       []string{"MY113"},
			message:    "takes orders.status from 255 to 256 members, across 255 members, where an ENUM value grows from one byte to two (1 to 2 bytes per value)",
		},
		{
			name:       "set member removed",
			alter:      "ALTER TABLE orders MODIFY COLUMN status SET('a','b');",
			columnType: "set('a','b','c')",
			want:       []string{"MY120"},
			message:    "removes SET member 'c' from orders.status",
		},
		{
			name:       "set members reordered",
			alter:      "ALTER TABLE orders MODIFY COLUMN status SET('a','c','b');",
			columnType: "set('a','b','c')",
			want:       []string{"MY121"},
			message:    "reorders the SET members of orders.status",
		},
		{
			name:       "set member inserted before the end",
			alter:      "ALTER TABLE orders MODIFY COLUMN status SET('a','x','b','c');",
			columnType: "set('a','b','c')",
			want:       []string{"MY122"},
			message:    "inserts SET member 'x' into orders.status ahead of existing members",
		},
		{
			name:       "set grows across eight members",
			alter:      "ALTER TABLE orders MODIFY COLUMN status SET(" + members(9) + ");",
			columnType: "set(" + members(8) + ")",
			want:       []string{"MY123"},
			message:    "takes orders.status from 8 to 9 members, across a multiple of eight members, where a SET value grows by a byte (1 to 2 bytes per value)",
		},
		{
			name:       "several facts in one clause are several findings",
			alter:      "ALTER TABLE orders MODIFY COLUMN status ENUM('held','shipped','new');",
			columnType: "enum('new','paid','shipped')",
			want:       []string{"MY110", "MY111", "MY112"},
			message:    "removes ENUM member 'paid'",
		},
		{
			name:       "CHANGE looks the column up by its old name",
			alter:      "ALTER TABLE orders CHANGE COLUMN status state ENUM('new','paid');",
			columnType: "enum('new','paid','shipped')",
			want:       []string{"MY110"},
			message:    "CHANGE COLUMN removes ENUM member 'shipped' from orders.state",
		},
		{
			name:       "the COLUMN keyword is optional",
			alter:      "ALTER TABLE orders MODIFY status ENUM('new','paid');",
			columnType: "enum('new','paid','shipped')",
			want:       []string{"MY110"},
			message:    "removes ENUM member 'shipped'",
		},
		{
			name:       "a quote inside a member is one value in both spellings",
			alter:      "ALTER TABLE orders MODIFY COLUMN status ENUM('new','can''t');",
			columnType: "enum('new','can''t','shipped')",
			want:       []string{"MY110"},
			message:    "removes ENUM member 'shipped'",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzeMemberChange(c, test.alter, test.columnType)

			findings := analysis.Findings()
			c.Assert(rulesOf(findings), qt.DeepEquals, test.want)
			c.Assert(messageOf(findings, test.want[0]), qt.Contains, test.message)
			c.Assert(messageOf(findings, test.want[0]), qt.Contains, "copies the whole table")
			c.Assert(analysis.UnmetInputs(), qt.HasLen, 0)
		})
	}
}

// TestMemberRules_StayQuietForChangesTheServerAppliesInPlace is the other
// half of every rule above: the change beside each hazard that the server
// performs without a copy, so a rule cannot be satisfied by firing on every
// MODIFY of a list-typed column.
func TestMemberRules_StayQuietForChangesTheServerAppliesInPlace(t *testing.T) {
	tests := []struct {
		name       string
		alter      string
		columnType string
	}{
		{
			name:       "enum member appended at the end",
			alter:      "ALTER TABLE orders MODIFY COLUMN status ENUM('new','paid','shipped','returned');",
			columnType: "enum('new','paid','shipped')",
		},
		{
			name:       "enum list unchanged",
			alter:      "ALTER TABLE orders MODIFY COLUMN status ENUM('new','paid','shipped') NOT NULL;",
			columnType: "enum('new','paid','shipped')",
		},
		{
			name:       "enum grows to 255 members without crossing the boundary",
			alter:      "ALTER TABLE orders MODIFY COLUMN status ENUM(" + members(254) + ");",
			columnType: "enum(" + members(253) + ")",
		},
		{
			name:       "set member appended at the end",
			alter:      "ALTER TABLE orders MODIFY COLUMN status SET('a','b','c','d');",
			columnType: "set('a','b','c')",
		},
		{
			name:       "set grows to a boundary without crossing it",
			alter:      "ALTER TABLE orders MODIFY COLUMN status SET(" + members(8) + ");",
			columnType: "set(" + members(7) + ")",
		},
		{
			name:       "trailing spaces the server trims are not a removal",
			alter:      "ALTER TABLE orders MODIFY COLUMN status ENUM('new ','paid','shipped');",
			columnType: "enum('new','paid','shipped')",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzeMemberChange(c, test.alter, test.columnType)

			c.Assert(memberCodes(rulesOf(analysis.Findings())), qt.HasLen, 0)
			c.Assert(analysis.UnmetInputs(), qt.HasLen, 0)
		})
	}
}

// TestMemberRules_StorageBoundaries pins every boundary independently, on
// both sides, because each one was measured separately and a formula that
// got one of them wrong would still pass a test written at the first.
func TestMemberRules_StorageBoundaries(t *testing.T) {
	tests := []struct {
		name string
		kind string
		from int
		to   int
		want []string
	}{
		{name: "enum 253 to 254", kind: "ENUM", from: 253, to: 254},
		{name: "enum 254 to 255", kind: "ENUM", from: 254, to: 255},
		{name: "enum 255 to 256", kind: "ENUM", from: 255, to: 256, want: []string{"MY113"}},
		{name: "set 7 to 8", kind: "SET", from: 7, to: 8},
		{name: "set 8 to 9", kind: "SET", from: 8, to: 9, want: []string{"MY123"}},
		{name: "set 15 to 16", kind: "SET", from: 15, to: 16},
		{name: "set 16 to 17", kind: "SET", from: 16, to: 17, want: []string{"MY123"}},
		{name: "set 23 to 24", kind: "SET", from: 23, to: 24},
		{name: "set 24 to 25", kind: "SET", from: 24, to: 25, want: []string{"MY123"}},
		{name: "set 31 to 32", kind: "SET", from: 31, to: 32},
		{name: "set 32 to 33", kind: "SET", from: 32, to: 33, want: []string{"MY123"}},
		{name: "set 63 to 64", kind: "SET", from: 63, to: 64},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			alter := fmt.Sprintf("ALTER TABLE orders MODIFY COLUMN status %s(%s);", test.kind, members(test.to))
			columnType := strings.ToLower(test.kind) + "(" + members(test.from) + ")"
			analysis := analyzeMemberChange(c, alter, columnType)

			c.Assert(memberCodes(rulesOf(analysis.Findings())), qt.DeepEquals, test.want)
		})
	}
}

// TestMemberRules_NameTheirInputWhenTheRunSuppliesNone pins the contract the
// issue asked for: without the starting state the rules find nothing, the
// generic rules still report the statement, and the run says which rules
// could have said more rather than reporting a clean pass.
func TestMemberRules_NameTheirInputWhenTheRunSuppliesNone(t *testing.T) {
	c := qt.New(t)

	analysis := analyzeMemberChange(c, "ALTER TABLE orders MODIFY COLUMN status ENUM('new','paid');", "")

	c.Assert(rulesOf(analysis.Findings()), qt.DeepEquals, []string{"DS103", "MY101"})
	unmet := unmetRules(analysis.UnmetInputs())
	c.Assert(unmet, qt.Contains, "MY110")
	c.Assert(unmet, qt.Contains, "MY123")
	c.Assert(analysis.BaselineVersions(), qt.DeepEquals, []int64{2})
}

// TestMemberRules_SubsumeTheGenericFindings pins the noise policy: once a
// rule has said which member moved and what the server does about it, the
// generic type-change and lock-heavy findings on that statement are not
// repeated. The control above -- no baseline, both generic rules report --
// is what proves the policy is keyed to the specific finding firing rather
// than to the statement's shape.
func TestMemberRules_SubsumeTheGenericFindings(t *testing.T) {
	c := qt.New(t)

	analysis := analyzeMemberChange(c,
		"ALTER TABLE orders MODIFY COLUMN status ENUM('new','paid');",
		"enum('new','paid','shipped')",
	)

	findings := analysis.Findings()
	c.Assert(rulesOf(findings), qt.DeepEquals, []string{"MY110"})
	c.Assert(findings[0].Context, qt.IsNotNil)
	c.Assert(findings[0].Context.Subjects, qt.DeepEquals, []lint.Subject{{
		Kind:     lint.SubjectColumn,
		Name:     "status",
		Parent:   "orders",
		DataType: "enum",
	}})
	c.Assert(findings[0].Line, qt.Equals, 1)
	c.Assert(findings[0].Severity, qt.Equals, lint.SeverityWarning)
}

// TestMemberRules_LeaveTypeChangesToTheGenericRule keeps the family to the
// question it can answer. A column becoming a list, or a list becoming the
// other kind of list, is a type change with no member transition to
// describe, and DS103 already reports it.
func TestMemberRules_LeaveTypeChangesToTheGenericRule(t *testing.T) {
	tests := []struct {
		name       string
		alter      string
		columnType string
	}{
		{
			name:       "varchar becomes enum",
			alter:      "ALTER TABLE orders MODIFY COLUMN status ENUM('new','paid');",
			columnType: "varchar(32)",
		},
		{
			name:       "enum becomes set",
			alter:      "ALTER TABLE orders MODIFY COLUMN status SET('new','paid');",
			columnType: "enum('new','paid','shipped')",
		},
		{
			name:       "enum becomes varchar",
			alter:      "ALTER TABLE orders MODIFY COLUMN status VARCHAR(32);",
			columnType: "enum('new','paid','shipped')",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzeMemberChange(c, test.alter, test.columnType)

			c.Assert(rulesOf(analysis.Findings()), qt.DeepEquals, []string{"DS103", "MY101"})
		})
	}
}

// TestMemberRules_RunOnlyForTheMySQLFamily pins the dialect gate: the
// semantics are those two servers', and a PostgreSQL run must not read a
// MODIFY it will never execute.
func TestMemberRules_RunOnlyForTheMySQLFamily(t *testing.T) {
	c := qt.New(t)
	opts := memberOptions(memberBaseline("enum('new','paid','shipped')"))
	opts.Dialect = "postgres"

	analysis, err := lint.AnalyzeFS(fixture(memberFS("ALTER TABLE orders MODIFY COLUMN status ENUM('new','paid');")), opts)

	c.Assert(err, qt.IsNil)
	c.Assert(rulesOf(analysis.Findings()), qt.Not(qt.Contains), "MY110")
	c.Assert(unmetRules(analysis.UnmetInputs()), qt.Not(qt.Contains), "MY110")
}
