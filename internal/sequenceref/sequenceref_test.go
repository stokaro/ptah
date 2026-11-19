package sequenceref_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/identifier"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/sequenceref"
)

// oracle are the rules Oracle's descriptor states.
func oracle() sequenceref.Rules {
	return sequenceref.Rules{
		Pseudocolumns: []string{"NEXTVAL", "CURRVAL"},
		Fold:          identifier.FoldUpper,
		Lexing:        dialectlexer.Options("oracle"),
	}
}

// TestCanonical_StoredNames reads each spelling of a reference as the names
// the catalog stores, in schema APP. Oracle stores an unqualified reference
// qualified and quoted, and a qualified one as written, so each declared
// spelling and the one Oracle stores for it read alike. The spaces Oracle
// drops around an operator do not count.
func TestCanonical_StoredNames(t *testing.T) {
	rows := []struct {
		name, expression, want string
	}{
		{name: "unqualified", expression: "seq.NEXTVAL", want: `"SEQ"."NEXTVAL"`},
		{name: "stored unqualified", expression: `"APP"."SEQ"."NEXTVAL"`, want: `"SEQ"."NEXTVAL"`},
		{name: "qualified in lower case", expression: "app.seq.nextval", want: `"SEQ"."NEXTVAL"`},
		{name: "another schema", expression: "other.seq.NEXTVAL", want: `"OTHER"."SEQ"."NEXTVAL"`},
		{name: "a quoted sequence", expression: `"Seq".CURRVAL`, want: `"Seq"."CURRVAL"`},
		{name: "stored quoted sequence", expression: `"APP"."Seq"."CURRVAL"`, want: `"Seq"."CURRVAL"`},
		{name: "an operator", expression: "seq.NEXTVAL + 1", want: `"SEQ"."NEXTVAL" + 1`},
		{name: "an operator, stored", expression: `"APP"."SEQ"."NEXTVAL"+1`, want: `"SEQ"."NEXTVAL" + 1`},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			got, found := sequenceref.Canonical(row.expression, oracle(), "APP")

			c.Assert(found, qt.IsTrue)
			c.Assert(got, qt.Equals, row.want)
		})
	}
}

// TestCanonical_NoReference finds no reference in a string literal, a call,
// a name that is no pseudocolumn, a pseudocolumn quoted in another case,
// words written with spaces around the dot, or under rules naming no
// pseudocolumn.
func TestCanonical_NoReference(t *testing.T) {
	rows := []struct {
		name, expression string
		rules            sequenceref.Rules
	}{
		{name: "a string", expression: "'seq.NEXTVAL'", rules: oracle()},
		{name: "a call", expression: "pkg.nextval(1)", rules: oracle()},
		{name: "another name", expression: "seq.value", rules: oracle()},
		{name: "a quoted pseudocolumn in lower case", expression: `seq."nextval"`, rules: oracle()},
		{name: "spaces around the dot", expression: "seq . NEXTVAL", rules: oracle()},
		{name: "a lone pseudocolumn", expression: "NEXTVAL", rules: oracle()},
		{name: "no pseudocolumns", expression: "seq.NEXTVAL", rules: sequenceref.Rules{Lexing: dialectlexer.Options("postgres")}},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			_, found := sequenceref.Canonical(row.expression, row.rules, "APP")

			c.Assert(found, qt.IsFalse)
		})
	}
}

// TestCanonical_DistinctReferences reads references to different sequences
// or pseudocolumns as different text: a quoted name keeps its case, and a
// schema other than the table's stays.
func TestCanonical_DistinctReferences(t *testing.T) {
	rows := []struct {
		name, left, right string
	}{
		{name: "quoted and folded", left: `"Seq".NEXTVAL`, right: "seq.NEXTVAL"},
		{name: "NEXTVAL and CURRVAL", left: "seq.NEXTVAL", right: "seq.CURRVAL"},
		{name: "two schemas", left: "other.seq.NEXTVAL", right: "seq.NEXTVAL"},
		{name: "two operands", left: "seq.NEXTVAL + 1", right: "seq.NEXTVAL + 2"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			left, leftFound := sequenceref.Canonical(row.left, oracle(), "APP")
			right, rightFound := sequenceref.Canonical(row.right, oracle(), "APP")

			c.Assert(leftFound, qt.IsTrue)
			c.Assert(rightFound, qt.IsTrue)
			c.Assert(left, qt.Not(qt.Equals), right)
		})
	}
}

// sqlServer are the rules SQL Server's descriptor states.
func sqlServer() sequenceref.Rules {
	return sequenceref.Rules{
		NextValue: []string{"NEXT", "VALUE", "FOR"},
		Lexing:    dialectlexer.Options("sqlserver"),
	}
}

// TestCanonical_NextValue reads each spelling of a NEXT VALUE FOR reference
// as the names the catalog stores. SQL Server 2022 stores the clause in upper
// case and each name bracketed, and keeps a schema as written, so each
// declared spelling and the one SQL Server stores for it read alike.
func TestCanonical_NextValue(t *testing.T) {
	rows := []struct {
		name, expression, want string
	}{
		{name: "unqualified", expression: "NEXT VALUE FOR ptah_s", want: `NEXT VALUE FOR "ptah_s"`},
		{name: "stored unqualified", expression: "(NEXT VALUE FOR [ptah_s])", want: `( NEXT VALUE FOR "ptah_s" )`},
		{name: "in lower case", expression: "(next value for ptah_s)", want: `( NEXT VALUE FOR "ptah_s" )`},
		{name: "qualified", expression: "NEXT VALUE FOR dbo.ptah_s", want: `NEXT VALUE FOR "dbo"."ptah_s"`},
		{name: "stored qualified", expression: "NEXT VALUE FOR [dbo].[ptah_s]", want: `NEXT VALUE FOR "dbo"."ptah_s"`},
		{name: "a bracket in the name", expression: "NEXT VALUE FOR [a]]b]", want: `NEXT VALUE FOR "a]b"`},
		{name: "double-quoted", expression: `NEXT VALUE FOR "ptah_s"`, want: `NEXT VALUE FOR "ptah_s"`},
		{name: "an operator", expression: "NEXT VALUE FOR [ptah_s]+1", want: `NEXT VALUE FOR "ptah_s" + 1`},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			got, found := sequenceref.Canonical(row.expression, sqlServer(), "dbo")

			c.Assert(found, qt.IsTrue)
			c.Assert(got, qt.Equals, row.want)
		})
	}
}

// TestCanonical_NoNextValue finds no NEXT VALUE FOR reference in a string, in
// a clause missing a word or its name, or under rules naming no clause.
func TestCanonical_NoNextValue(t *testing.T) {
	rows := []struct {
		name, expression string
		rules            sequenceref.Rules
	}{
		{name: "a string", expression: "'NEXT VALUE FOR ptah_s'", rules: sqlServer()},
		{name: "a missing word", expression: "NEXT VALUE ptah_s", rules: sqlServer()},
		{name: "no name", expression: "NEXT VALUE FOR", rules: sqlServer()},
		{name: "a bracketed word", expression: "[NEXT] VALUE FOR ptah_s", rules: sqlServer()},
		{name: "no clause", expression: "NEXT VALUE FOR ptah_s", rules: sequenceref.Rules{Lexing: dialectlexer.Options("postgres")}},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			_, found := sequenceref.Canonical(row.expression, row.rules, "dbo")

			c.Assert(found, qt.IsFalse)
		})
	}
}

// TestCanonical_DistinctNextValues reads references to different sequences
// as different text: SQL Server keeps a schema as written, so a qualified
// and an unqualified reference stay apart, as do two names.
func TestCanonical_DistinctNextValues(t *testing.T) {
	rows := []struct {
		name, left, right string
	}{
		{name: "qualified and unqualified", left: "NEXT VALUE FOR dbo.ptah_s", right: "NEXT VALUE FOR ptah_s"},
		{name: "two names", left: "NEXT VALUE FOR [a]", right: "NEXT VALUE FOR [b]"},
		{name: "two operands", left: "NEXT VALUE FOR a + 1", right: "NEXT VALUE FOR a + 2"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			left, leftFound := sequenceref.Canonical(row.left, sqlServer(), "dbo")
			right, rightFound := sequenceref.Canonical(row.right, sqlServer(), "dbo")

			c.Assert(leftFound, qt.IsTrue)
			c.Assert(rightFound, qt.IsTrue)
			c.Assert(left, qt.Not(qt.Equals), right)
		})
	}
}
