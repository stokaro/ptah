package schemamodel_test

import (
	"fmt"
	"hash/fnv"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
)

// TestTrigger_FunctionName_EscapesTheJoinBoundary reproduces the reported
// collision. A table name that already carries an underscore reads exactly
// like the join FunctionName inserts between the table and the trigger name,
// so two distinct pairs used to land on the same generated function:
// table "a_b" trigger "c" and table "a" trigger "b_c" both produced
// "ptah_trigger_a_b_c". Doubling every underscore a sanitized part keeps,
// before the parts join on a single undoubled "_", tells the two apart.
func TestTrigger_FunctionName_EscapesTheJoinBoundary(t *testing.T) {
	rows := []struct {
		name    string
		trigger schemamodel.Trigger
		want    string
	}{
		{
			name:    "the underscore sits in the table name",
			trigger: schemamodel.Trigger{Table: "a_b", Name: "c"},
			want:    "ptah_trigger_a__b_c",
		},
		{
			name:    "the underscore sits in the trigger name",
			trigger: schemamodel.Trigger{Table: "a", Name: "b_c"},
			want:    "ptah_trigger_a_b__c",
		},
	}

	got := make([]string, len(rows))
	for i, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			got[i] = row.trigger.FunctionName()
			c.Assert(got[i], qt.Equals, row.want)
		})
	}

	c := qt.New(t)
	c.Assert(got[0], qt.Not(qt.Equals), got[1],
		qt.Commentf("table %q trigger %q and table %q trigger %q must not share a function name",
			rows[0].trigger.Table, rows[0].trigger.Name, rows[1].trigger.Table, rows[1].trigger.Name))
}

// TestTrigger_FunctionName_NoUnderscoreNameIsUnchanged locks in that a table
// and trigger name carrying no underscore anywhere renders the same name the
// naive, unescaped join always produced. Most declared table and trigger
// names carry no underscore, and the escaping fix must not rename the
// function an already-deployed database is still running for one of them.
func TestTrigger_FunctionName_NoUnderscoreNameIsUnchanged(t *testing.T) {
	rows := []struct {
		name    string
		trigger schemamodel.Trigger
		want    string
	}{
		{
			name:    "plain lower case names",
			trigger: schemamodel.Trigger{Table: "users", Name: "notify"},
			want:    "ptah_trigger_users_notify",
		},
		{
			name:    "mixed case folds without otherwise changing",
			trigger: schemamodel.Trigger{Table: "Orders", Name: "Touch"},
			want:    "ptah_trigger_orders_touch",
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(row.trigger.FunctionName(), qt.Equals, row.want)
		})
	}
}

// TestTrigger_FunctionName_LeadingDigitComposesWithEscaping covers a name
// sanitizeTriggerFunctionPart escapes twice over: once for the leading digit
// PostgreSQL would otherwise read as the start of a number, and once for the
// doubling every remaining underscore gets. The digit-escape prefix is added
// after the trim and before the doubling, so it has to be doubled along with
// everything else or it would be the one lone, undoubled underscore the join
// logic depends on there being none of.
func TestTrigger_FunctionName_LeadingDigitComposesWithEscaping(t *testing.T) {
	c := qt.New(t)

	// "1abc" sanitizes to "1abc" (nothing to collapse), then the leading
	// digit gets its escape prefix: "_1abc". "2_x" sanitizes to "2_x"
	// unchanged (the underscore is already an identifier character) and
	// digit-escapes to "_2_x". Both pre-escape segments are derived by the
	// documented rule, and only the final doubling is asserted here.
	trigger := schemamodel.Trigger{Table: "1abc", Name: "2_x"}

	want := "ptah_trigger_" +
		strings.ReplaceAll("_1abc", "_", "__") + "_" +
		strings.ReplaceAll("_2_x", "_", "__")

	c.Assert(trigger.FunctionName(), qt.Equals, want)
}

// TestTrigger_FunctionName_PunctuationOnlyNameFallsBackToObject covers a
// table or trigger name with nothing sanitizeTriggerFunctionPart's collapsing
// keeps: the result is empty, and the "object" fallback carries neither a
// digit nor an underscore, so escaping is a no-op over it.
func TestTrigger_FunctionName_PunctuationOnlyNameFallsBackToObject(t *testing.T) {
	c := qt.New(t)

	trigger := schemamodel.Trigger{Table: "!!!", Name: "???"}

	c.Assert(trigger.FunctionName(), qt.Equals, "ptah_trigger_object_object")
}

// TestTrigger_FunctionName_TruncatesTheEscapedNameNotTheRawOne is the case
// the escaping fix could get wrong silently: a pair whose OLD, unescaped
// join landed exactly at the 63-byte PostgreSQL identifier limit, so it never
// reached the hash-suffix fallback, but whose escaped join is one byte over
// and must reach it. The fallback still hashes the raw table and trigger
// name, unescaped and untruncated, so the suffix does not change; only the
// prefix it truncates has to be the escaped name.
func TestTrigger_FunctionName_TruncatesTheEscapedNameNotTheRawOne(t *testing.T) {
	c := qt.New(t)

	table := strings.Repeat("a", 24) + "_" + strings.Repeat("a", 23)
	name := "n"
	trigger := schemamodel.Trigger{Table: table, Name: name}

	rawJoin := "ptah_trigger_" + table + "_" + name
	c.Assert(rawJoin, qt.HasLen, 63,
		qt.Commentf("the fixture's point is a pair the unescaped join keeps at the limit, not over it"))

	escapedJoin := "ptah_trigger_" + strings.ReplaceAll(table, "_", "__") + "_" + strings.ReplaceAll(name, "_", "__")
	c.Assert(escapedJoin, qt.HasLen, 64,
		qt.Commentf("escaping the table's one underscore should be exactly what crosses the limit"))

	hash := fnv.New32a()
	_, _ = hash.Write([]byte(table))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(name))
	suffix := fmt.Sprintf("_%08x", hash.Sum32())

	got := trigger.FunctionName()
	c.Assert(got, qt.HasLen, 63)
	c.Assert(got, qt.Equals, escapedJoin[:63-len(suffix)]+suffix)
}
