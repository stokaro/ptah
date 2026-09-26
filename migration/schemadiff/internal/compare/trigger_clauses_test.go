package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/exprkey"
	"ptah.run/migration/schemadiff/difftypes"
	"ptah.run/migration/schemadiff/internal/compare"
)

// clauseTrigger is one side's view of a trigger, reduced to the clauses these
// tests vary.
type clauseTrigger struct {
	event    string
	forEach  string
	when     string
	oldTable string
	newTable string
}

func (s clauseTrigger) declared() schemamodel.Trigger {
	return schemamodel.Trigger{
		Name: "trg", Table: "a", Timing: "AFTER", Event: s.event, ForEach: s.forEach,
		When: s.when, OldTable: s.oldTable, NewTable: s.newTable, ExecuteFunction: "f",
	}
}

func (s clauseTrigger) read() catalog.Trigger {
	return catalog.Trigger{
		Name: "trg", Table: "a", Timing: "AFTER", Event: s.event, ForEach: s.forEach,
		When: s.when, OldTable: s.oldTable, NewTable: s.newTable, ExecuteFunction: "f",
	}
}

// TestTriggerDefinitions_ClausesTheServerSpellsItsOwnWay_HappyPath holds a
// declaration against what PostgreSQL 18.6 reads back for it. Each row is a
// spelling the server changes without changing the trigger, and none of them
// is a change.
func TestTriggerDefinitions_ClausesTheServerSpellsItsOwnWay_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		declared clauseTrigger
		read     clauseTrigger
	}{
		{
			name:     "events in another order",
			declared: clauseTrigger{event: "UPDATE OR INSERT", forEach: "ROW"},
			read:     clauseTrigger{event: "INSERT OR UPDATE", forEach: "ROW"},
		},
		{
			name:     "update columns folded the way the server folds them",
			declared: clauseTrigger{event: `UPDATE OF B, "Total" OR DELETE`, forEach: "ROW"},
			read:     clauseTrigger{event: `DELETE OR UPDATE OF b, "Total"`, forEach: "ROW"},
		},
		{
			name:     "a quoted column the server prints bare",
			declared: clauseTrigger{event: `UPDATE OF "total"`, forEach: "ROW"},
			read:     clauseTrigger{event: "UPDATE OF total", forEach: "ROW"},
		},
		{
			name:     "a condition the server parenthesizes",
			declared: clauseTrigger{event: "UPDATE", forEach: "ROW", when: "NEW.a IS DISTINCT FROM OLD.a"},
			read:     clauseTrigger{event: "UPDATE", forEach: "ROW", when: "(new.a IS DISTINCT FROM old.a)"},
		},
		{
			name:     "the same transition tables",
			declared: clauseTrigger{event: "UPDATE", forEach: "STATEMENT", oldTable: "OldRows", newTable: "newrows"},
			read:     clauseTrigger{event: "UPDATE", forEach: "STATEMENT", oldTable: "OldRows", newTable: "newrows"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := compare.TriggerDefinitions(test.declared.declared(), test.read.read(), identifier.ForDialect(platform.Postgres))
			c.Assert(diff.Changes, qt.HasLen, 0)
		})
	}
}

// TestTriggerDefinitions_ClauseChanges names the clause that changed, so the
// plan replaces the trigger rather than keeping one that fires at other
// times, on other rows, or without the tables its function reads.
func TestTriggerDefinitions_ClauseChanges(t *testing.T) {
	tests := []struct {
		name     string
		declared clauseTrigger
		read     clauseTrigger
		want     map[string]string
	}{
		{
			name:     "an event added",
			declared: clauseTrigger{event: "INSERT OR UPDATE", forEach: "ROW"},
			read:     clauseTrigger{event: "INSERT", forEach: "ROW"},
			want:     map[string]string{"event": "INSERT -> INSERT OR UPDATE"},
		},
		{
			name:     "update columns in another order",
			declared: clauseTrigger{event: "UPDATE OF a, b", forEach: "ROW"},
			read:     clauseTrigger{event: "UPDATE OF b, a", forEach: "ROW"},
			want:     map[string]string{"event": "UPDATE OF b, a -> UPDATE OF a, b"},
		},
		{
			name:     "a quoted column is another column",
			declared: clauseTrigger{event: `UPDATE OF "Total"`, forEach: "ROW"},
			read:     clauseTrigger{event: "UPDATE OF total", forEach: "ROW"},
			want:     map[string]string{"event": `UPDATE OF total -> UPDATE OF "Total"`},
		},
		{
			name:     "a condition added",
			declared: clauseTrigger{event: "UPDATE", forEach: "ROW", when: "NEW.a > 0"},
			read:     clauseTrigger{event: "UPDATE", forEach: "ROW"},
			want:     map[string]string{"when": "(none) -> NEW.a > 0"},
		},
		{
			name:     "a condition changed",
			declared: clauseTrigger{event: "UPDATE", forEach: "ROW", when: "NEW.a > 1"},
			read:     clauseTrigger{event: "UPDATE", forEach: "ROW", when: "(new.a > 0)"},
			want:     map[string]string{"when": "(new.a > 0) -> NEW.a > 1"},
		},
		{
			name:     "a transition table named in another case",
			declared: clauseTrigger{event: "UPDATE", forEach: "STATEMENT", oldTable: "OldRows"},
			read:     clauseTrigger{event: "UPDATE", forEach: "STATEMENT", oldTable: "oldrows"},
			want:     map[string]string{"referencing": "old=oldrows new= -> old=OldRows new="},
		},
		{
			name:     "a transition table dropped",
			declared: clauseTrigger{event: "UPDATE", forEach: "STATEMENT", oldTable: "o"},
			read:     clauseTrigger{event: "UPDATE", forEach: "STATEMENT", oldTable: "o", newTable: "n"},
			want:     map[string]string{"referencing": "old=o new=n -> old=o new="},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := compare.TriggerDefinitions(test.declared.declared(), test.read.read(), identifier.ForDialect(platform.Postgres))
			c.Assert(diff.Changes, qt.DeepEquals, test.want)
		})
	}
}

// TestTriggersWithSemanticsAndConditions_UsesTheServerSpelling compares a
// declared condition through the spelling the server gave it, where folding
// alone cannot tell the two apart: PostgreSQL 18.6 prints `NEW.a IN (1, 2)` as
// `(new.a = ANY (ARRAY[1, 2]))`.
func TestTriggersWithSemanticsAndConditions_UsesTheServerSpelling(t *testing.T) {
	declared := clauseTrigger{event: "UPDATE", forEach: "ROW", when: "NEW.a IN (1, 2)"}
	read := clauseTrigger{event: "UPDATE", forEach: "ROW", when: "(new.a = ANY (ARRAY[1, 2]))"}
	semantics := identifier.ForDialect(platform.Postgres)
	key := exprkey.Trigger(semantics, "a", "trg")

	tests := []struct {
		name       string
		conditions map[string]config.TriggerCondition
		wantCount  int
	}{
		{name: "folded, the two differ", conditions: nil, wantCount: 1},
		{
			name:       "resolved, the two agree",
			conditions: map[string]config.TriggerCondition{key: {Condition: read.when, Resolved: true}},
			wantCount:  0,
		},
		{
			name:       "an unresolved answer is not used",
			conditions: map[string]config.TriggerCondition{key: {Condition: read.when}},
			wantCount:  1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{Triggers: []schemamodel.Trigger{declared.declared()}}
			database := &catalog.Database{Triggers: []catalog.Trigger{read.read()}}
			diff := &difftypes.SchemaDiff{}

			compare.TriggersWithSemanticsAndConditions(desired, database, diff, semantics, test.conditions, capability.Postgres18())

			c.Assert(diff.TriggersModified, qt.HasLen, test.wantCount)
		})
	}
}

// TestTriggersWithSemanticsAndConditions_ComparesOnlyAConditionTheReadReports
// holds a declared condition against a read that could not report one. On a
// target without pg_get_triggerdef the read has every trigger without its
// condition, so comparing it would replace the trigger on every plan; on a
// target with the function, a condition the server lacks is a change
// (stokaro/ptah#3707).
func TestTriggersWithSemanticsAndConditions_ComparesOnlyAConditionTheReadReports(t *testing.T) {
	declared := clauseTrigger{event: "UPDATE", forEach: "ROW", when: "(NEW).a > 0"}
	read := clauseTrigger{event: "UPDATE", forEach: "ROW"}
	tests := []struct {
		name      string
		caps      capability.Capabilities
		wantCount int
	}{
		{name: "CockroachDB 25.4 reads no condition", caps: capability.CockroachDB25(), wantCount: 0},
		{name: "CockroachDB 26.3 reads one", caps: capability.CockroachDB263(), wantCount: 1},
		{name: "PostgreSQL 18 reads one", caps: capability.Postgres18(), wantCount: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{Triggers: []schemamodel.Trigger{declared.declared()}}
			database := &catalog.Database{Triggers: []catalog.Trigger{read.read()}}
			diff := &difftypes.SchemaDiff{}

			compare.TriggersWithSemanticsAndConditions(
				desired, database, diff, identifier.ForDialect(platform.CockroachDB), nil, test.caps,
			)

			c.Assert(diff.TriggersModified, qt.HasLen, test.wantCount)
		})
	}
}

// TestTriggerDefinitions_CockroachTypeAnnotations compares a condition with
// CockroachDB's read-back of it, which annotates each constant with its type:
// `WHEN ((NEW).a > 0)` reads back as `(new).a > 0:::INT8` on v26.3.1. The
// annotation is not a change; a different constant still is, and so is `:::`
// inside a string.
func TestTriggerDefinitions_CockroachTypeAnnotations(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		read     string
		want     map[string]string
	}{
		{name: "an annotated constant", declared: "(NEW).a > 0", read: "(new).a > 0:::INT8", want: make(map[string]string)},
		{name: "two annotated constants", declared: "1 = 1", read: "1:::INT8 = 1:::INT8", want: make(map[string]string)},
		{
			name: "another constant", declared: "(NEW).a > 1", read: "(new).a > 0:::INT8",
			want: map[string]string{"when": "(new).a > 0:::INT8 -> (NEW).a > 1"},
		},
		{
			name: "the annotation inside a string is text", declared: "(NEW).s = 'a'", read: "(new).s = 'a:::INT8':::STRING",
			want: map[string]string{"when": "(new).s = 'a:::INT8':::STRING -> (NEW).s = 'a'"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := compare.TriggerDefinitions(
				clauseTrigger{event: "UPDATE", forEach: "ROW", when: test.declared}.declared(),
				clauseTrigger{event: "UPDATE", forEach: "ROW", when: test.read}.read(),
				identifier.ForDialect(platform.CockroachDB),
			)
			c.Assert(diff.Changes, qt.DeepEquals, test.want)
		})
	}
}
