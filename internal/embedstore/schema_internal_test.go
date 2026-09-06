package embedstore

// White-box testing required: the persistence ratchet enumerates embedrun.Run
// against RunFields and runFieldsNotPersisted, and the second list is not
// reachable from outside the package.

import (
	"reflect"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/embedrun"
)

// TestRunFields_CoverEveryRunField is the ratchet.
//
// A field added to embedrun.Run and not to RunFields is a field a resumed run
// silently forgets, and nothing about that failure looks like a storage bug: the
// run resumes, the phase is right, and one value is the zero it started life
// with. So the struct is enumerated and each leaf field has to appear in the
// column list or in runFieldsNotPersisted with the reason a run resumed without
// it resumes correctly (stokaro/ptah#2068).
func TestRunFields_CoverEveryRunField(t *testing.T) {
	c := qt.New(t)

	c.Assert(unpersistedFields(), qt.HasLen, 0, qt.Commentf(
		"each of these embedrun.Run fields must have a column in RunFields, or be listed in "+
			"runFieldsNotPersisted with the reason a run resumed without it resumes correctly"))
}

// TestGenerationFields_CoverEveryGenerationField is the same ratchet over the
// registry.
//
// The run's ratchet exists because a forgotten field resumes as a zero value.
// The registry's exists because a forgotten field is one a LATER VERB CANNOT
// ASK ABOUT. Retirement has to know which source an outbox belongs to, and the
// registry recorded only the target: the question was asked about the target
// instead, and retiring one generation destroyed the change capture a second
// live generation over the same source was still fed by (stokaro/ptah#2649).
//
// Every field is expected to have a column. There is no exemption list here on
// purpose -- unlike a run, which carries in-flight state a restart recomputes,
// a generation is a durable record and everything on it is part of that record.
// A field that genuinely should not be stored gets this test's counterpart, and
// a written reason, at the point somebody has one.
func TestGenerationFields_CoverEveryGenerationField(t *testing.T) {
	c := qt.New(t)

	c.Assert(unrecordedGenerationFields(), qt.HasLen, 0, qt.Commentf(
		"each of these embedstore.Generation fields must have a column in GenerationFields, "+
			"or a later verb cannot ask the registry about it"))
}

// TestRunFields_TheOmissionsAreDeliberate keeps the other list honest.
func TestRunFields_TheOmissionsAreDeliberate(t *testing.T) {
	c := qt.New(t)

	for field, reason := range runFieldsNotPersisted {
		c.Assert(len(strings.TrimSpace(reason)) > 20, qt.IsTrue,
			qt.Commentf("%s is not persisted and there is no reason worth reading", field))
	}
}

// TestEventFields_HoldNoContent is the audit trail's boundary, in the schema.
//
// embedrun.Event has its own reflection test refusing a field for row content
// or a vector. That guards the struct, and this guards the table -- a column
// nothing writes today is a column somebody writes tomorrow, and the audit
// trail is the one place a corpus leaks into by accident.
func TestEventFields_HoldNoContent(t *testing.T) {
	forbidden := []string{"content", "text", "body", "vector", "embedding", "input", "payload"}
	for _, word := range forbidden {
		t.Run(word, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(columnsMentioning(EventFields(), word), qt.HasLen, 0)
		})
	}
}

// TestObjects_NameEveryTableThisPackageWrites keeps the schema and the
// constants from drifting apart.
//
// A table constant with no table behind it is a store that compiles and cannot
// write, and the failure surfaces as a missing relation at the first
// checkpoint -- hours into a backfill.
func TestObjects_NameEveryTableThisPackageWrites(t *testing.T) {
	c := qt.New(t)

	names := make([]string, 0, len(Objects()))
	for _, table := range Objects() {
		names = append(names, table.Name)
	}

	c.Assert(names, qt.DeepEquals,
		[]string{GenerationTable, RunTable, EventTable, PointerTable})
}

// TestFields_AreAllFiledUnderTheirOwnTable catches a column declared against
// the wrong one.
//
// Every field carries the table it belongs to, and the helpers take it as an
// argument. A copied line with the previous table's name still compiles, still
// renders, and creates a column on a table that has no business holding it.
func TestFields_AreAllFiledUnderTheirOwnTable(t *testing.T) {
	tests := []struct {
		name  string
		table string
	}{
		{name: GenerationTable, table: GenerationTable},
		{name: RunTable, table: RunTable},
		{name: EventTable, table: EventTable},
		{name: PointerTable, table: PointerTable},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(misfiledColumns(test.table), qt.HasLen, 0)
		})
	}
}

// fieldsOf returns one table's declared columns.
func fieldsOf(table string) []schemamodel.Field {
	switch table {
	case GenerationTable:
		return GenerationFields()
	case RunTable:
		return RunFields()
	case EventTable:
		return EventFields()
	case PointerTable:
		return PointerFields()
	default:
		return nil
	}
}

// misfiledColumns lists the columns of a table that name a different one.
func misfiledColumns(table string) []string {
	var misfiled []string
	for _, field := range fieldsOf(table) {
		if field.StructName != table {
			misfiled = append(misfiled, field.StructName+"."+field.Name)
		}
	}
	return misfiled
}

// columnsMentioning lists the columns whose name contains a word.
func columnsMentioning(fields []schemamodel.Field, word string) []string {
	var matching []string
	for _, field := range fields {
		if strings.Contains(field.Name, word) {
			matching = append(matching, field.Name)
		}
	}
	return matching
}

// unpersistedFields lists the Run fields with no column and no exemption.
//
// The walk lives here rather than in the test body because a test asserts and
// does not branch, which is the rule scripts/check-test-style.sh enforces.
func unpersistedFields() []string {
	columns := make(map[string]bool, len(RunFields()))
	for _, field := range RunFields() {
		columns[field.Name] = true
	}
	var missing []string
	for _, field := range leafFieldPaths(reflect.TypeFor[embedrun.Run](), "") {
		if _, exempt := runFieldsNotPersisted[field]; exempt {
			continue
		}
		if columns[columnFor(field)] {
			continue
		}
		missing = append(missing, field)
	}
	return missing
}

// unrecordedGenerationFields lists the Generation fields with no column.
func unrecordedGenerationFields() []string {
	columns := make(map[string]bool, len(GenerationFields()))
	for _, field := range GenerationFields() {
		columns[field.Name] = true
	}
	var missing []string
	for _, field := range leafFieldPaths(reflect.TypeFor[Generation](), "") {
		if columns[snakeCase(field)] {
			continue
		}
		missing = append(missing, field)
	}
	return missing
}

// columnFor maps a Go field path onto the column name that holds it.
//
// Progress is flattened: its fields are the run's counters and live beside the
// rest rather than in a nested row nothing would ever read on its own.
func columnFor(fieldPath string) string {
	return snakeCase(strings.TrimPrefix(fieldPath, "Progress."))
}

// snakeCase renders a Go field name the way the columns spell it.
//
// A run of capitals is one word, so `ID` is `id` and `RunID` is `run_id`. The
// naive rule -- underscore before every capital -- answers `i_d`, which is not
// a column anybody would write and would have sent this ratchet looking for one.
func snakeCase(name string) string {
	runes := []rune(name)
	var b strings.Builder
	for index, symbol := range runes {
		if index > 0 && isUpper(symbol) && startsAWord(runes, index) {
			b.WriteByte('_')
		}
		b.WriteRune(lower(symbol))
	}
	return b.String()
}

// startsAWord reports whether the capital at this position begins a new word.
func startsAWord(runes []rune, index int) bool {
	if !isUpper(runes[index-1]) {
		return true
	}
	return index+1 < len(runes) && !isUpper(runes[index+1])
}

// isUpper reports whether a rune is an ASCII capital.
func isUpper(symbol rune) bool {
	return symbol >= 'A' && symbol <= 'Z'
}

// lower folds an ASCII capital.
func lower(symbol rune) rune {
	if isUpper(symbol) {
		return symbol - 'A' + 'a'
	}
	return symbol
}

// leafFieldPaths lists the dotted paths of every leaf field, treating time.Time
// as a leaf because a column holds one rather than its parts.
func leafFieldPaths(typ reflect.Type, prefix string) []string {
	var paths []string
	for field := range typ.Fields() {
		path := field.Name
		if prefix != "" {
			path = prefix + "." + field.Name
		}
		if field.Type.Kind() == reflect.Struct && field.Type != reflect.TypeFor[time.Time]() {
			paths = append(paths, leafFieldPaths(field.Type, path)...)
			continue
		}
		paths = append(paths, path)
	}
	return paths
}
