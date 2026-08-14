package sqliterebuild_test

import (
	"reflect"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/planner/sqliterebuild"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

// TestNeedsTableRebuildReadsEveryChangeField pins the predicate one recorded
// change at a time.
//
// The rows matter in both directions. A false answer for a rebuilding change
// lets the SQLite virtual-table guard wave through a drop-recreate-copy against
// storage it cannot classify, which is the destruction stokaro/ptah#1028
// measured. A true answer for added columns costs a capability instead: the
// planner expresses those as `ALTER TABLE ... ADD COLUMN`, and refusing that
// took away `schema diff --include users` on a database holding an fts4 index.
func TestNeedsTableRebuildReadsEveryChangeField(t *testing.T) {
	tests := []struct {
		name  string
		table difftypes.TableDiff
		want  bool
	}{
		{
			// The exclusion, and the reason this predicate is shared rather
			// than spelled twice.
			name:  "added columns alone are an ALTER TABLE",
			table: difftypes.TableDiff{TableName: "users", ColumnsAdded: []string{"email"}},
			want:  false,
		},
		{
			name:  "removed columns rebuild",
			table: difftypes.TableDiff{TableName: "users", ColumnsRemoved: []string{"email"}},
			want:  true,
		},
		{
			name: "modified columns rebuild",
			table: difftypes.TableDiff{
				TableName:       "users",
				ColumnsModified: []difftypes.ColumnDiff{{ColumnName: "name", Changes: map[string]string{"type": "TEXT -> INTEGER"}}},
			},
			want: true,
		},
		{
			name:  "an added constraint rebuilds",
			table: difftypes.TableDiff{TableName: "users", ConstraintsAdded: []string{"users_chk"}},
			want:  true,
		},
		{
			name:  "a removed constraint rebuilds",
			table: difftypes.TableDiff{TableName: "users", ConstraintsRemoved: []string{"users_chk"}},
			want:  true,
		},
		{
			// The combination, because the exclusion is about a diff whose
			// ONLY change is added columns. One rebuilding change beside them
			// still rebuilds, and the added columns then travel with it.
			name: "added columns beside a removal still rebuild",
			table: difftypes.TableDiff{
				TableName:      "users",
				ColumnsAdded:   []string{"email"},
				ColumnsRemoved: []string{"legacy"},
			},
			want: true,
		},
		{
			name:  "a table diff recording nothing rebuilds nothing",
			table: difftypes.TableDiff{TableName: "users"},
			want:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(sqliterebuild.NeedsTableRebuild(tt.table), qt.Equals, tt.want)
		})
	}
}

// TestEveryTableDiffFieldIsClassified is the census that keeps this predicate
// from silently ignoring a field somebody adds later.
//
// [difftypes.TableDiff] is the whole record of what changed about one existing
// table. Every field on it is either a change SQLite cannot express in place --
// in which case the planner rebuilds and the SQLite virtual-table guard must
// refuse -- or one it can. A field nobody classified is read as "nothing to
// rebuild" by default, which is the direction that loses the index, so this
// test requires the classification to be written down AND requires the
// predicate to agree with it, field by field.
func TestEveryTableDiffFieldIsClassified(t *testing.T) {
	c := qt.New(t)

	// The classification, maintained by hand because that is the point: adding
	// a field to TableDiff must make somebody answer this question.
	classified := map[string]bool{
		"TableName":          false, // the name, not a change
		"ColumnsAdded":       false, // ALTER TABLE ... ADD COLUMN
		"ColumnsRemoved":     true,
		"ColumnsModified":    true,
		"ConstraintsAdded":   true,
		"ConstraintsRemoved": true,
	}

	// One non-zero value per field kind, so the census exercises the predicate
	// rather than only naming the fields. A field of a kind not listed here
	// fails the lookup below, which is the intended outcome: a new shape needs
	// a decision, not a default.
	nonZero := map[reflect.Kind]func(reflect.Type) reflect.Value{
		reflect.String: func(fieldType reflect.Type) reflect.Value {
			return reflect.ValueOf("users").Convert(fieldType)
		},
		reflect.Slice: func(fieldType reflect.Type) reflect.Value {
			return reflect.MakeSlice(fieldType, 1, 1)
		},
	}

	fields := reflect.VisibleFields(reflect.TypeFor[difftypes.TableDiff]())
	c.Assert(fields, qt.HasLen, len(classified))

	for _, field := range fields {
		want, known := classified[field.Name]
		c.Assert(known, qt.IsTrue, qt.Commentf(
			"TableDiff.%s is not classified: decide whether SQLite can converge it in place"+
				" and record the answer here and in NeedsTableRebuild", field.Name,
		))
		build, buildable := nonZero[field.Type.Kind()]
		c.Assert(buildable, qt.IsTrue, qt.Commentf(
			"TableDiff.%s has kind %s, which this census cannot populate", field.Name, field.Type.Kind(),
		))

		table := difftypes.TableDiff{}
		reflect.ValueOf(&table).Elem().FieldByName(field.Name).Set(build(field.Type))

		c.Assert(sqliterebuild.NeedsTableRebuild(table), qt.Equals, want, qt.Commentf(
			"TableDiff.%s alone", field.Name,
		))
	}
}
