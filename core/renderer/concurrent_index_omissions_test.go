package renderer_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
)

// concurrentIndexSchema asks for an index built without locking the table.
//
// The declaration is worth making on a table large enough that the lock is the
// difference between a migration and an outage, and the statement a target
// writes for it reads as an ordinary CREATE INDEX either way.
func concurrentIndexSchema(concurrently bool) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Doc", Name: "docs"}},
		Fields: []schemamodel.Field{
			{StructName: "Doc", Name: "id", Type: "INT", Primary: true},
			{StructName: "Doc", Name: "title", Type: "VARCHAR(80)"},
		},
		Indexes: []schemamodel.Index{{
			StructName:   "Doc",
			Name:         "idx_docs_title",
			Fields:       []string{"title"},
			Concurrently: concurrently,
		}},
	}
}

// concurrentOmissionSubjects renders each omission as the object it belongs to
// and the property it lost, so a case row can carry the answer as data.
func concurrentOmissionSubjects(c *qt.C, omissions []renderer.Omission) []string {
	c.Helper()

	// Declared nil rather than made empty, so a target that lost nothing and a
	// target that was not asked for anything read the same in a case row.
	var subjects []string
	for _, omission := range omissions {
		subjects = append(subjects, omission.Kind+" "+omission.Name+" "+omission.Property)
	}
	return subjects
}

// TestGetOrderedCreateStatementsReportingOmissions_BuildsTheIndexConcurrently is
// the reproduction from stokaro/ptah#3042, at the entry point a caller uses.
//
// The keyword reached ast.IndexNode from the parser and schemamodel.Index from
// the schema reader, and internal/modelast dropped it on the way to the AST the
// renderer walks. Every layer was green while the rendered script took a write
// lock the author had asked it not to take.
func TestGetOrderedCreateStatementsReportingOmissions_BuildsTheIndexConcurrently(t *testing.T) {
	c := qt.New(t)

	statements, omissions, err := renderer.GetOrderedCreateStatementsReportingOmissions(
		concurrentIndexSchema(true), platform.Postgres, capability.Postgres17())

	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(statements, "\n"), qt.Contains, "CREATE INDEX CONCURRENTLY")
	c.Assert(concurrentOmissionSubjects(c, omissions), qt.HasLen, 0)
}

// TestGetOrderedCreateStatementsReportingOmissions_NamesAConcurrentBuildATargetCannotMake
// covers the targets in this family that have no such build.
//
// The rows are measured rather than assumed: CockroachDB and Spanner speak the
// PostgreSQL wire protocol and go through the same renderer, and neither
// carries CreateIndexConcurrently. YugabyteDB does, so it is here as the row
// that must report nothing -- without it, a report firing for any concurrent
// declaration at all would satisfy the table.
func TestGetOrderedCreateStatementsReportingOmissions_NamesAConcurrentBuildATargetCannotMake(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		want    []string
	}{
		{
			name:    "cockroachdb builds under a lock",
			dialect: platform.CockroachDB,
			want:    []string{"index idx_docs_title concurrent index build"},
		},
		{
			name:    "spanner builds under a lock",
			dialect: platform.Spanner,
			want:    []string{"index idx_docs_title concurrent index build"},
		},
		{
			name:    "yugabytedb builds concurrently",
			dialect: platform.YugabyteDB,
			want:    nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, omissions, err := renderer.GetOrderedCreateStatementsReportingOmissions(
				concurrentIndexSchema(true), test.dialect, capability.ForDialect(test.dialect))

			c.Assert(err, qt.IsNil)
			c.Assert(concurrentOmissionSubjects(c, omissions), qt.DeepEquals, test.want)
		})
	}
}

// TestGetOrderedCreateStatementsReportingOmissions_LeavesAnUndeclaredIndexAlone
// is the control for the rows above.
//
// They are satisfied by a report that names every index a target renders, and
// this schema is the same one with the declaration taken out. A finding here
// would mean the record describes the target rather than what the author asked
// for.
func TestGetOrderedCreateStatementsReportingOmissions_LeavesAnUndeclaredIndexAlone(t *testing.T) {
	c := qt.New(t)

	statements, omissions, err := renderer.GetOrderedCreateStatementsReportingOmissions(
		concurrentIndexSchema(false), platform.CockroachDB, capability.ForDialect(platform.CockroachDB))

	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(statements, "\n"), qt.Contains, "CREATE INDEX")
	c.Assert(concurrentOmissionSubjects(c, omissions), qt.HasLen, 0)
}
