package builtin_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgextension"
	"ptah.run/feature/pgtype"
)

// objectCommentSchema declares one object of every kind whose comment is a
// COMMENT ON of its own on the PostgreSQL family, each with a comment.
func objectCommentSchema() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Item", Name: "items"}},
		Fields: []schemamodel.Field{{StructName: "Item", Name: "id", Type: "INT", Primary: true}},
		Views:  []schemamodel.View{{Name: "recent", Body: "SELECT id FROM items", Comment: "view note"}},
		Sequences: []schemamodel.Sequence{{
			Name: "ticket", Comment: "sequence note",
		}},
		FeatureObjects: must.Must(schemaext.NewObjects(
			schemaext.Object{Ref: pgtype.Ref(pgtype.DomainKind, "", "positive"), Value: &pgtype.DesiredDomain{BaseType: "integer", Comment: "domain note"}},
			schemaext.Object{Ref: pgtype.Ref(pgtype.CompositeKind, "", "point2"),
				Value: &pgtype.DesiredComposite{Attributes: []pgtype.Attribute{{Name: "x", Type: "integer"}}, Comment: "composite note"}},
			schemaext.Object{Ref: pgtype.Ref(pgtype.RangeKind, "", "span"), Value: &pgtype.DesiredRange{Subtype: "integer", Comment: "range note"}, Targets: []string{"postgres"}},
			must.Must(pgextension.DesiredObject(pgextension.Ref("hstore"), pgextension.DesiredExtension{Comment: "extension note"})))),
		FeatureCoverage: must.Must(pgextension.CompleteCoverage(schemaext.Desired)),
	}
}

// commentLines lists the COMMENT ON statements a render wrote.
func commentLines(statements []string) []string {
	var lines []string
	for _, statement := range statements {
		for line := range strings.SplitSeq(statement, "\n") {
			if strings.HasPrefix(line, "COMMENT ON") {
				lines = append(lines, line)
			}
		}
	}
	return lines
}

// Each object's comment is written after the statement that creates it, where
// the target stores that kind's comment, and is reported as left out where it
// does not (stokaro/ptah#3627). A `-- note` line in the script is what the
// comment was before, and the server keeps none of it.
func TestGetOrderedCreateStatementsReportingOmissions_ObjectComments(t *testing.T) {
	tests := []struct {
		name          string
		dialect       string
		caps          capability.Capabilities
		wantComments  []string
		wantOmissions []string
	}{
		{
			name:    "PostgreSQL stores every one",
			dialect: platform.Postgres,
			caps:    capability.Postgres18(),
			// The omission list is empty rather than absent when nothing
			// was left out.
			wantOmissions: make([]string, 0),
			wantComments: []string{
				`COMMENT ON EXTENSION "hstore" IS 'extension note';`,
				`COMMENT ON SEQUENCE "ticket" IS 'sequence note';`,
				`COMMENT ON DOMAIN "positive" IS 'domain note';`,
				`COMMENT ON TYPE "point2" IS 'composite note';`,
				`COMMENT ON TYPE "span" IS 'range note';`,
				`COMMENT ON VIEW "recent" IS 'view note';`,
			},
		},
		{
			// 26.3 takes the view's and the sequence's and cannot read back
			// a type comment.
			name:    "CockroachDB 26.3 stores two of them",
			dialect: platform.CockroachDB,
			caps:    capability.CockroachDB263(),
			wantComments: []string{
				`COMMENT ON SEQUENCE "ticket" IS 'sequence note';`,
				`COMMENT ON VIEW "recent" IS 'view note';`,
			},
			wantOmissions: []string{
				"domain comment positive ",
				"extension comment hstore ",
				"type comment point2 ",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			schema := objectCommentSchema()
			// Apply the declaration scope before lowering for the selected target.
			schema.FeatureObjects = schema.FeatureObjects.Select(func(ref objectidentity.ID) bool {
				return ref.Kind != objectidentity.Kind(pgtype.RangeKind) || test.dialect == platform.Postgres
			})

			statements, omissions, err := builtin.GetOrderedCreateStatementsReportingOmissions(schema, test.dialect, test.caps)

			c.Assert(err, qt.IsNil)
			c.Assert(commentLines(statements), qt.ContentEquals, test.wantComments)
			c.Assert(omissionSubjects(c, omissions), qt.DeepEquals, test.wantOmissions)
			c.Assert(strings.Join(statements, "\n"), qt.Not(qt.Contains), "-- view note")
		})
	}
}

// A dialect with no COMMENT ON says so for a node built by hand rather than
// writing a statement the server refuses or nothing at all.
func TestRenderSQL_ObjectCommentOutsideThePostgreSQLFamily(t *testing.T) {
	tests := []struct {
		dialect string
		want    string
	}{
		{dialect: platform.MySQL, want: "-- MYSQL: COMMENT ON VIEW app.v is not generated for this target; skipped."},
		{dialect: platform.MariaDB, want: "-- MARIADB: COMMENT ON VIEW app.v is not generated for this target; skipped."},
		{dialect: platform.SQLite, want: `-- SQLITE: COMMENT ON VIEW "app.v" is not supported`},
		{dialect: platform.SQLServer, want: "-- SQLSERVER: COMMENT ON VIEW"},
		{dialect: platform.Oracle, want: `-- ORACLE: COMMENT ON VIEW "app.v" is not supported`},
		{dialect: platform.ClickHouse, want: `-- CLICKHOUSE: COMMENT ON VIEW "app.v" is not supported`},
	}
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)

			sql, err := builtin.RenderSQL(test.dialect, ast.NewObjectComment(ast.CommentedView, "app.v", "note"))

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, test.want)
			c.Assert(sql, qt.Not(qt.Contains), "IS 'note'")
		})
	}
}
