package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/sqlschema"
)

// indexCommentsByTable lists the comment of every index under its table and
// its name, so two indexes of one name are two entries.
func indexCommentsByTable(database schemamodel.Database) map[string]string {
	comments := make(map[string]string, len(database.Indexes))
	for _, index := range database.Indexes {
		comments[index.TableName+"@"+index.Name] = index.Comment
	}
	return comments
}

// twoSchemaIndexes declares the table t with an index t_a in s2 and in public,
// in the order the schemas are given.
func twoSchemaIndexes(first, second string) string {
	return "CREATE SCHEMA s2;\n" +
		"CREATE TABLE " + first + ".t (a int);\nCREATE INDEX t_a ON " + first + ".t (a);\n" +
		"CREATE TABLE " + second + ".t (a int);\nCREATE INDEX t_a ON " + second + ".t (a);\n"
}

// twoTableIndexes declares the tables t and u in the default schema, each with
// an index t_a, which CockroachDB allows because it names an index within its
// table.
const twoTableIndexes = "CREATE TABLE t (a int);\nCREATE TABLE u (a int);\n" +
	"CREATE INDEX t_a ON t (a);\nCREATE INDEX t_a ON u (a);\n"

// A COMMENT ON INDEX reaches the index of the schema it names, and of the
// default schema when the name is bare, which is how the server resolves it.
// Each row has two indexes called t_a, declared in either order, so a comment
// found by the name alone lands on the wrong one of them in at least one row.
func TestRead_CommentOnIndex_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		file    string
		want    map[string]string
	}{
		{
			name:    "in two schemas, s2 declared first",
			dialect: "postgres",
			file:    twoSchemaIndexes("s2", "public") + "COMMENT ON INDEX s2.t_a IS 'ia';\nCOMMENT ON INDEX public.t_a IS 'pia';",
			want:    map[string]string{"s2.t@t_a": "ia", "public.t@t_a": "pia"},
		},
		{
			name:    "in two schemas, public declared first",
			dialect: "postgres",
			file:    twoSchemaIndexes("public", "s2") + "COMMENT ON INDEX public.t_a IS 'pia';\nCOMMENT ON INDEX s2.t_a IS 'ia';",
			want:    map[string]string{"s2.t@t_a": "ia", "public.t@t_a": "pia"},
		},
		{
			name:    "a bare name is the default schema's",
			dialect: "postgres",
			file:    twoSchemaIndexes("s2", "public") + "COMMENT ON INDEX t_a IS 'pia';\nCOMMENT ON INDEX s2.t_a IS 'ia';",
			want:    map[string]string{"s2.t@t_a": "ia", "public.t@t_a": "pia"},
		},
		{
			name:    "one of the two commented",
			dialect: "postgres",
			file:    twoSchemaIndexes("s2", "public") + "COMMENT ON INDEX public.t_a IS 'pia';",
			want:    map[string]string{"s2.t@t_a": "", "public.t@t_a": "pia"},
		},
		{
			name:    "CockroachDB, in two schemas",
			dialect: "cockroachdb",
			file:    twoSchemaIndexes("s2", "public") + "COMMENT ON INDEX public.t_a IS 'pia';\nCOMMENT ON INDEX s2.t_a IS 'ia';",
			want:    map[string]string{"s2.t@t_a": "ia", "public.t@t_a": "pia"},
		},
		{
			name:    "CockroachDB, each table's index by its table",
			dialect: "cockroachdb",
			file:    twoTableIndexes + "COMMENT ON INDEX u@t_a IS 'uia';\nCOMMENT ON INDEX public.t@t_a IS 'tia';",
			want:    map[string]string{"t@t_a": "tia", "u@t_a": "uia"},
		},
		{
			name:    "CockroachDB, the spelling its planner writes",
			dialect: "cockroachdb",
			file:    twoTableIndexes + `COMMENT ON INDEX "public"."u"@"t_a" IS 'uia';`,
			want:    map[string]string{"t@t_a": "", "u@t_a": "uia"},
		},
		{
			name:    "an index of a materialized view",
			dialect: "postgres",
			file: "CREATE TABLE orders (amount int);\nCREATE MATERIALIZED VIEW order_totals AS SELECT sum(amount) AS total FROM orders;\n" +
				"CREATE UNIQUE INDEX order_totals_total ON order_totals (total);\nCOMMENT ON INDEX order_totals_total IS 'one row';",
			want: map[string]string{"order_totals@order_totals_total": "one row"},
		},
		{
			name:    "an index of a materialized view in another schema",
			dialect: "postgres",
			file: "CREATE SCHEMA s2;\nCREATE TABLE s2.orders (amount int);\nCREATE MATERIALIZED VIEW s2.order_totals AS SELECT sum(amount) AS total FROM s2.orders;\n" +
				"CREATE UNIQUE INDEX order_totals_total ON s2.order_totals (total);\nCOMMENT ON INDEX s2.order_totals_total IS 'one row';",
			want: map[string]string{"s2.order_totals@order_totals_total": "one row"},
		},
		{
			name:    "CockroachDB, a name only one table holds yet",
			dialect: "cockroachdb",
			file: "CREATE TABLE t (a int);\nCREATE INDEX t_a ON t (a);\nCOMMENT ON INDEX t_a IS 'tia';\n" +
				"CREATE TABLE u (a int);\nCREATE INDEX t_a ON u (a);",
			want: map[string]string{"t@t_a": "tia", "u@t_a": ""},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := readWithCommentOwners([]byte(test.file), test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(indexCommentsByTable(database), qt.DeepEquals, test.want)
		})
	}
}

// A name the server would not resolve to one index is refused rather than
// applied to one: a bare name is not an index of another schema, PostgreSQL has
// no table@index, and on CockroachDB a name two tables of the schema hold is
// ambiguous until the statement names the table.
func TestRead_CommentOnIndex_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		file    string
		wantErr string
	}{
		{
			name:    "a bare name for an index of another schema",
			dialect: "postgres",
			file:    "CREATE SCHEMA s2;\nCREATE TABLE s2.t (a int);\nCREATE INDEX t_a ON s2.t (a);\nCOMMENT ON INDEX t_a IS 'x';",
			wantErr: `line 4, column 1: the schema model has no place for this statement: COMMENT ON INDEX t_a names an object this schema does not declare`,
		},
		{
			name:    "a schema the index is not in",
			dialect: "postgres",
			file:    "CREATE SCHEMA s2;\nCREATE TABLE t (a int);\nCREATE INDEX t_a ON t (a);\nCOMMENT ON INDEX s2.t_a IS 'x';",
			wantErr: `line 4, column 1: the schema model has no place for this statement: COMMENT ON INDEX s2.t_a names an object this schema does not declare`,
		},
		{
			name:    "a bare name for an index of another schema's materialized view",
			dialect: "postgres",
			file: "CREATE SCHEMA s2;\nCREATE TABLE s2.orders (amount int);\nCREATE MATERIALIZED VIEW s2.order_totals AS SELECT sum(amount) AS total FROM s2.orders;\n" +
				"CREATE UNIQUE INDEX order_totals_total ON s2.order_totals (total);\nCOMMENT ON INDEX order_totals_total IS 'x';",
			wantErr: `line 5, column 1: the schema model has no place for this statement: COMMENT ON INDEX order_totals_total names an object this schema does not declare`,
		},
		{
			name:    "PostgreSQL names no index by its table",
			dialect: "postgres",
			file:    "CREATE TABLE t (a int);\nCREATE INDEX t_a ON t (a);\nCOMMENT ON INDEX t@t_a IS 'x';",
			wantErr: `line 3, column 1: the schema model has no place for this statement: COMMENT ON INDEX t@t_a names an object this schema does not declare`,
		},
		{
			name:    "CockroachDB, a bare name two tables hold",
			dialect: "cockroachdb",
			file:    twoTableIndexes + "COMMENT ON INDEX t_a IS 'x';",
			wantErr: `line 5, column 1: the schema model has no place for this statement: COMMENT ON INDEX t_a names an index ` +
				`more than one declared table holds; write it as table@index to name one`,
		},
		{
			name:    "CockroachDB, a qualified name two tables hold",
			dialect: "cockroachdb",
			file:    twoTableIndexes + "COMMENT ON INDEX public.t_a IS 'x';",
			wantErr: `line 5, column 1: the schema model has no place for this statement: COMMENT ON INDEX public.t_a names an index ` +
				`more than one declared table holds; write it as table@index to name one`,
		},
		{
			name:    "CockroachDB, a table the document does not declare",
			dialect: "cockroachdb",
			file:    twoTableIndexes + "COMMENT ON INDEX w@t_a IS 'x';",
			wantErr: `line 5, column 1: the schema model has no place for this statement: COMMENT ON INDEX w@t_a names an object this schema does not declare`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, statements, err := readWithCommentOwners([]byte(test.file), test.dialect)

			c.Assert(err, qt.ErrorIs, sqlschema.ErrUnmodeledStatement)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(database.Tables, qt.HasLen, 0)
			c.Assert(statements, qt.IsNil)
		})
	}
}
