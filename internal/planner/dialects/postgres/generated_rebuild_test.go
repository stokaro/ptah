package postgres_test

import (
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/planext"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/postgres"
)

// pricesSearch is the generated column of public.prices that reads nickname.
const pricesSearch = "to_tsvector('simple'::regconfig, nickname)"

// pricesRetype is a diff that changes public.prices.nickname from varchar(40)
// to varchar(100). The table holds the stored generated column search_vector,
// which reads nickname, with the indexes and constraints given; desired lists
// the columns the declaration holds, and removed the columns the plan drops.
func pricesRetype(indexes []catalog.Index, constraints []catalog.Constraint, desired, removed []string) *difftypes.SchemaDiff {
	fields := map[string]schemamodel.Field{
		"id":       {StructName: "Price", Name: "id", Type: "integer", Primary: true},
		"nickname": {StructName: "Price", Name: "nickname", Type: "varchar(100)", Nullable: true},
		"search_vector": {
			StructName: "Price", Name: "search_vector", Type: "tsvector", Nullable: true, Comment: "search",
			GeneratedExpression: pricesSearch, GeneratedKind: "STORED",
		},
	}
	declaration := schemacapture.TableDeclaration{Table: schemamodel.Table{StructName: "Price", Name: "public.prices"}}
	for _, name := range desired {
		declaration.Fields = append(declaration.Fields, fields[name])
	}
	var removals []schemamodel.Field
	for _, name := range removed {
		removals = append(removals, fields[name])
	}
	return &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
		TableName: "public.prices",
		ColumnsModified: []difftypes.ColumnDiff{{
			ColumnName: "nickname",
			Changes:    map[string]string{"type": "varchar(40) -> varchar(100)"},
			Desired:    fields["nickname"],
		}},
		ColumnsRemoved: removals,
		Current: schemacapture.TableObservation{
			Table: catalog.Table{Name: "prices", Schema: "public", Type: "BASE TABLE", Columns: []catalog.Column{
				{Name: "id", DataType: "integer", UDTName: "int4"},
				{Name: "nickname", DataType: "character varying", UDTName: "varchar", IsNullable: "YES"},
				{
					Name: "search_vector", DataType: "tsvector", UDTName: "tsvector", IsNullable: "YES", Comment: "search",
					GeneratedExpression: new(pricesSearch), GeneratedKind: "STORED",
				},
			}},
			Indexes:     indexes,
			Constraints: constraints,
		},
		Desired: declaration,
	}}}
}

// searchIndexes are a GIN index and a partial GIN index on search_vector, as
// the catalog reports them, and an index on nickname alone.
var searchIndexes = []catalog.Index{
	{
		Name: "prices_search", TableName: "prices", Schema: "public", Columns: []string{"search_vector"},
		Definition: "CREATE INDEX prices_search ON public.prices USING gin (search_vector)", Comment: "full text",
	},
	{
		Name: "user_search_index", TableName: "prices", Schema: "public", Columns: []string{"search_vector"},
		Definition: "CREATE INDEX user_search_index ON public.prices USING gin (search_vector) WHERE (id > 0)",
		Condition:  "(id > 0)",
	},
	{
		Name: "prices_nickname", TableName: "prices", Schema: "public", Columns: []string{"nickname"},
		Definition: "CREATE INDEX prices_nickname ON public.prices USING btree (nickname)",
	},
}

// searchCheck is a CHECK constraint that reads search_vector.
var searchCheck = catalog.Constraint{
	Name: "prices_search_present", TableName: "prices", Schema: "public", Type: "CHECK",
	ColumnName: "search_vector", CheckClause: new("(search_vector IS NOT NULL)"), Comment: "present",
}

// planPrices plans diff for the PostgreSQL family target dialect, in the exact
// statement style the commands plan in.
func planPrices(c *qt.C, dialect string, diff *difftypes.SchemaDiff) (string, error) {
	c.Helper()
	planner := postgres.NewForDialect(dialect, capability.Postgres18()).WithStatementStyle(planext.StatementStyleExact)
	nodes, err := planner.GenerateMigrationAST(c.Context(), must.Must(builtin.New()), diff)
	if err != nil {
		return "", err
	}
	return builtin.RenderSQLWithCapabilities(platform.Postgres, capability.Postgres18(), nodes...)
}

// PostgreSQL refuses a type change of a column a generated column reads, so
// the plan takes the generated column out of its table first and puts it back
// after the change. Its indexes, its CHECK constraints and the comments on all
// three come back as the catalog holds them, the index on nickname alone is
// left alone, and the guard ahead of the drop leaves out exactly what the plan
// restates.
func TestPlanner_GeneratedColumnRebuild_HappyPath(t *testing.T) {
	c := qt.New(t)
	diff := pricesRetype(searchIndexes, []catalog.Constraint{searchCheck}, []string{"id", "nickname", "search_vector"}, nil)

	sql, err := planPrices(c, platform.Postgres, diff)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, `-- Rebuild generated column public.prices.search_vector around the type change of nickname
SELECT ('refusing to rebuild generated column public.prices.search_vector around the type change of nickname: dropping it would take ' || string_agg(dependent, ', ' ORDER BY dependent) || ', which the plan does not restate')::integer
FROM (
    SELECT pg_describe_object(d.classid, d.objid, d.objsubid) AS dependent
    FROM pg_depend d
    JOIN pg_attribute a ON a.attrelid = d.refobjid AND a.attnum = d.refobjsubid
    WHERE d.refclassid = 'pg_class'::regclass AND d.refobjid = '"public"."prices"'::regclass AND a.attname = 'search_vector'
      AND d.deptype IN ('n', 'a') AND d.classid <> 'pg_attrdef'::regclass
      AND NOT (d.classid = 'pg_class'::regclass AND d.objid = ANY (ARRAY[to_regclass('"public"."prices_search"'), to_regclass('"public"."user_search_index"')]::oid[]))
      AND NOT (d.classid = 'pg_constraint'::regclass AND d.objid IN (
          SELECT oid FROM pg_constraint WHERE conrelid = '"public"."prices"'::regclass AND (contype = 'n' OR conname = ANY (ARRAY['prices_search_present']::text[]))))
    UNION ALL
    SELECT 'the privileges or storage options of the column'
    FROM pg_attribute a JOIN pg_type t ON t.oid = a.atttypid
    WHERE a.attrelid = '"public"."prices"'::regclass AND a.attname = 'search_vector'
      AND (a.attacl IS NOT NULL OR EXISTS (SELECT 1 FROM unnest(a.attoptions) o WHERE o NOT LIKE 'pg\_%')
           OR COALESCE(a.attstattarget::int, -1) <> -1
           OR COALESCE(to_jsonb(a) ->> 'attcompression', '') <> '' OR a.attstorage <> t.typstorage)
    UNION ALL
    SELECT 'the security label of the column'
    FROM pg_seclabel l JOIN pg_attribute a ON a.attrelid = l.objoid AND a.attnum = l.objsubid
    WHERE l.classoid = 'pg_class'::regclass AND l.objoid = '"public"."prices"'::regclass AND a.attname = 'search_vector'
) dependents
HAVING count(*) > 0;
ALTER TABLE "public"."prices" DROP COLUMN "search_vector";

-- Add/modify columns for table: public.prices
-- Modify column public.prices.nickname: type: varchar(40) -> varchar(100)
ALTER TABLE "public"."prices" ALTER COLUMN "nickname" TYPE varchar(100);

ALTER TABLE "public"."prices" ADD COLUMN "search_vector" tsvector GENERATED ALWAYS AS (to_tsvector('simple'::regconfig, nickname)) STORED;

COMMENT ON COLUMN "public"."prices"."search_vector" IS 'search';

CREATE INDEX prices_search ON "public"."prices" USING gin (search_vector);
COMMENT ON INDEX "public"."prices_search" IS 'full text';
CREATE INDEX user_search_index ON "public"."prices" USING gin (search_vector) WHERE (id > 0);
ALTER TABLE "public"."prices" ADD CONSTRAINT "prices_search_present" CHECK ((search_vector IS NOT NULL));
COMMENT ON CONSTRAINT "prices_search_present" ON "public"."prices" IS 'present';
`)
}

// labelsRetype is a diff that changes public.labels.nickname from varchar(40)
// to varchar(100). The table holds two stored generated columns that read
// nickname, a NOT NULL and b, an index on (a, b) and a CHECK that a <> b.
func labelsRetype() *difftypes.SchemaDiff {
	fields := []schemamodel.Field{
		{StructName: "Label", Name: "id", Type: "integer", Primary: true},
		{StructName: "Label", Name: "nickname", Type: "varchar(100)", Nullable: true},
		{StructName: "Label", Name: "a", Type: "text", GeneratedExpression: "upper(nickname)", GeneratedKind: "STORED"},
		{StructName: "Label", Name: "b", Type: "text", Nullable: true, GeneratedExpression: "lower(nickname)", GeneratedKind: "STORED"},
	}
	return &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
		TableName: "public.labels",
		ColumnsModified: []difftypes.ColumnDiff{{
			ColumnName: "nickname",
			Changes:    map[string]string{"type": "varchar(40) -> varchar(100)"},
			Desired:    fields[1],
		}},
		Current: schemacapture.TableObservation{
			Table: catalog.Table{Name: "labels", Schema: "public", Type: "BASE TABLE", Columns: []catalog.Column{
				{Name: "id", DataType: "integer", UDTName: "int4"},
				{Name: "nickname", DataType: "character varying", UDTName: "varchar", IsNullable: "YES"},
				{
					Name: "a", DataType: "text", UDTName: "text", IsNullable: "NO",
					GeneratedExpression: new("upper((nickname)::text)"), GeneratedKind: "STORED",
				},
				{
					Name: "b", DataType: "text", UDTName: "text", IsNullable: "YES",
					GeneratedExpression: new("lower((nickname)::text)"), GeneratedKind: "STORED",
				},
			}},
			Indexes: []catalog.Index{{
				Name: "labels_ab", TableName: "labels", Schema: "public", Columns: []string{"a", "b"},
				Definition: "CREATE INDEX labels_ab ON public.labels USING btree (a, b)",
			}},
			Constraints: []catalog.Constraint{{
				Name: "labels_a_b", TableName: "labels", Schema: "public", Type: "CHECK", CheckClause: new("(a <> b)"),
			}},
		},
		Desired: schemacapture.TableDeclaration{Table: schemamodel.Table{StructName: "Label", Name: "public.labels"}, Fields: fields},
	}}}
}

// statementsOf keeps the ALTER TABLE and CREATE INDEX lines of sql, the
// statements whose order the server checks.
func statementsOf(sql string) []string {
	return slices.DeleteFunc(strings.Split(sql, "\n"), func(line string) bool {
		return !strings.HasPrefix(line, "ALTER TABLE ") && !strings.HasPrefix(line, "CREATE INDEX ")
	})
}

// An index and a CHECK over two rebuilt generated columns read both, and each
// drop takes them. The plan adds both columns back before it restates either,
// and restates each once: the server refuses an index on a column it does not
// have yet, and a second index under the same name.
func TestPlanner_GeneratedColumnRebuildOfTwoColumns_HappyPath(t *testing.T) {
	c := qt.New(t)

	sql, err := planPrices(c, platform.Postgres, labelsRetype())

	c.Assert(err, qt.IsNil)
	c.Assert(statementsOf(sql), qt.DeepEquals, []string{
		`ALTER TABLE "public"."labels" DROP COLUMN "a";`,
		`ALTER TABLE "public"."labels" DROP COLUMN "b";`,
		`ALTER TABLE "public"."labels" ALTER COLUMN "nickname" TYPE varchar(100);`,
		`ALTER TABLE "public"."labels" ADD COLUMN "a" text NOT NULL GENERATED ALWAYS AS (upper(nickname)) STORED;`,
		`ALTER TABLE "public"."labels" ADD COLUMN "b" text GENERATED ALWAYS AS (lower(nickname)) STORED;`,
		`CREATE INDEX labels_ab ON "public"."labels" USING btree (a, b);`,
		`ALTER TABLE "public"."labels" ADD CONSTRAINT "labels_a_b" CHECK ((a <> b));`,
	})
}

// An index whose definition does not have the shape pg_get_indexdef writes is
// not restated: the guard does not leave it out, so the rebuild is refused
// naming it rather than dropping it.
func TestPlanner_GeneratedColumnRebuildOfAnUnreadIndex_HappyPath(t *testing.T) {
	c := qt.New(t)
	unread := catalog.Index{
		Name: "prices_search", TableName: "prices", Schema: "public", Columns: []string{"search_vector"},
		Definition: "an index the reader did not describe",
	}

	sql, err := planPrices(c, platform.Postgres, pricesRetype([]catalog.Index{unread}, nil, []string{"id", "nickname", "search_vector"}, nil))

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "AND NOT (d.classid = 'pg_class'::regclass AND d.objid = ANY (ARRAY[]::oid[]))")
	c.Assert(sql, qt.Not(qt.Contains), "an index the reader did not describe")
}

// A rebuild is refused where it cannot put the column back as it was, and on
// CockroachDB, whose catalog cannot tell the guard what the drop takes.
func TestPlanner_GeneratedColumnRebuild_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		desired []string
		removed []string
		want    string
	}{
		{
			name:    "the plan also removes the generated column",
			dialect: platform.Postgres,
			desired: []string{"id", "nickname"},
			removed: []string{"search_vector"},
			want:    `.*generated column public.prices.search_vector reads column nickname, whose type this plan changes, and the plan also removes it: .*`,
		},
		{
			name:    "the declaration does not hold the generated column",
			dialect: platform.Postgres,
			desired: []string{"id", "nickname"},
			want:    `.*generated column public.prices.search_vector reads column nickname, whose type this plan changes, and the declaration of the table does not hold it, .*`,
		},
		{
			name:    "CockroachDB",
			dialect: platform.CockroachDB,
			desired: []string{"id", "nickname", "search_vector"},
			want:    `.*generated column public.prices.search_vector reads column nickname, whose type this plan changes; CockroachDB refuses the change .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := pricesRetype(searchIndexes, nil, test.desired, test.removed)

			sql, err := planPrices(c, test.dialect, diff)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(sql, qt.Equals, "")
		})
	}
}

// A collation change is made by the TYPE clause too, and PostgreSQL refuses it
// alike while a generated column reads the column ("cannot alter type of a
// column used by a generated column"), so the plan rebuilds the generated
// column around it.
func TestPlanner_GeneratedColumnRebuildAroundACollationChange_HappyPath(t *testing.T) {
	c := qt.New(t)
	diff := pricesRetype(nil, nil, []string{"id", "nickname", "search_vector"}, nil)
	modified := &diff.TablesModified[0].ColumnsModified[0]
	modified.Changes = map[string]string{"collation": " -> C"}
	modified.Desired.Type, modified.Desired.Collate = "varchar(40)", "C"

	sql, err := planPrices(c, platform.Postgres, diff)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "-- Rebuild generated column public.prices.search_vector around the type change of nickname\n")
	c.Assert(sql, qt.Contains, "ALTER TABLE \"public\".\"prices\" DROP COLUMN \"search_vector\";\n")
	c.Assert(sql, qt.Contains, "ALTER TABLE \"public\".\"prices\" ALTER COLUMN \"nickname\" TYPE varchar(40) COLLATE \"C\";\n")
	c.Assert(strings.Index(sql, "DROP COLUMN \"search_vector\"") < strings.Index(sql, "COLLATE \"C\""), qt.IsTrue)
}

// A generated column with a collation of its own comes back with the
// collation its declaration gives it: the add writes the declared column, and
// the guard leaves the collation to it. A declaration that gives the column
// none brings it back under its type's collation, the one the comparison holds
// an undeclared column to.
func TestPlanner_GeneratedColumnRebuildRestatesItsCollation_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		want     string
	}{
		{name: "the collation it holds", declared: "C", want: "ALTER TABLE \"public\".\"prices\" ADD COLUMN \"label\" text COLLATE \"C\" GENERATED ALWAYS AS"},
		{name: "none", want: "ALTER TABLE \"public\".\"prices\" ADD COLUMN \"label\" text GENERATED ALWAYS AS"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := pricesRetype(nil, nil, []string{"id", "nickname", "search_vector"}, nil)
			declared := &diff.TablesModified[0].Desired.Fields[2]
			declared.Name, declared.Type, declared.Collate, declared.GeneratedExpression = "label", "text", test.declared, "lower(nickname)"
			declared.Comment = ""
			held := &diff.TablesModified[0].Current.Table.Columns[2]
			held.Name, held.DataType, held.UDTName, held.Collate, held.GeneratedExpression = "label", "text", "text", "C", new("lower((nickname)::text)")
			held.Comment = ""

			sql, err := planPrices(c, platform.Postgres, diff)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, "ALTER TABLE \"public\".\"prices\" DROP COLUMN \"label\";\n")
			c.Assert(sql, qt.Contains, test.want)
			c.Assert(sql, qt.Not(qt.Contains), "pg_collation")
		})
	}
}
