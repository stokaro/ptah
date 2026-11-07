package builtin_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/postgres/pgtable"
	"ptah.run/engine/builtin"
)

func partitionNode(bound string, facets ...schemaext.Value) *ast.CreateTableNode {
	node := ast.NewCreateTable("e1")
	values := append([]schemaext.Value{&pgtable.DesiredPartitionMember{PartitionMember: pgtable.PartitionMember{Parent: "events", Bound: bound}}}, facets...)
	node.Facets = must.Must(schemaext.NewFacets(values...))
	return node
}

// TestPartitionOf_RendersAsTheStatementThatDeclaresIt runs on each target with the
// statement. A declaration from a read also holds the columns the catalog listed,
// which the clause takes the place of.
func TestPartitionOf_RendersAsTheStatementThatDeclaresIt(t *testing.T) {
	inRange := "FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')"
	for _, target := range []string{"postgres", "yugabytedb"} {
		for _, test := range []struct {
			name string
			node *ast.CreateTableNode
			want string
		}{
			{name: "a bound", node: partitionNode(inRange), want: "CREATE TABLE \"e1\" PARTITION OF \"events\" " + inRange + ";\n\n"},
			{name: "the default partition", node: partitionNode("DEFAULT"), want: "CREATE TABLE \"e1\" PARTITION OF \"events\" DEFAULT;\n\n"},
			{name: "columns from a read render nowhere", node: partitionNode("DEFAULT").AddColumn(ast.NewColumn("id", "bigint")),
				want: "CREATE TABLE \"e1\" PARTITION OF \"events\" DEFAULT;\n\n"},
			{name: "a partition that is partitioned", node: partitionNode("DEFAULT", &pgtable.DesiredPartition{Type: "RANGE", Parts: []pgtable.PartitionPart{{Name: "day"}}}),
				want: "CREATE TABLE \"e1\" PARTITION OF \"events\" DEFAULT PARTITION BY RANGE (\"day\");\n\n"},
			{name: "a guard", node: partitionNode("DEFAULT").SetIfNotExists(), want: "CREATE TABLE IF NOT EXISTS \"e1\" PARTITION OF \"events\" DEFAULT;\n\n"},
		} {
			t.Run(target+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)

				sql, omissions, err := builtin.RenderSQLReportingOmissions(target, capability.ForDialect(target), test.node)

				c.Assert(err, qt.IsNil)
				c.Assert(omissions, qt.HasLen, 0)
				c.Assert(sql, qt.Equals, test.want)
			})
		}
	}
}

// TestPartitionOf_RendersItsOwnConstraintAfterIt keeps what a clause with no column
// list has no place for: a key or a check of the partition's own.
func TestPartitionOf_RendersItsOwnConstraintAfterIt(t *testing.T) {
	c := qt.New(t)
	node := partitionNode("DEFAULT")
	node.AddConstraint(&ast.ConstraintNode{Type: ast.CheckConstraint, Name: "small", Expression: "id < 5"})
	node.Comment = "the rest"

	sql, _, err := builtin.RenderSQLReportingOmissions("postgres", capability.ForDialect("postgres"), node)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, "CREATE TABLE \"e1\" PARTITION OF \"events\" DEFAULT;\n\n"+
		"ALTER TABLE \"e1\" ADD CONSTRAINT \"small\" CHECK (id < 5);\n"+
		"COMMENT ON TABLE \"e1\" IS 'the rest';\n\n")
}

// TestPartitionOf_RendersItsOwnPrimaryKeyAfterIt covers a single-column key, which an
// ordinary table writes on its column and a partition has no column to write on.
func TestPartitionOf_RendersItsOwnPrimaryKeyAfterIt(t *testing.T) {
	c := qt.New(t)
	table := schemamodel.Table{StructName: "E1", Name: "e1", PrimaryKey: []string{"id"},
		Facets: must.Must(schemaext.NewFacets(&pgtable.DesiredPartitionMember{PartitionMember: pgtable.PartitionMember{Parent: "events", Bound: "DEFAULT"}}))}
	parent := schemamodel.Table{StructName: "Events", Name: "events",
		Facets: must.Must(schemaext.NewFacets(&pgtable.DesiredPartition{Type: "LIST", Parts: []pgtable.PartitionPart{{Name: "id"}}}))}
	database := &schemamodel.Database{Tables: []schemamodel.Table{parent, table}, Fields: []schemamodel.Field{{StructName: "Events", Name: "id", Type: "bigint"}}}
	schemamodel.Finalize(database)

	sql, err := builtin.GetOrderedCreateStatements(database, "postgres")

	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(sql, "\n"), qt.Contains, "CREATE TABLE \"e1\" PARTITION OF \"events\" DEFAULT;\n\nALTER TABLE \"e1\" ADD PRIMARY KEY (\"id\");")
}

// TestPartitionOf_QuotesTheParentAsItsCatalogSpellsIt writes a parent in another
// schema and one that needs its quotes.
func TestPartitionOf_QuotesTheParentAsItsCatalogSpellsIt(t *testing.T) {
	c := qt.New(t)
	node := ast.NewCreateTable("e1")
	node.Facets = must.Must(schemaext.NewFacets(&pgtable.DesiredPartitionMember{PartitionMember: pgtable.PartitionMember{ParentSchema: "My App", Parent: "Events.Log", Bound: "DEFAULT"}}))

	sql, _, err := builtin.RenderSQLReportingOmissions("postgres", capability.ForDialect("postgres"), node)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `CREATE TABLE "e1" PARTITION OF "My App"."Events.Log" DEFAULT;`)
}

// TestPartitionOf_FailurePath refuses a declared partition on every target with no
// such statement. A partition key is reported as omitted on some of them, and that
// is no answer here: a table with no columns and no parent is nothing they can create.
func TestPartitionOf_FailurePath(t *testing.T) {
	for _, target := range []string{"mysql", "mariadb", "sqlite", "sqlserver", "oracle", "clickhouse", "cockroachdb"} {
		t.Run(target, func(t *testing.T) {
			c := qt.New(t)

			sql, _, err := builtin.RenderSQLReportingOmissions(target, capability.ForDialect(target), partitionNode("DEFAULT"))

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `.*CREATE TABLE \.\.\. PARTITION OF is a PostgreSQL partition declaration, which `+target+` has no statement for`)
			c.Assert(sql, qt.Equals, "")
		})
	}
}

// partitionKeysSchema holds a parent with a key, a partition of it, and a table whose
// foreign key the row names.
func partitionKeysSchema(constraint schemamodel.Constraint) *schemamodel.Database {
	database := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Events", Name: "events", PrimaryKey: []string{"id", "day"},
				Facets: must.Must(schemaext.NewFacets(&pgtable.DesiredPartition{Type: "RANGE", Parts: []pgtable.PartitionPart{{Name: "day"}}}))},
			{StructName: "E1", Name: "e1", Facets: must.Must(schemaext.NewFacets(&pgtable.DesiredPartitionMember{PartitionMember: pgtable.PartitionMember{Parent: "events", Bound: "DEFAULT"}}))},
			{StructName: "Holder", Name: "holder"},
		},
		Fields: []schemamodel.Field{
			{StructName: "Events", Name: "id", Type: "bigint"},
			{StructName: "Events", Name: "day", Type: "date"},
			{StructName: "Holder", Name: "event", Type: "bigint"},
			{StructName: "Holder", Name: "day", Type: "date"},
		},
		Constraints: []schemamodel.Constraint{constraint},
	}
	schemamodel.Finalize(database)
	return database
}

// TestPartitionOf_ForeignKeysAreCheckedAgainstTheParent reads a partition's columns and
// keys from its parent. A key declared on the partition, and a key that references it,
// are what the server takes.
func TestPartitionOf_ForeignKeysAreCheckedAgainstTheParent(t *testing.T) {
	for _, test := range []struct {
		name       string
		constraint schemamodel.Constraint
	}{
		{name: "a reference to a partition", constraint: schemamodel.Constraint{Type: "FOREIGN KEY", Name: "holder_event", StructName: "Holder", Table: "holder",
			Columns: []string{"event", "day"}, ForeignTable: "e1", ForeignColumns: []string{"id", "day"}, ForeignColumn: "id"}},
		{name: "a key on a partition", constraint: schemamodel.Constraint{Type: "FOREIGN KEY", Name: "e1_self", StructName: "E1", Table: "e1",
			Columns: []string{"id", "day"}, ForeignTable: "events", ForeignColumns: []string{"id", "day"}, ForeignColumn: "id"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := builtin.GetOrderedCreateStatements(partitionKeysSchema(test.constraint), "postgres")

			c.Assert(err, qt.IsNil)
			c.Assert(strings.Join(statements, "\n"), qt.Contains, `ADD CONSTRAINT "`+test.constraint.Name+`" FOREIGN KEY`)
		})
	}
}

func TestPartitionOf_ForeignKeys_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name       string
		constraint schemamodel.Constraint
		want       string
	}{
		{name: "a referenced column the parent does not have", constraint: schemamodel.Constraint{Type: "FOREIGN KEY", Name: "holder_event", StructName: "Holder", Table: "holder",
			Columns: []string{"event", "day"}, ForeignTable: "e1", ForeignColumns: []string{"nope", "day"}, ForeignColumn: "nope"},
			want: `.*referenced table "e1" has no column "nope"`},
		{name: "a local column the parent does not have", constraint: schemamodel.Constraint{Type: "FOREIGN KEY", Name: "e1_self", StructName: "E1", Table: "e1",
			Columns: []string{"nope", "day"}, ForeignTable: "events", ForeignColumns: []string{"id", "day"}, ForeignColumn: "id"},
			want: `.*table "e1" has no local foreign-key column "nope"`},
		{name: "a reference to columns neither the partition nor its parent keys", constraint: schemamodel.Constraint{Type: "FOREIGN KEY", Name: "holder_event", StructName: "Holder", Table: "holder",
			Columns: []string{"day"}, ForeignTable: "e1", ForeignColumns: []string{"day"}, ForeignColumn: "day"},
			want: `.*requires referenced columns day on table "e1" to be declared unique`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := builtin.GetOrderedCreateStatements(partitionKeysSchema(test.constraint), "postgres")

			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}

// TestPartitionOf_ForeignKeyOnAPartitionOfAnUndeclaredParent asks nothing a declaration
// cannot answer. The parent's columns are the server's to know, so a key on the
// partition renders and the server decides.
func TestPartitionOf_ForeignKeyOnAPartitionOfAnUndeclaredParent(t *testing.T) {
	c := qt.New(t)
	database := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "E1", Name: "e1", Facets: must.Must(schemaext.NewFacets(&pgtable.DesiredPartitionMember{PartitionMember: pgtable.PartitionMember{Parent: "events", Bound: "DEFAULT"}}))},
			{StructName: "Holder", Name: "holder", PrimaryKey: []string{"event"}},
		},
		Fields: []schemamodel.Field{{StructName: "Holder", Name: "event", Type: "bigint"}},
		Constraints: []schemamodel.Constraint{{Type: "FOREIGN KEY", Name: "e1_holder", StructName: "E1", Table: "e1",
			Columns: []string{"event"}, ForeignTable: "holder", ForeignColumns: []string{"event"}, ForeignColumn: "event"}},
	}
	schemamodel.Finalize(database)

	statements, err := builtin.GetOrderedCreateStatements(database, "postgres")

	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(statements, "\n"), qt.Contains, `ALTER TABLE "e1" ADD CONSTRAINT "e1_holder" FOREIGN KEY`)
}
