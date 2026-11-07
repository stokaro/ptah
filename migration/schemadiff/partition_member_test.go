package schemadiff_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/postgres/pgtable"
	"ptah.run/engine/builtin"
	"ptah.run/internal/sqlschema"
	"ptah.run/migration/schemadiff"
)

const partitionBound = "FOR VALUES FROM (1) TO (10)"

// partitionDocument reads a document the way a PostgreSQL schema file reads, with
// the coverage that says a table it leaves out is one it wants removed.
func partitionDocument(c *qt.C, source string) *schemamodel.Database {
	c.Helper()
	runtime := must.Must(builtin.New())
	database, _, err := sqlschema.ReadWith(c.Context(), []byte(source), "postgres", nil,
		sqlschema.Options{Owners: runtime.SQL(), Vocabulary: runtime.CoverageVocabulary(), Relations: runtime})
	c.Assert(err, qt.IsNil)
	return &database
}

// partitionedCatalog is what a read returns for events and its partition e1, with
// the partition's copies of the parent's key and check marked as the catalog marks
// them, and one check of the partition's own.
func partitionedCatalog() *catalog.Database {
	clause, own := "(id > 0)", "(id < 5)"
	key := func(table string) catalog.Table {
		return catalog.Table{Name: table, Type: "BASE TABLE", Columns: []catalog.Column{
			{Name: "id", DataType: "bigint", UDTName: "int8", IsNullable: "NO", IsPrimaryKey: true},
			{Name: "note", DataType: "text", UDTName: "text", IsNullable: "YES"},
		}}
	}
	parent, partition := key("events"), key("e1")
	parent.Partitioned = true
	parent.Facets = must.Must(schemaext.NewFacets(&pgtable.ObservedPartition{Type: "RANGE", Parts: []pgtable.PartitionPart{{Name: "id"}}}))
	partition.Parents = []string{"events"}
	partition.Facets = must.Must(schemaext.NewFacets(&pgtable.ObservedPartitionMember{Parent: "events", Bound: partitionBound}))
	return &catalog.Database{
		Tables: []catalog.Table{parent, partition},
		Constraints: []catalog.Constraint{
			{TableName: "events", Name: "events_pkey", Type: "PRIMARY KEY", ColumnName: "id", ColumnNames: []string{"id"}},
			{TableName: "events", Name: "id_positive", Type: "CHECK", CheckClause: &clause},
			{TableName: "e1", Name: "e1_pkey", Type: "PRIMARY KEY", ColumnName: "id", ColumnNames: []string{"id"}, Inherited: true},
			{TableName: "e1", Name: "id_positive", Type: "CHECK", CheckClause: &clause, Inherited: true},
			{TableName: "e1", Name: "id_small", Type: "CHECK", CheckClause: &own},
		},
		FeatureCoverage: observedTableCoverage(),
	}
}

// observedTableCoverage says what a read of PostgreSQL says about its tables: it
// looked at all the owner's table settings.
func observedTableCoverage() schemaext.Coverage {
	complete := schemaext.Knowledge{State: schemaext.Complete}
	coverage := must.Must(pgtable.PartitionMemberCoverage(schemaext.Observed, complete, nil))
	coverage = must.Must(coverage.Combine(must.Must(pgtable.PartitionCoverage(schemaext.Observed, complete, nil))))
	return must.Must(coverage.Combine(must.Must(pgtable.PersistenceCoverage(schemaext.Observed, complete, nil))))
}

const partitionedParent = `
CREATE TABLE events (id bigint NOT NULL PRIMARY KEY, note text, CONSTRAINT id_positive CHECK (id > 0)) PARTITION BY RANGE (id);
`

func comparePartitioned(c *qt.C, source string) *difftypes.SchemaDiff {
	c.Helper()
	diff, err := schemadiff.CompareWithDialect(c.Context(), partitionDocument(c, source), partitionedCatalog(), "postgres", must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)
	return diff
}

func removedConstraints(diff *difftypes.SchemaDiff) []string {
	names := make([]string, 0, len(diff.ConstraintsRemoved))
	for _, removal := range diff.ConstraintsRemoved {
		names = append(names, removal.TableName+"."+removal.Name)
	}
	slices.Sort(names)
	return names
}

// TestCompare_DeclaredPartitionComparesNoColumns pins what a partition with no column
// list means. The columns the catalog lists are its parent's, so none is removed, and
// the copies of the parent's constraints are no statement's to drop. The check the
// partition holds for itself is a difference, which is the control: the skip is not of
// the whole table.
func TestCompare_DeclaredPartitionComparesNoColumns(t *testing.T) {
	c := qt.New(t)

	diff := comparePartitioned(c, partitionedParent+`CREATE TABLE e1 PARTITION OF events FOR VALUES FROM (1) TO (10);`)

	c.Assert(diff.TablesModified, qt.HasLen, 0)
	c.Assert(diff.TablesAdded, qt.HasLen, 0)
	c.Assert(diff.TablesRemoved, qt.HasLen, 0)
	c.Assert(removedConstraints(diff), qt.DeepEquals, []string{"e1.id_small"})
}

// TestCompare_DeclaredPartitionKeepsItsOwnConstraint is the same comparison with the
// partition's own check declared, which leaves nothing to do.
func TestCompare_DeclaredPartitionKeepsItsOwnConstraint(t *testing.T) {
	c := qt.New(t)

	diff := comparePartitioned(c, partitionedParent+`CREATE TABLE e1 PARTITION OF events FOR VALUES FROM (1) TO (10);
ALTER TABLE e1 ADD CONSTRAINT id_small CHECK (id < 5);`)

	c.Assert(removedConstraints(diff), qt.HasLen, 0)
	c.Assert(diff.ConstraintsAdded, qt.HasLen, 0)
	c.Assert(diff.HasChanges(), qt.IsFalse)
}

// TestCompare_DeclaredPartitionStillComparesItsComment holds a table-level property
// apart from the column list: a comment belongs to the partition.
func TestCompare_DeclaredPartitionStillComparesItsComment(t *testing.T) {
	c := qt.New(t)

	diff := comparePartitioned(c, partitionedParent+`CREATE TABLE e1 PARTITION OF events FOR VALUES FROM (1) TO (10);
ALTER TABLE e1 ADD CONSTRAINT id_small CHECK (id < 5);
COMMENT ON TABLE e1 IS 'first';`)

	c.Assert(diff.TablesModified, qt.HasLen, 1)
	c.Assert(diff.TablesModified[0].TableName, qt.Equals, "e1")
	c.Assert(diff.TablesModified[0].CommentChange, qt.IsNotNil)
	c.Assert(diff.TablesModified[0].ColumnsRemoved, qt.HasLen, 0)
}

// TestCompare_PartitionAddedAndRemoved covers both directions a partition tree grows
// and shrinks in. A partition the document leaves out is an ordinary table removal,
// which is what a destructive gate reads.
func TestCompare_PartitionAddedAndRemoved(t *testing.T) {
	c := qt.New(t)

	removed := comparePartitioned(c, partitionedParent)
	added := comparePartitioned(c, partitionedParent+`CREATE TABLE e1 PARTITION OF events FOR VALUES FROM (1) TO (10);
ALTER TABLE e1 ADD CONSTRAINT id_small CHECK (id < 5);
CREATE TABLE e2 PARTITION OF events FOR VALUES FROM (10) TO (20);`)

	c.Assert(removed.TablesRemoved.Names(), qt.DeepEquals, []string{"e1"})
	c.Assert(removedConstraints(removed), qt.DeepEquals, []string{"e1.id_small"})
	c.Assert(added.TablesRemoved, qt.HasLen, 0)
	c.Assert(added.TablesAdded, qt.HasLen, 1)
	c.Assert(added.TablesAdded[0].Name, qt.Equals, "e2")
}

// TestCompare_SourceWithNoPartitionSyntaxKeepsComparingColumns is the control for a
// source that cannot declare a partition. A live partition declared as a table with
// columns compares as one, with no claim about its membership, and one it leaves out
// is still dropped.
func TestCompare_SourceWithNoPartitionSyntaxKeepsComparingColumns(t *testing.T) {
	runtime := must.Must(builtin.New())
	table := func(structName, name string) schemamodel.Table {
		return schemamodel.Table{StructName: structName, Name: name}
	}
	field := func(structName, name, kind string, nullable bool) schemamodel.Field {
		return schemamodel.Field{StructName: structName, Name: name, Type: kind, Nullable: nullable}
	}
	for _, test := range []struct {
		name        string
		tables      []schemamodel.Table
		fields      []schemamodel.Field
		wantRemoved []string
		wantColumns int
	}{
		{name: "declared with the same columns", tables: []schemamodel.Table{table("Events", "events"), table("E1", "e1")},
			fields: []schemamodel.Field{field("Events", "id", "bigint", false), field("Events", "note", "text", true), field("E1", "id", "bigint", false), field("E1", "note", "text", true)}},
		{name: "declared with a column less", tables: []schemamodel.Table{table("Events", "events"), table("E1", "e1")},
			fields:      []schemamodel.Field{field("Events", "id", "bigint", false), field("Events", "note", "text", true), field("E1", "id", "bigint", false)},
			wantColumns: 1},
		{name: "left out", tables: []schemamodel.Table{table("Events", "events")},
			fields:      []schemamodel.Field{field("Events", "id", "bigint", false), field("Events", "note", "text", true)},
			wantRemoved: []string{"e1"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDialect(t.Context(), &schemamodel.Database{Tables: test.tables, Fields: test.fields}, partitionedCatalog(), "postgres", runtime)

			c.Assert(err, qt.IsNil)
			c.Assert(diff.TablesRemoved.Names(), qt.DeepEquals, test.wantRemoved)
			columns := 0
			for _, modified := range diff.TablesModified {
				columns += len(modified.ColumnsRemoved)
			}
			c.Assert(columns, qt.Equals, test.wantColumns)
		})
	}
}

// A partition the document gives another parent, or declares as an ordinary
// table, is a move on the kept table: the diff carries it as a change of its
// partition membership, with the parent it holds and the one it is given.
func TestCompare_PartitionMembership_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name, source string
		want         *pgtable.PartitionMemberChange
	}{
		{name: "another parent", source: partitionedParent + `CREATE TABLE other (id bigint) PARTITION BY LIST (id);
CREATE TABLE e1 PARTITION OF other FOR VALUES IN (1);`,
			want: &pgtable.PartitionMemberChange{Before: &pgtable.ObservedPartitionMember{Parent: "events", Bound: "FOR VALUES FROM (1) TO (10)"},
				After: &pgtable.DesiredPartitionMember{PartitionMember: pgtable.PartitionMember{Parent: "other", Bound: "FOR VALUES IN (1)"}}}},
		{name: "an ordinary table", source: partitionedParent + `CREATE TABLE e1 (id bigint NOT NULL, note text);`,
			want: &pgtable.PartitionMemberChange{Before: &pgtable.ObservedPartitionMember{Parent: "events", Bound: "FOR VALUES FROM (1) TO (10)"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDialect(t.Context(), partitionDocument(c, test.source), partitionedCatalog(), "postgres", must.Must(builtin.New()))

			c.Assert(err, qt.IsNil)
			c.Assert(diff.TablesModified, qt.HasLen, 1)
			c.Assert(diff.TablesModified[0].TableName, qt.Equals, "e1")
			c.Assert(diff.TablesModified[0].FeatureChanges, qt.HasLen, 1)
			c.Assert(diff.TablesModified[0].FeatureChanges[0].Value, qt.DeepEquals, schemaext.ChangeValue(test.want))
		})
	}
}

func TestCompare_PartitionMembership_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name, source, want string
	}{
		{name: "a parent the document removes", source: `CREATE TABLE e1 PARTITION OF events FOR VALUES FROM (1) TO (10);`,
			want: `.*table public\.e1 is declared a partition of public\.events, which the database holds and the desired schema does not declare; dropping a partitioned table drops its partitions`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDialect(t.Context(), partitionDocument(c, test.source), partitionedCatalog(), "postgres", must.Must(builtin.New()))

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(diff, qt.IsNil)
		})
	}
}

// TestCompare_BoundSpelledAnotherWayStaysUndecidedWithoutAServer asks nothing of a
// server: two spellings of one bound are neither a change nor the same bound until
// one says so.
func TestCompare_BoundSpelledAnotherWayStaysUndecidedWithoutAServer(t *testing.T) {
	c := qt.New(t)

	diff, err := schemadiff.CompareWithDialect(t.Context(), partitionDocument(c, partitionedParent+`CREATE TABLE e1 PARTITION OF events FOR VALUES FROM ('1') TO ('10');
ALTER TABLE e1 ADD CONSTRAINT id_small CHECK (id < 5);`), partitionedCatalog(), "postgres", must.Must(builtin.New()))

	c.Assert(err, qt.ErrorMatches, `schema comparison is incomplete: .*the partition bound needs normalization by the connected server`)
	c.Assert(diff, qt.IsNil)
}

// TestCompare_SelectStarOverAPartitionExpandsItsParentsColumns reads a `SELECT *` over
// a partition as the catalog stores it, with the columns the partition has from its
// parent. Without them the star stays a star, and every run replaces the view.
func TestCompare_SelectStarOverAPartitionExpandsItsParentsColumns(t *testing.T) {
	c := qt.New(t)
	live := partitionedCatalog()
	live.Views = []catalog.View{{Name: "recent", Body: "SELECT e1.id,\n    e1.note\n   FROM e1;"}}

	diff, err := schemadiff.CompareWithDialect(t.Context(), partitionDocument(c, partitionedParent+`CREATE TABLE e1 PARTITION OF events FOR VALUES FROM (1) TO (10);
ALTER TABLE e1 ADD CONSTRAINT id_small CHECK (id < 5);
CREATE VIEW recent AS SELECT * FROM e1;`), live, "postgres", must.Must(builtin.New()))

	c.Assert(err, qt.IsNil)
	c.Assert(diff.ViewsModified, qt.HasLen, 0)
	c.Assert(diff.ViewsAdded, qt.HasLen, 0)
}
