package schemadiff_test

import (
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/postgres/pgtable"
	"ptah.run/engine/builtin"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/convert/dbschematogo"
	"ptah.run/migration/schemadiff"
)

// ownershipShape is one way a target reports an object that has both a semantic
// identity and a physical backing.
//
// Each fixture matches a reader: primary keys carry IsPrimaryKey, feature-owned
// constraints mark their backing indexes, and SQL Server reports a UNIQUE
// constraint without the index its query excludes.
type ownershipShape struct {
	name string
	// skip names the dialects whose reader cannot produce this shape, with the
	// reason. A shape a reader cannot emit is a shape whose handling nothing has
	// to get right, and asserting about it measures the fixture.
	skip     map[string]string
	build    func(*catalog.Database)
	dialects []string
}

func ownershipShapes() []ownershipShape {
	return []ownershipShape{
		{
			name: "a UNIQUE constraint and the index that backs it",
			skip: map[string]string{
				"sqlserver": "the SQL Server reader's index query filters " +
					"`is_unique_constraint = 0`, so this catalog never carries both",
				"ydb": "YDB has no UNIQUE constraint (`UNIQUE (a)` in CREATE TABLE is a parse error), " +
					"so a YDB catalog carries the unique index alone",
			},
			build: func(database *catalog.Database) {
				database.Constraints = []catalog.Constraint{{
					Name: "uq_t_a", TableName: "t", Type: "UNIQUE", ColumnNames: []string{"a"},
				}}
				database.Indexes = []catalog.Index{{
					Name: "uq_t_a", TableName: "t", Columns: []string{"a"}, IsUnique: true,
					Definition: `CREATE UNIQUE INDEX "uq_t_a" ON "t" ("a")`,
				}}
			},
		},
		{
			name: "a PRIMARY KEY and the index that backs it",
			skip: map[string]string{
				"sqlserver": "the same query filters `is_primary_key = 0`",
			},
			build: func(database *catalog.Database) {
				database.Constraints = []catalog.Constraint{{
					Name: "t_pkey", TableName: "t", Type: "PRIMARY KEY", ColumnNames: []string{"id"},
				}}
				database.Indexes = []catalog.Index{{
					Name: "t_pkey", TableName: "t", Columns: []string{"id"},
					IsUnique: true, IsPrimary: true,
				}}
			},
		},
		{
			name:     "an EXCLUDE constraint and the index that backs it",
			dialects: []string{"postgres"},
			build: func(database *catalog.Database) {
				ref := pgtable.ExclusionRef("t", "t_excl", identifier.ForDialect("postgres"))
				database.FeatureCoverage = must.Must(pgtable.ExclusionCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil))
				database.FeatureObjects = must.Must(schemaext.NewObjects(schemaext.Object{
					Ref: ref, Value: &pgtable.ObservedExclusion{UsingMethod: "gist", Elements: "a WITH ="},
				}))
				database.Indexes = []catalog.Index{{
					Name: "t_excl", TableName: "t", Columns: []string{"a"}, ManagedBy: ref,
				}}
			},
		},
		{
			name: "a unique index nothing backs",
			build: func(database *catalog.Database) {
				database.Indexes = []catalog.Index{{
					Name: "ix_t_a", TableName: "t", Columns: []string{"a"}, IsUnique: true,
					Definition: `CREATE UNIQUE INDEX "ix_t_a" ON "t" ("a")`,
				}}
			},
		},
		{
			name: "a plain index",
			build: func(database *catalog.Database) {
				database.Indexes = []catalog.Index{{
					Name: "ix_t_a", TableName: "t", Columns: []string{"a"},
				}}
			},
		},
		{
			name: "a FOREIGN KEY and an index carrying its name",
			build: func(database *catalog.Database) {
				database.Tables[0].Columns = append(database.Tables[0].Columns, catalog.Column{
					Name: "p_id", DataType: "bigint", ColumnType: "bigint", IsNullable: "YES",
				})
				database.Tables = append(database.Tables, catalog.Table{
					Name: "p", Type: "TABLE",
					Columns: []catalog.Column{{
						Name: "id", DataType: "bigint", ColumnType: "bigint",
						IsNullable: "NO", IsPrimaryKey: true,
					}},
				})
				foreignTable, foreignColumn := "p", "id"
				database.Constraints = []catalog.Constraint{{
					Name: "fk_t_p", TableName: "t", Type: "FOREIGN KEY",
					ColumnName:   "p_id",
					ColumnNames:  []string{"p_id"},
					ForeignTable: &foreignTable, ForeignColumn: &foreignColumn,
					ForeignColumns: []string{"id"},
				}}
				database.Indexes = []catalog.Index{{
					Name: "fk_t_p", TableName: "t", Columns: []string{"p_id"},
				}}
			},
		},
	}
}

// ownershipCatalog is one table plus the shape's own objects.
func ownershipCatalog(shape ownershipShape) *catalog.Database {
	database := &catalog.Database{
		Tables: []catalog.Table{{
			Name: "t", Type: "TABLE",
			Columns: []catalog.Column{
				{
					Name: "id", DataType: "bigint", ColumnType: "bigint",
					IsNullable: "NO", IsPrimaryKey: true,
				},
				{Name: "a", DataType: "text", ColumnType: "text", IsNullable: "YES"},
			},
		}},
	}
	shape.build(database)
	return database
}

// ownershipCell is one shape measured against one dialect, with the pairs a
// reader cannot produce already removed.
type ownershipCell struct {
	name    string
	shape   ownershipShape
	dialect string
}

// ownershipCells is every pair worth measuring.
func ownershipCells() []ownershipCell {
	var cells []ownershipCell
	for _, shape := range ownershipShapes() {
		dialects := shape.dialects
		if len(dialects) == 0 {
			dialects = capability.DefaultDialects()
		}
		for _, dialect := range dialects {
			if _, skipped := shape.skip[dialect]; skipped {
				continue
			}
			cells = append(cells, ownershipCell{
				name:    shape.name + " on " + dialect,
				shape:   shape,
				dialect: dialect,
			})
		}
	}
	return cells
}

// TestOwnershipRoundTrip_ACatalogComparedWithItselfPlansNoObject is the
// behavioral control for stokaro/ptah#2606's problem 1, offline and on every
// declared dialect.
//
// Two resolvers answer "is this index the physical backing of that constraint"
// on the same catalog: internal/convert/dbschematogo decides which object the
// MODEL keeps, and migration/schemadiff decides which one the COMPARISON owns.
// Neither can be checked against the other by reading, because each is correct
// against its own rules. What catches a disagreement is their behavior: convert
// a catalog to a model, compare that model back against the catalog it came
// from, and require that no object is added or removed. A divergence shows up
// as exactly the duplicate or missing object the issue is about.
//
// The assertion is confined to objects on purpose. A column type folds
// differently per target -- a PostgreSQL-spelled `text` compared as Oracle
// answers `text -> clob` -- and that is a separate subject with its own tests;
// including it here would make this gate red for a reason it is not about.
func TestOwnershipRoundTrip_ACatalogComparedWithItselfPlansNoObject(t *testing.T) {
	for _, cell := range ownershipCells() {
		t.Run(cell.name, func(t *testing.T) {
			c := qt.New(t)

			// The dialect reaches BOTH sides. Until the two ownership answers
			// became one, this converter took none, so the control compared a
			// dialect-aware comparator against a dialect-blind description --
			// agreement between a pairing the product does not ship
			// (stokaro/ptah#2606).
			desired := must.Must(dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), ownershipCatalog(cell.shape), cell.dialect, builtintest.Runtime()))
			diff := must.Must(schemadiff.CompareWithDialect(t.Context(), desired, ownershipCatalog(cell.shape), cell.dialect, builtintest.Runtime()))

			changes := objectChanges(diff)
			c.Assert(changes, qt.HasLen, 0,
				qt.Commentf("a catalog compared with itself planned:\n%s",
					strings.Join(changes, "\n")))

			// The zero diff alone does not say the model kept ONE
			// representation: a model carrying both the constraint and its
			// index matches a catalog carrying both, and the comparison is
			// content. Rendering that model emits one name twice, which the
			// server refuses, so the second half is asserted on the artifact.
			doubled := doublyRepresentedObjects(desired)
			c.Assert(doubled, qt.HasLen, 0,
				qt.Commentf("the model keeps two representations of one object:\n%s",
					strings.Join(doubled, "\n")))
		})
	}
}

// TestOwnershipRoundTrip_ADroppedObjectIsStillReported is the non-vacuity
// control.
//
// The sweep above asserts that nothing is planned, which a comparison that
// planned nothing at all would satisfy for every shape. Taking the objects off
// the desired side has to bring the change back, or the sweep is measuring a
// comparison that cannot speak.
//
// The primary key needs its own removal: it does not live in
// schemamodel.Database.Constraints after conversion but on the column and the
// table, so clearing the two object lists leaves it declared.
func TestOwnershipRoundTrip_ADroppedObjectIsStillReported(t *testing.T) {
	for _, shape := range ownershipShapes() {
		t.Run(shape.name, func(t *testing.T) {
			c := qt.New(t)

			desired := must.Must(dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), ownershipCatalog(shape), "postgres", must.Must(builtin.New())))
			desired.Indexes = nil
			desired.Constraints = nil
			desired.FeatureObjects = schemaext.Objects{}
			for index := range desired.Tables {
				desired.Tables[index].PrimaryKey = nil
				desired.Tables[index].PrimaryKeyName = ""
			}
			for index := range desired.Fields {
				desired.Fields[index].Primary = false
			}

			diff := must.Must(schemadiff.CompareWithDialect(t.Context(), desired, ownershipCatalog(shape), "postgres", must.Must(builtin.New())))

			c.Assert(len(objectChanges(diff)) > 0, qt.IsTrue,
				qt.Commentf("a desired schema declaring none of the objects planned nothing"))
		})
	}
}

// doublyRepresentedObjects is every identity the model carries as both an index
// and a constraint.
//
// One physical object, one representation: a UNIQUE constraint and the index
// that backs it share a name on the target, so a model that keeps both renders
// that name twice and the server refuses the second.
func doublyRepresentedObjects(desired *schemamodel.Database) []string {
	constraints := make(map[string]struct{}, len(desired.Constraints))
	for _, constraint := range desired.Constraints {
		constraints[constraint.Table+"."+constraint.Name] = struct{}{}
	}
	for _, constraint := range schemaext.DefinedConstraints(desired.FeatureObjects) {
		constraints[schemamodel.QualifyTableName(constraint.Ref.Schema.Authored(), constraint.Ref.Parent.Source)+"."+constraint.Ref.Name.Source] = struct{}{}
	}
	var doubled []string
	for _, index := range desired.Indexes {
		identity := index.TableName + "." + index.Name
		if _, both := constraints[identity]; both {
			doubled = append(doubled, identity)
		}
	}
	return doubled
}

// objectChanges is every index and constraint the comparison plans, as one list
// a failure can name.
func objectChanges(diff *difftypes.SchemaDiff) []string {
	var changes []string
	for _, ref := range diff.IndexAdditions() {
		changes = append(changes, fmt.Sprintf("index added: %s on %s", ref.Name, ref.TableName))
	}
	for _, ref := range diff.IndexesRemoved {
		changes = append(changes, fmt.Sprintf("index removed: %s on %s", ref.Name, ref.TableName))
	}
	for _, added := range diff.ConstraintsAdded {
		changes = append(changes, fmt.Sprintf("constraint added: %s on %s", added.Name, added.TableName))
	}
	for _, removed := range diff.ConstraintsRemoved {
		changes = append(changes, fmt.Sprintf("constraint removed: %s on %s", removed.Name, removed.TableName))
	}
	for _, change := range diff.FeatureChanges {
		changes = append(changes, fmt.Sprintf("feature changed: %s", change.Subject))
	}
	for _, table := range diff.TablesModified {
		for _, change := range table.FeatureChanges {
			changes = append(changes, fmt.Sprintf("feature changed: %s", change.Subject))
		}
	}
	return changes
}
