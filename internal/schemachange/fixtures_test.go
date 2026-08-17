package schemachange_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/platform/identifier"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/schemachange"
	"go.5x5.cz/ptah/internal/schemastate"
)

// postgresProfile is the target most rows below run against: a folding
// identifier rule and a capability set that has foreign keys.
func postgresProfile() schemastate.Profile {
	return schemastate.Profile{
		Dialect:      "postgres",
		Semantics:    identifier.ForDialect("postgres"),
		Capabilities: capability.Postgres17(),
	}
}

// clickhouseProfile is the target that cannot host the family at all, which is
// what makes the blocked path measurable rather than hypothetical: ClickHouse
// parses no FOREIGN KEY clause, and the capability probe measures that on every
// matrix cell.
func clickhouseProfile() schemastate.Profile {
	return schemastate.Profile{
		Dialect:      "clickhouse",
		Semantics:    identifier.ForDialect("clickhouse"),
		Capabilities: capability.ClickHouse2411(),
	}
}

// parentChildDescription is the desired schema every row starts from: a parent
// with a key, and a child whose column references it.
func parentChildDescription(onDelete string) *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Parent", Name: "parent"},
			{StructName: "Child", Name: "child"},
		},
		Fields: []goschema.Field{
			{StructName: "Parent", Name: "id", Type: "int", Primary: true},
			{StructName: "Child", Name: "id", Type: "int", Primary: true},
			{
				StructName:     "Child",
				Name:           "parent_id",
				Type:           "int",
				Foreign:        "parent(id)",
				ForeignKeyName: "fk_child_parent",
				OnDelete:       onDelete,
			},
		},
	}
}

// parentChildCatalog is the same schema as a catalog reports it, with the
// referential action spelled the way an engine spells it.
func parentChildCatalog(onDelete string) *dbschematypes.DBSchema {
	parent := "parent"
	column := "id"
	return &dbschematypes.DBSchema{
		Tables: []dbschematypes.DBTable{
			{Name: "parent", Schema: "public", Columns: []dbschematypes.DBColumn{
				{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
			}},
			{Name: "child", Schema: "public", Columns: []dbschematypes.DBColumn{
				{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
				{Name: "parent_id", DataType: "integer", IsNullable: "YES"},
			}},
		},
		Constraints: []dbschematypes.DBConstraint{
			{
				Name:          "fk_child_parent",
				TableName:     "child",
				Schema:        "public",
				Type:          "FOREIGN KEY",
				ColumnName:    "parent_id",
				ColumnNames:   []string{"parent_id"},
				ForeignTable:  &parent,
				ForeignSchema: "public",
				ForeignColumn: &column,
				DeleteRule:    &onDelete,
			},
		},
	}
}

// emptyCatalog is a database with the two tables and no foreign key, which is
// the state an addition starts from.
func emptyCatalog() *dbschematypes.DBSchema {
	return &dbschematypes.DBSchema{
		Tables: []dbschematypes.DBTable{
			{Name: "parent", Schema: "public", Columns: []dbschematypes.DBColumn{
				{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
			}},
			{Name: "child", Schema: "public", Columns: []dbschematypes.DBColumn{
				{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
				{Name: "parent_id", DataType: "integer", IsNullable: "YES"},
			}},
		},
	}
}

// pipeline runs the whole prototype for one input pair, which is the thing
// under test in most rows: adapters, normalization, comparison, graph, plan.
func pipeline(
	c *qt.C,
	description *goschema.Database,
	catalog *dbschematypes.DBSchema,
	profile schemastate.Profile,
) []schemachange.PlannedOperation {
	c.Helper()
	ordered, err := orderedChanges(c, description, catalog, profile)
	c.Assert(err, qt.IsNil)
	operations, err := schemachange.Plan(ordered, profile)
	c.Assert(err, qt.IsNil)
	return operations
}

// orderedChanges runs the pipeline up to the ordered change list, for rows that
// assert on changes rather than on statements.
func orderedChanges(
	c *qt.C,
	description *goschema.Database,
	catalog *dbschematypes.DBSchema,
	profile schemastate.Profile,
) ([]schemachange.Change, error) {
	c.Helper()
	desired, current, err := states(description, catalog, profile)
	if err != nil {
		return nil, err
	}
	changes, err := schemachange.Compare(current, desired, profile)
	if err != nil {
		return nil, err
	}
	graph, err := schemachange.BuildGraph(changes, current, desired)
	if err != nil {
		return nil, err
	}
	return graph.Forward()
}

// states builds and normalizes both sides.
func states(
	description *goschema.Database,
	catalog *dbschematypes.DBSchema,
	profile schemastate.Profile,
) (desired, current *schemastate.State, err error) {
	rawDesired, err := schemastate.FromDescription(description, profile.Dialect, profile.Semantics)
	if err != nil {
		return nil, nil, err
	}
	rawCurrent, err := schemastate.FromCatalog(catalog, profile.Dialect, profile.Semantics)
	if err != nil {
		return nil, nil, err
	}
	desired, err = schemastate.Normalize(rawDesired, profile)
	if err != nil {
		return nil, nil, err
	}
	current, err = schemastate.Normalize(rawCurrent, profile)
	if err != nil {
		return nil, nil, err
	}
	return desired, current, nil
}

// statementsOf is [schemachange.Statements] with the helper marker, so a
// failing row points at the test rather than at this file.
func statementsOf(c *qt.C, operations []schemachange.PlannedOperation) []string {
	c.Helper()
	return schemachange.Statements(operations)
}

var _ = testing.Verbose
