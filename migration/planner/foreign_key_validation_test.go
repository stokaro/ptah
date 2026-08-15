package planner_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

func TestGenerateSchemaDiffAST_ValidatesTargetForeignKeys_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		diff    *types.SchemaDiff
		schema  *goschema.Database
		wantIs  error
		wantErr string
	}{
		{
			name:    "mysql nonunique referenced key",
			dialect: platform.MySQL,
			diff:    &types.SchemaDiff{},
			schema:  plannerIndexedForeignKeyDatabase(),
			wantIs:  ptaherr.ErrUnsupportedFeature,
			wantErr: `mysql requires referenced columns tenant_id, code on table "parents" to be declared unique`,
		},
		{
			name:    "sql server cascade cycle",
			dialect: platform.SQLServer,
			diff:    plannerSQLServerDiff(),
			schema:  plannerCascadeCycleDatabase(),
			wantIs:  ptaherr.ErrUnsupportedFeature,
			wantErr: `sqlserver does not allow ON DELETE cycles or multiple cascade paths reaching table .*`,
		},
		{
			name:    "postgres incompatible types",
			dialect: platform.Postgres,
			diff:    &types.SchemaDiff{},
			schema:  plannerTypeMismatchDatabase(),
			wantIs:  ptaherr.ErrInvalidSchemaDiff,
			wantErr: `foreign-key columns "children"\."parent_id" \(BIGINT\) and "parents"\."id" \(INTEGER\) have incompatible types`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			nodes, err := planner.GenerateSchemaDiffAST(test.diff, test.schema, test.dialect)
			c.Assert(err, qt.ErrorIs, test.wantIs, qt.Commentf("error: %v", err))
			c.Assert(err, qt.ErrorMatches, `.*`+test.wantErr)
			c.Assert(nodes, qt.IsNil)
		})
	}
}

func plannerSQLServerDiff() *types.SchemaDiff {
	semantics := identifier.ForSQLServerCatalog("SQL_Latin1_General_CP1_CI_AS").
		WithResolvedNames([]identifier.ResolvedName{
			{Name: "dbo", Key: "dbo"},
			{Name: "left_nodes", Key: "left_nodes"},
			{Name: "right_nodes", Key: "right_nodes"},
			{Name: "id", Key: "id"},
			{Name: "left_id", Key: "left_id"},
			{Name: "right_id", Key: "right_id"},
		})
	return &types.SchemaDiff{IdentifierSemantics: &semantics}
}

func plannerIndexedForeignKeyDatabase() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Parent", Name: "parents"},
			{StructName: "Child", Name: "children"},
		},
		Fields: []goschema.Field{
			{StructName: "Parent", Name: "tenant_id", Type: "INTEGER"},
			{StructName: "Parent", Name: "code", Type: "INTEGER"},
			{StructName: "Child", Name: "tenant_id", Type: "INTEGER"},
			{StructName: "Child", Name: "parent_code", Type: "INTEGER"},
		},
		Indexes: []goschema.Index{{
			StructName: "Parent",
			Name:       "idx_parents_tenant_code",
			Fields:     []string{"tenant_id", "code"},
		}},
		Constraints: []goschema.Constraint{{
			StructName:     "Child",
			Name:           "fk_children_parents",
			Type:           "FOREIGN KEY",
			Columns:        []string{"tenant_id", "parent_code"},
			ForeignTable:   "parents",
			ForeignColumns: []string{"tenant_id", "code"},
		}},
	}
}

func plannerCascadeCycleDatabase() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Left", Name: "left_nodes"},
			{StructName: "Right", Name: "right_nodes"},
		},
		Fields: []goschema.Field{
			{StructName: "Left", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Left", Name: "right_id", Type: "INTEGER", Foreign: "right_nodes(id)", OnDelete: "CASCADE"},
			{StructName: "Right", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Right", Name: "left_id", Type: "INTEGER", Foreign: "left_nodes(id)", OnDelete: "CASCADE"},
		},
	}
}

func plannerTypeMismatchDatabase() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Parent", Name: "parents"},
			{StructName: "Child", Name: "children"},
		},
		Fields: []goschema.Field{
			{StructName: "Parent", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Child", Name: "parent_id", Type: "BIGINT", Foreign: "parents(id)"},
		},
	}
}
