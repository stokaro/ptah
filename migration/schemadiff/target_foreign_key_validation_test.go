package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// TestCompareWithDatabaseInfo_ValidatesTargetForeignKeys_FailurePath covers the
// foreign-key rules that read the whole declaration.
//
// Each of these is a relationship between two tables rather than a property of
// one change: a referenced key that is not unique, a cascade path that closes
// a cycle, a pair of columns whose types cannot be compared. The plan reads
// only the diff, so the validation runs where the whole target is supplied
// (stokaro/ptah#2315).
func TestCompareWithDatabaseInfo_ValidatesTargetForeignKeys_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		semantics identifier.Semantics
		schema    *schemamodel.Database
		wantIs    error
		wantErr   string
	}{
		{
			name:    "mysql nonunique referenced key",
			dialect: platform.MySQL,
			schema:  indexedForeignKeyDatabase(),
			wantIs:  ptaherr.ErrUnsupportedFeature,
			wantErr: `mysql requires referenced columns tenant_id, code on table "parents" to be declared unique`,
		},
		{
			name:      "sql server cascade cycle",
			dialect:   platform.SQLServer,
			semantics: sqlServerCascadeCycleSemantics(),
			schema:    cascadeCycleDatabase(),
			wantIs:    ptaherr.ErrUnsupportedFeature,
			wantErr:   `sqlserver does not allow ON DELETE cycles or multiple cascade paths reaching table .*`,
		},
		{
			name:    "postgres incompatible types",
			dialect: platform.Postgres,
			schema:  typeMismatchDatabase(),
			wantIs:  ptaherr.ErrInvalidSchemaDiff,
			wantErr: `foreign-key columns "children"\."parent_id" \(BIGINT\) and "parents"\."id" \(INTEGER\) have incompatible types`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDatabaseInfo(
				test.schema,
				&catalog.Database{},
				catalog.ServerInfo{Dialect: test.dialect, IdentifierSemantics: test.semantics},
				nil,
			)

			c.Assert(err, qt.ErrorIs, test.wantIs, qt.Commentf("error: %v", err))
			c.Assert(err, qt.ErrorMatches, `.*`+test.wantErr)
			c.Assert(diff, qt.IsNil)
		})
	}
}

func sqlServerCascadeCycleSemantics() identifier.Semantics {
	return identifier.ForSQLServerCatalog("SQL_Latin1_General_CP1_CI_AS").
		WithResolvedNames([]identifier.ResolvedName{
			{Name: "dbo", Key: "dbo"},
			{Name: "left_nodes", Key: "left_nodes"},
			{Name: "right_nodes", Key: "right_nodes"},
			{Name: "id", Key: "id"},
			{Name: "left_id", Key: "left_id"},
			{Name: "right_id", Key: "right_id"},
		})
}

func indexedForeignKeyDatabase() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Parent", Name: "parents"},
			{StructName: "Child", Name: "children"},
		},
		Fields: []schemamodel.Field{
			{StructName: "Parent", Name: "tenant_id", Type: "INTEGER"},
			{StructName: "Parent", Name: "code", Type: "INTEGER"},
			{StructName: "Child", Name: "tenant_id", Type: "INTEGER"},
			{StructName: "Child", Name: "parent_code", Type: "INTEGER"},
		},
		Indexes: []schemamodel.Index{{
			StructName: "Parent",
			Name:       "idx_parents_tenant_code",
			Fields:     []string{"tenant_id", "code"},
		}},
		Constraints: []schemamodel.Constraint{{
			StructName:     "Child",
			Name:           "fk_children_parents",
			Type:           "FOREIGN KEY",
			Columns:        []string{"tenant_id", "parent_code"},
			ForeignTable:   "parents",
			ForeignColumns: []string{"tenant_id", "code"},
		}},
	}
}

func cascadeCycleDatabase() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Left", Name: "left_nodes"},
			{StructName: "Right", Name: "right_nodes"},
		},
		Fields: []schemamodel.Field{
			{StructName: "Left", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Left", Name: "right_id", Type: "INTEGER", Foreign: "right_nodes(id)", OnDelete: "CASCADE"},
			{StructName: "Right", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Right", Name: "left_id", Type: "INTEGER", Foreign: "left_nodes(id)", OnDelete: "CASCADE"},
		},
	}
}

func typeMismatchDatabase() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Parent", Name: "parents"},
			{StructName: "Child", Name: "children"},
		},
		Fields: []schemamodel.Field{
			{StructName: "Parent", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Child", Name: "parent_id", Type: "BIGINT", Foreign: "parents(id)"},
		},
	}
}
