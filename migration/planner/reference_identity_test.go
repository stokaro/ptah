package planner_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

func TestGenerateSchemaDiffSQL_TableModificationUsesStructuralIdentity(t *testing.T) {
	c := qt.New(t)
	diff := &types.SchemaDiff{
		TablesModified: []types.TableDiff{{
			TableName: "tenant.data",
			ColumnsModified: []types.ColumnDiff{{
				ColumnName: "payload",
				Changes:    map[string]string{"type": "TEXT -> BIGINT"},
			}},
		}},
	}

	postgresSQL, err := planner.GenerateSchemaDiffSQL(diff, referenceCollisionSchema(), platform.Postgres)
	c.Assert(err, qt.IsNil)
	c.Assert(postgresSQL, qt.Contains, `ALTER TABLE "tenant"."data" ALTER COLUMN "payload" TYPE BIGINT`)
	c.Assert(postgresSQL, qt.Not(qt.Contains), `ALTER TABLE "tenant.data"`)

	mysqlSQL, err := planner.GenerateSchemaDiffSQL(diff, referenceCollisionSchema(), platform.MySQL)
	c.Assert(err, qt.IsNil)
	c.Assert(mysqlSQL, qt.Contains, "ALTER TABLE `tenant`.`data` MODIFY COLUMN `payload` BIGINT")
	c.Assert(mysqlSQL, qt.Not(qt.Contains), "ALTER TABLE `tenant.data`")
}

func TestGenerateSchemaDiffSQL_SQLiteRebuildUsesStructuralIdentity(t *testing.T) {
	c := qt.New(t)
	diff := &types.SchemaDiff{
		TablesModified: []types.TableDiff{{
			TableName:      "tenant.data",
			ColumnsRemoved: []string{"obsolete"},
		}},
	}

	sql, err := planner.GenerateSchemaDiffSQL(diff, referenceCollisionSchema(), platform.SQLite)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `FROM "tenant"."data"`)
	c.Assert(sql, qt.Not(qt.Contains), `FROM "tenant.data"`)
}

func TestGenerateSchemaDiffSQL_SQLiteTableCreationUsesStructuralIdentity(t *testing.T) {
	c := qt.New(t)
	generated := referenceCollisionSchema()
	generated.Constraints = []goschema.Constraint{
		{
			StructName:      "Literal",
			Name:            "literal_check",
			Type:            "CHECK",
			Table:           `"tenant.data"`,
			CheckExpression: "payload <> ''",
		},
		{
			StructName:      "Qualified",
			Name:            "qualified_check",
			Type:            "CHECK",
			Table:           "tenant.data",
			CheckExpression: "payload > 0",
		},
	}
	diff := &types.SchemaDiff{
		TablesAdded: []string{`"tenant.data"`, "tenant.data"},
	}

	sql, err := planner.GenerateSchemaDiffSQL(diff, generated, platform.SQLite)

	c.Assert(err, qt.IsNil)
	c.Assert(strings.Count(sql, "CREATE TABLE"), qt.Equals, 2)
	c.Assert(strings.Count(sql, `"literal_check"`), qt.Equals, 1)
	c.Assert(strings.Count(sql, `"qualified_check"`), qt.Equals, 1)
}

func TestGenerateSchemaDiffSQL_ForeignKeyPreservesStructuralIdentity(t *testing.T) {
	tests := []struct {
		name          string
		dialect       string
		wantAlter     string
		wantReference string
	}{
		{
			name:          "postgres",
			dialect:       platform.Postgres,
			wantAlter:     `ALTER TABLE "tenant.data"`,
			wantReference: `REFERENCES "tenant"."data"`,
		},
		{
			name:          "mysql",
			dialect:       platform.MySQL,
			wantAlter:     "ALTER TABLE `tenant.data`",
			wantReference: "REFERENCES `tenant`.`data`",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			generated := referenceCollisionForeignKeySchema()
			sql, err := planner.GenerateSchemaDiffSQL(
				&types.SchemaDiff{TablesAdded: []string{`"tenant.data"`, "tenant.data"}},
				generated,
				tt.dialect,
			)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, tt.wantAlter)
			c.Assert(sql, qt.Contains, tt.wantReference)
		})
	}
}

func TestGenerateSchemaDiffSQL_MySQLSelfForeignKeyTypeChangePreservesStructuralIdentity(t *testing.T) {
	c := qt.New(t)
	generated := referenceCollisionSchema()
	generated.Fields = append(generated.Fields,
		goschema.Field{StructName: "Qualified", Name: "id", Type: "BIGINT", Primary: true},
		goschema.Field{StructName: "Qualified", Name: "parent_id", Type: "BIGINT", Nullable: true},
	)
	generated.SelfReferencingForeignKeys = map[string][]goschema.SelfReferencingFK{
		"tenant.data": {{
			FieldName:      "parent_id",
			Foreign:        "tenant.data(id)",
			ForeignKeyName: "fk_qualified_parent",
		}},
	}
	diff := &types.SchemaDiff{
		TablesModified: []types.TableDiff{{
			TableName: "tenant.data",
			ColumnsModified: []types.ColumnDiff{{
				ColumnName: "parent_id",
				Changes:    map[string]string{"type": "INTEGER -> BIGINT"},
			}},
		}},
	}

	sql, err := planner.GenerateSchemaDiffSQL(diff, generated, platform.MySQL)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "ALTER TABLE `tenant`.`data` DROP FOREIGN KEY `fk_qualified_parent`")
	c.Assert(sql, qt.Contains, "ALTER TABLE `tenant`.`data` ADD CONSTRAINT `fk_qualified_parent`")
	c.Assert(sql, qt.Not(qt.Contains), "ALTER TABLE `tenant.data` DROP FOREIGN KEY")
}

func TestGenerateSchemaDiffSQL_PostgresEnumRemovalPreservesLiteralDotIdentity(t *testing.T) {
	c := qt.New(t)
	generated := &goschema.Database{
		Enums: []goschema.Enum{{Name: `"tenant.data"`, Values: []string{"active"}}},
		Tables: []goschema.Table{{
			StructName: "Literal",
			Name:       "tenant.data",
		}},
		Fields: []goschema.Field{{
			StructName: "Literal",
			Name:       "status",
			Type:       `"tenant.data"`,
		}},
	}
	diff := &types.SchemaDiff{
		EnumsModified: []types.EnumDiff{{
			EnumName:      `"tenant.data"`,
			ValuesRemoved: []string{"retired"},
		}},
	}

	sql, err := planner.GenerateSchemaDiffSQL(diff, generated, platform.Postgres)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `ALTER TYPE "tenant.data" RENAME TO "tenant.data__ptah_old"`)
	c.Assert(sql, qt.Contains, `ALTER TABLE "tenant.data" ALTER COLUMN "status" TYPE "tenant.data"`)
	c.Assert(sql, qt.Contains, `DROP TYPE "tenant.data__ptah_old"`)
	c.Assert(sql, qt.Not(qt.Contains), `"""tenant"`)
}

func TestGenerateSchemaDiffSQL_PostgresSequenceRemovalPreservesLiteralDotIdentity(t *testing.T) {
	c := qt.New(t)
	diff := &types.SchemaDiff{
		SequencesRemoved: []string{`"tenant.data"`},
	}

	sql, err := planner.GenerateSchemaDiffSQL(diff, &goschema.Database{}, platform.Postgres)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `DROP SEQUENCE IF EXISTS "tenant.data"`)
	c.Assert(sql, qt.Not(qt.Contains), `"tenant"."data"`)
}

func referenceCollisionSchema() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Literal", Name: "tenant.data"},
			{StructName: "Qualified", Schema: "tenant", Name: "data"},
		},
		Fields: []goschema.Field{
			{StructName: "Literal", Name: "payload", Type: "TEXT"},
			{StructName: "Qualified", Name: "payload", Type: "BIGINT"},
		},
	}
}

func referenceCollisionForeignKeySchema() *goschema.Database {
	database := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Literal", Name: "tenant.data"},
			{StructName: "Qualified", Schema: "tenant", Name: "data"},
		},
		Fields: []goschema.Field{
			{StructName: "Literal", Name: "id", Type: "INTEGER", Primary: true},
			{
				StructName: "Literal",
				Name:       "qualified_id",
				Type:       "INTEGER",
				Foreign:    "tenant.data(id)",
			},
			{StructName: "Qualified", Name: "id", Type: "INTEGER", Primary: true},
		},
	}
	goschema.Finalize(database)
	return database
}
