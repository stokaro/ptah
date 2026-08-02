package generator

// White-box testing required: these tests exercise the unexported SQL-only
// generation stage without introducing filesystem or live-database setup from
// the exported migration-file API.

import (
	"regexp"
	"strings"
	"testing"
	"unicode/utf8"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/platform"
	dbschematypes "github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/migration/schemadiff"
)

var generatorForeignKeyNamePattern = regexp.MustCompile(`fk_[[:alnum:]_]+_[0-9a-f]{8}`)

func TestGenerateUpMigrationSQL_AssignsLengthLimitedForeignKeyNames(t *testing.T) {
	c := qt.New(t)
	tableName := strings.Repeat("children_", 6) + "records"
	fieldName := strings.Repeat("parent_", 5) + "id"
	schema := generatorForeignKeyNameSchema(tableName, fieldName)
	goschema.Finalize(schema)
	diff := schemadiff.CompareWithDialect(schema, &dbschematypes.DBSchema{}, platform.MySQL)

	sql, err := generateUpMigrationSQL(diff, schema, platform.MySQL)

	c.Assert(err, qt.IsNil)
	matches := generatorForeignKeyNamePattern.FindAllString(sql, -1)
	c.Assert(matches, qt.HasLen, 1)
	c.Assert(utf8.RuneCountInString(matches[0]), qt.Equals, 64)
	c.Assert(schema.Fields[2].ForeignKeyName, qt.Equals, "")
}

func TestGenerateUpMigrationSQL_AvoidsExplicitAndGeneratedForeignKeyNameCollision(t *testing.T) {
	c := qt.New(t)
	schema := generatorForeignKeyNameSchema("children", "parent_id")
	schema.Fields = append(schema.Fields, goschema.Field{
		StructName:     "Child",
		Name:           "backup_parent_id",
		Type:           "INTEGER",
		Foreign:        "parents(id)",
		ForeignKeyName: "FK_CHILDREN_PARENT_ID",
	})
	goschema.Finalize(schema)
	diff := schemadiff.CompareWithDialect(schema, &dbschematypes.DBSchema{}, platform.MySQL)

	sql, err := generateUpMigrationSQL(diff, schema, platform.MySQL)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "FK_CHILDREN_PARENT_ID")
	c.Assert(sql, qt.Matches, `(?s).*fk_children_parent_id_[0-9a-f]{8}.*`)
	c.Assert(strings.Count(strings.ToLower(sql), "add constraint"), qt.Equals, 2)
	c.Assert(schema.Fields[2].ForeignKeyName, qt.Equals, "")
}

func generatorForeignKeyNameSchema(tableName, fieldName string) *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Parent", Name: "parents"},
			{StructName: "Child", Name: tableName},
		},
		Fields: []goschema.Field{
			{StructName: "Parent", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Child", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Child", Name: fieldName, Type: "INTEGER", Foreign: "parents(id)"},
		},
	}
}
