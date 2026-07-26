package generator_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/migration/generator"
)

func checkpointSampleSchema() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "User", Name: "users"},
			{StructName: "Post", Name: "posts"},
		},
		Fields: []goschema.Field{
			{StructName: "User", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "Post", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "Post", Name: "user_id", Type: "INTEGER", Foreign: "users(id)"},
		},
	}
}

func TestGenerateCheckpoint_UpCreatesAndDownDropsInDependencyOrder(t *testing.T) {
	c := qt.New(t)

	up, down, err := generator.GenerateCheckpoint(checkpointSampleSchema(), "postgres")
	c.Assert(err, qt.IsNil)

	// Up creates every table; the referenced table (users) comes before the
	// referencing one (posts), and the foreign key is added afterward.
	c.Assert(up, qt.Contains, `CREATE TABLE "users"`)
	c.Assert(up, qt.Contains, `CREATE TABLE "posts"`)
	c.Assert(strings.Index(up, `CREATE TABLE "users"`) < strings.Index(up, `CREATE TABLE "posts"`), qt.IsTrue)
	c.Assert(up, qt.Contains, `FOREIGN KEY ("user_id") REFERENCES "users"`)

	// Down drops in reverse dependency order: posts before users.
	c.Assert(down, qt.Contains, `DROP TABLE IF EXISTS "posts"`)
	c.Assert(down, qt.Contains, `DROP TABLE IF EXISTS "users"`)
	c.Assert(strings.Index(down, `DROP TABLE IF EXISTS "posts"`) < strings.Index(down, `DROP TABLE IF EXISTS "users"`), qt.IsTrue)
}

func TestGenerateCheckpoint_DeterministicSchemaContent(t *testing.T) {
	c := qt.New(t)

	up1, down1, err := generator.GenerateCheckpoint(checkpointSampleSchema(), "postgres")
	c.Assert(err, qt.IsNil)
	up2, down2, err := generator.GenerateCheckpoint(checkpointSampleSchema(), "postgres")
	c.Assert(err, qt.IsNil)

	// The generated DDL is deterministic; only the generated-on timestamp
	// comment varies, so compare with that line stripped.
	c.Assert(stripGeneratedOn(up1), qt.Equals, stripGeneratedOn(up2))
	c.Assert(stripGeneratedOn(down1), qt.Equals, stripGeneratedOn(down2))
}

func TestGenerateCheckpoint_NilAndEmpty(t *testing.T) {
	c := qt.New(t)

	_, _, err := generator.GenerateCheckpoint(nil, "postgres")
	c.Assert(err, qt.ErrorMatches, `checkpoint schema is required`)

	up, down, err := generator.GenerateCheckpoint(&goschema.Database{}, "postgres")
	c.Assert(err, qt.IsNil)
	c.Assert(up, qt.Equals, "")
	c.Assert(down, qt.Equals, "")
}

func stripGeneratedOn(sql string) string {
	var b strings.Builder
	for line := range strings.SplitSeq(sql, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "-- Generated on:") {
			continue
		}
		b.WriteString(line)
		b.WriteByte('\n')
	}
	return b.String()
}
