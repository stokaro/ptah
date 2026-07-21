package schemaviz_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/internal/schemaviz"
)

func TestRenderMermaidWithColumnsAndRelationships(t *testing.T) {
	c := qt.New(t)

	output, err := schemaviz.Render(sampleDatabase(), schemaviz.Options{
		Format:         schemaviz.FormatMermaid,
		IncludeColumns: true,
	})

	c.Assert(err, qt.IsNil)
	text := string(output)
	c.Assert(text, qt.Contains, "erDiagram\n")
	c.Assert(text, qt.Contains, "  users {\n")
	c.Assert(text, qt.Contains, "    SERIAL id PK\n")
	c.Assert(text, qt.Contains, "    INTEGER author_id FK\n")
	c.Assert(text, qt.Contains, `  users ||--o{ posts : "fk_posts_author"`)
	c.Assert(text, qt.Contains, `  users ||--o{ audit_logs : "fk_audit_logs_user_id"`)
}

func TestRenderDOTExcludesTablesAndRelationships(t *testing.T) {
	c := qt.New(t)

	output, err := schemaviz.Render(sampleDatabase(), schemaviz.Options{
		Format:         schemaviz.FormatDOT,
		IncludeColumns: true,
		ExcludeTables:  []string{"audit_logs"},
		Theme:          schemaviz.ThemeDark,
	})

	c.Assert(err, qt.IsNil)
	text := string(output)
	c.Assert(text, qt.Contains, "digraph ptah_schema")
	c.Assert(text, qt.Contains, "bgcolor=\"#111827\"")
	c.Assert(text, qt.Contains, `"posts" -> "users" [label="fk_posts_author"]`)
	c.Assert(text, qt.Not(qt.Contains), "audit_logs")
}

func TestRenderDeduplicatesConcreteFieldsAndRelationships(t *testing.T) {
	c := qt.New(t)
	db := sampleDatabase()
	db.Fields = append(db.Fields,
		goschema.Field{StructName: "Post", Name: "author_id", Type: "INTEGER", Foreign: "users(id)", ForeignKeyName: "fk_posts_author"},
	)
	db.Constraints = append(db.Constraints, goschema.Constraint{
		StructName:    "Post",
		Name:          "fk_posts_author",
		Type:          "FOREIGN KEY",
		Columns:       []string{"author_id"},
		ForeignTable:  "users",
		ForeignColumn: "id",
	})

	output, err := schemaviz.Render(db, schemaviz.Options{
		Format:         schemaviz.FormatMermaid,
		IncludeColumns: true,
	})

	c.Assert(err, qt.IsNil)
	text := string(output)
	c.Assert(strings.Count(text, "    INTEGER author_id FK\n"), qt.Equals, 1)
	c.Assert(strings.Count(text, `  users ||--o{ posts : "fk_posts_author"`), qt.Equals, 1)
}

func TestRenderMermaidAvoidsSanitizedNameCollisions(t *testing.T) {
	c := qt.New(t)
	db := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "AuthUser", Schema: "auth", Name: "users"},
			{StructName: "AuditUser", Name: "auth_users"},
			{StructName: "ArchiveUser", Name: "auth_users_2"},
		},
		Fields: []goschema.Field{
			{StructName: "AuthUser", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "AuditUser", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "AuditUser", Name: "user_id", Type: "INTEGER", Foreign: "auth.users(id)"},
			{StructName: "ArchiveUser", Name: "id", Type: "SERIAL", Primary: true},
		},
	}

	output, err := schemaviz.Render(db, schemaviz.Options{Format: schemaviz.FormatMermaid})

	c.Assert(err, qt.IsNil)
	text := string(output)
	c.Assert(strings.Count(text, "  auth_users {\n"), qt.Equals, 1)
	c.Assert(strings.Count(text, "  auth_users_2 {\n"), qt.Equals, 1)
	c.Assert(strings.Count(text, "  auth_users_2_2 {\n"), qt.Equals, 1)
	c.Assert(text, qt.Contains, `  auth_users ||--o{ auth_users_2 : "fk_auth_users_user_id"`)
}

func TestRenderRejectsBadOptions(t *testing.T) {
	c := qt.New(t)

	_, err := schemaviz.Render(sampleDatabase(), schemaviz.Options{Format: "json"})
	c.Assert(err, qt.ErrorMatches, `unsupported visualization format "json": expected dot or mermaid`)

	_, err = schemaviz.Render(sampleDatabase(), schemaviz.Options{Format: schemaviz.FormatDOT, Theme: "sepia"})
	c.Assert(err, qt.ErrorMatches, `unsupported visualization theme "sepia": expected light or dark`)
}

func sampleDatabase() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "User", Name: "users"},
			{StructName: "Post", Name: "posts"},
			{StructName: "AuditLog", Name: "audit_logs"},
		},
		Fields: []goschema.Field{
			{StructName: "User", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "User", Name: "email", Type: "TEXT"},
			{StructName: "Post", Name: "id", Type: "SERIAL", Primary: true},
			{
				StructName:     "Post",
				Name:           "author_id",
				Type:           "INTEGER",
				Foreign:        "users(id)",
				ForeignKeyName: "fk_posts_author",
			},
			{StructName: "AuditLog", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "AuditLog", Name: "user_id", Type: "INTEGER"},
		},
		Constraints: []goschema.Constraint{{
			StructName:    "AuditLog",
			Type:          "FOREIGN KEY",
			Columns:       []string{"user_id"},
			ForeignTable:  "users",
			ForeignColumn: "id",
		}},
	}
}
