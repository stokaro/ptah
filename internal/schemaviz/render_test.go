package schemaviz_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/schemaviz"
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

func TestRenderDOTPreservesStructuralTableIdentity(t *testing.T) {
	c := qt.New(t)
	db := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Qualified", Schema: "tenant", Name: "data"},
			{StructName: "Literal", Name: "tenant.data"},
		},
		Fields: []goschema.Field{
			{StructName: "Qualified", Name: "id", Type: "INTEGER"},
			{StructName: "Literal", Name: "id", Type: "INTEGER"},
		},
		Constraints: []goschema.Constraint{
			{
				StructName:    "Qualified",
				Name:          "qualified_to_literal",
				Type:          "FOREIGN KEY",
				Table:         "tenant.data",
				Columns:       []string{"id"},
				ForeignTable:  `"tenant.data"`,
				ForeignColumn: "id",
			},
			{
				StructName:    "Literal",
				Name:          "literal_to_qualified",
				Type:          "FOREIGN KEY",
				Table:         `"tenant.data"`,
				Columns:       []string{"id"},
				ForeignTable:  "tenant.data",
				ForeignColumn: "id",
			},
		},
	}

	output, err := schemaviz.Render(db, schemaviz.Options{Format: schemaviz.FormatDOT})

	c.Assert(err, qt.IsNil)
	text := string(output)
	c.Assert(text, qt.Contains, `  "tenant.data" -> "\"tenant.data\"" [label="qualified_to_literal"];`)
	c.Assert(text, qt.Contains, `  "\"tenant.data\"" -> "tenant.data" [label="literal_to_qualified"];`)
}

func TestRenderDOTUnqualifiedExclusionDoesNotHideLiteralDotTable(t *testing.T) {
	c := qt.New(t)
	db := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Qualified", Schema: "tenant", Name: "data"},
			{StructName: "Literal", Name: "tenant.data"},
		},
	}

	output, err := schemaviz.Render(db, schemaviz.Options{
		Format:        schemaviz.FormatDOT,
		ExcludeTables: []string{"data"},
	})

	c.Assert(err, qt.IsNil)
	text := string(output)
	c.Assert(text, qt.Contains, `  "\"tenant.data\"" [label=<`)
	c.Assert(text, qt.Not(qt.Contains), `  "tenant.data" [label=<`)
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

// TestRender_AnnotationsMarkTheNodeTheyBelongTo pins what an annotated diagram
// carries in each format.
//
// DOT can draw a marked node: the border takes the severity's color and a row
// names the codes. Mermaid's erDiagram has neither per-entity styling nor a
// display name, so the annotation is written as a comment beside the entity --
// present in the file rather than dropped, which is the honest answer for a
// format that cannot draw it (stokaro/ptah#1035).
func TestRender_AnnotationsMarkTheNodeTheyBelongTo(t *testing.T) {
	tests := []struct {
		name   string
		format string
		// wantContains is what a marked diagram has to carry.
		wantContains []string
		// wantMissing is what an UNMARKED node must not acquire, which is what
		// separates marking one node from marking the diagram.
		wantMissing []string
	}{
		{
			name:   "dot draws the mark on the node",
			format: schemaviz.FormatDOT,
			wantContains: []string{
				// The TABLE tag, not the FONT one: the border is what a reader
				// sees before reading anything, and `COLOR="..."` alone matches
				// the row's font color too -- so an assertion on it would pass
				// with the border left at its ordinary gray.
				`CELLPADDING="6" COLOR="#b45309"`,
				"warning: PRV03",
				"routine escalate: info PRV02",
			},
			wantMissing: []string{"posts</B></FONT></TD></TR>\n      <TR><TD ALIGN=\"LEFT\""},
		},
		{
			name:   "mermaid writes it beside the entity",
			format: schemaviz.FormatMermaid,
			wantContains: []string{
				"%% users: warning PRV03",
				"%% routine escalate: info PRV02",
			},
			wantMissing: []string{"%% posts:"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			output, err := schemaviz.Render(sampleDatabase(), schemaviz.Options{
				Format: test.format,
				Annotations: map[string]schemaviz.Annotation{
					"users": {Severity: schemaviz.SeverityWarning, Labels: []string{"PRV03"}},
				},
				Unattached: []string{"routine escalate: info PRV02"},
			})

			c.Assert(err, qt.IsNil)
			for _, want := range test.wantContains {
				c.Assert(string(output), qt.Contains, want)
			}
			for _, missing := range test.wantMissing {
				c.Assert(string(output), qt.Not(qt.Contains), missing)
			}
		})
	}
}

// TestRender_WithoutAnnotationsIsByteIdentical is the control.
//
// Annotations are opt-in, so every diagram rendered before they existed has to
// render the same bytes now. A renderer that marked nothing but still moved a
// color or a row would break every caller comparing its output.
func TestRender_WithoutAnnotationsIsByteIdentical(t *testing.T) {
	tests := []struct{ name, format string }{
		{name: "dot", format: schemaviz.FormatDOT},
		{name: "mermaid", format: schemaviz.FormatMermaid},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			plain, plainErr := schemaviz.Render(sampleDatabase(), schemaviz.Options{Format: test.format})
			empty, emptyErr := schemaviz.Render(sampleDatabase(), schemaviz.Options{
				Format:      test.format,
				Annotations: make(map[string]schemaviz.Annotation),
				Unattached:  make([]string, 0),
			})

			c.Assert(plainErr, qt.IsNil)
			c.Assert(emptyErr, qt.IsNil)
			c.Assert(string(empty), qt.Equals, string(plain))
			c.Assert(string(plain), qt.Not(qt.Contains), "%%")
		})
	}
}
