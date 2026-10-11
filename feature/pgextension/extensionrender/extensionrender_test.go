package extensionrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/feature/pgextension"
	"ptah.run/feature/pgextension/extensionrender"
)

func render(target string, payload ast.ExtensionPayload) ([]string, error) {
	return must.Must(extensionrender.Registry()).Render(renderer.ExtensionContext{Target: target}, ast.StatementExtension, payload)
}

func TestRender_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		target  string
		payload ast.ExtensionPayload
		want    []string
	}{
		{
			name: "a bare creation", target: "postgres",
			payload: &pgextension.Operation{Name: "pg_trgm", Action: pgextension.Create},
			want:    []string{`CREATE EXTENSION "pg_trgm";`},
		},
		{
			name: "a full creation", target: "yugabytedb",
			payload: &pgextension.Operation{Name: "postgis", Action: pgextension.Create, Schema: "geo", Version: "3.0", IfNotExists: true},
			want:    []string{`CREATE EXTENSION IF NOT EXISTS "postgis" WITH SCHEMA "geo" VERSION '3.0';`},
		},
		{
			// A dot in an extension's name separates nothing.
			name: "a dotted name", target: "postgres",
			payload: &pgextension.Operation{Name: "audit.tools", Action: pgextension.Create},
			want:    []string{`CREATE EXTENSION "audit.tools";`},
		},
		{
			name: "an update", target: "postgres",
			payload: &pgextension.Operation{Name: "pg_trgm", Action: pgextension.UpdateVersion, Version: "1.6"},
			want:    []string{`ALTER EXTENSION "pg_trgm" UPDATE TO '1.6';`},
		},
		{
			name: "a move", target: "postgres",
			payload: &pgextension.Operation{Name: "pg_trgm", Action: pgextension.SetSchema, Schema: "ext"},
			want:    []string{`ALTER EXTENSION "pg_trgm" SET SCHEMA "ext";`},
		},
		{
			name: "a drop", target: "postgres",
			payload: &pgextension.Operation{Name: "pg_trgm", Action: pgextension.Drop},
			want:    []string{`DROP EXTENSION IF EXISTS "pg_trgm";`},
		},
		{
			name: "a creation where alterations are refused", target: "cockroachdb",
			payload: &pgextension.Operation{Name: "pg_trgm", Action: pgextension.Create, IfNotExists: true},
			want:    []string{`CREATE EXTENSION IF NOT EXISTS "pg_trgm";`},
		},
		{
			name: "a comment", target: "postgres",
			payload: &pgextension.CommentOperation{Name: "pg_trgm", Comment: "it's trigrams"},
			want:    []string{`COMMENT ON EXTENSION "pg_trgm" IS 'it''s trigrams';`},
		},
		{
			name: "a removed comment", target: "postgres",
			payload: &pgextension.CommentOperation{Name: "pg_trgm"},
			want:    []string{`COMMENT ON EXTENSION "pg_trgm" IS NULL;`},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := render(test.target, test.payload)

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, test.want)
		})
	}
}

func TestRender_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		target  string
		payload ast.ExtensionPayload
		is      error
		want    string
	}{
		{
			name: "an installation schema on CockroachDB", target: "cockroachdb",
			payload: &pgextension.Operation{Name: "pg_trgm", Action: pgextension.Create, Schema: "ext"},
			is:      ptaherr.ErrUnsupportedFeature,
			want:    `.*cockroachdb does not support PostgreSQL extension installation schema "ext" for extension "pg_trgm"`,
		},
		{
			name: "an update on Spanner", target: "spanner",
			payload: &pgextension.Operation{Name: "pg_trgm", Action: pgextension.UpdateVersion, Version: "1.6"},
			is:      ptaherr.ErrUnsupportedFeature,
			want:    `.*spanner cannot alter PostgreSQL extension "pg_trgm"; this target has no ALTER EXTENSION`,
		},
		{
			name: "a move on CockroachDB", target: "cockroachdb",
			payload: &pgextension.Operation{Name: "pg_trgm", Action: pgextension.SetSchema, Schema: "ext"},
			is:      ptaherr.ErrUnsupportedFeature,
			want:    `.*cockroachdb cannot alter PostgreSQL extension "pg_trgm"; this target has no ALTER EXTENSION`,
		},
		{
			name: "a target outside the family", target: "mysql",
			payload: &pgextension.Operation{Name: "pg_trgm", Action: pgextension.Create},
			is:      ptaherr.ErrUnsupportedFeature,
			want:    `.*target "mysql" does not support extension "ptah.run/pgextension/extension-operation" in role "statement"`,
		},
		{
			name: "a comment outside the family", target: "sqlite",
			payload: &pgextension.CommentOperation{Name: "pg_trgm"},
			is:      ptaherr.ErrUnsupportedFeature,
			want:    `.*target "sqlite" does not support extension "ptah.run/pgextension/extension-comment-operation" in role "statement"`,
		},
		{
			name: "an invalid operation", target: "postgres",
			payload: &pgextension.Operation{Name: "pg_trgm", Action: pgextension.SetSchema},
			is:      ptaherr.ErrInvalidSchemaDiff,
			want:    `.*an extension move names its schema and nothing else`,
		},
		{
			name: "an invalid comment", target: "postgres",
			payload: &pgextension.CommentOperation{Name: ""},
			is:      ptaherr.ErrInvalidSchemaDiff,
			want:    `.*identified by its name alone.*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := render(test.target, test.payload)

			c.Assert(err, qt.ErrorIs, test.is)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(statements, qt.IsNil)
		})
	}
}

func TestHostsAlterations(t *testing.T) {
	tests := []struct {
		target string
		want   bool
	}{
		{target: "postgres", want: true},
		{target: "postgresql", want: true},
		{target: "yugabytedb", want: true},
		{target: "cockroachdb", want: false},
		{target: "spanner", want: false},
		{target: "mysql", want: false},
	}
	for _, test := range tests {
		t.Run(test.target, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(extensionrender.HostsAlterations(test.target), qt.Equals, test.want)
		})
	}
}
