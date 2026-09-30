package migrationfile_test

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/migrationfile"
)

func TestRenderAtlasTemplateSQL_PromptPlaceholdersStayLiteral(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{name: "quoted prompt", sql: "UPDATE prompts SET body = '{{DATE}}; {{ ... }}' WHERE id = 1;"},
		{name: "dollar quoted prompt", sql: "UPDATE prompts SET body = $prompt${{DATE}}; {{ ... }}$prompt$ WHERE id = 1;"},
		{name: "comment example", sql: "-- Example: {{ ... }}\nUPDATE prompts SET body = '{{DATE}}' WHERE id = 1;"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			fsys := fstest.MapFS{"001_prompt.sql": {Data: []byte(tt.sql)}}
			got, rendered, err := migrationfile.RenderAtlasTemplateSQL(fsys, "001_prompt.sql", nil)
			c.Assert(err, qt.IsNil)
			c.Assert(rendered, qt.IsFalse)
			c.Assert(got, qt.Equals, tt.sql)
		})
	}
}

func TestRenderAtlasTemplateSQL_DotActionsStillRender(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		data any
		want string
	}{
		{name: "quoted field", sql: "SELECT '{{ .Env }}';", data: migrationfile.AtlasTemplateData{Env: "prod"}, want: "SELECT 'prod';"},
		{name: "dot", sql: "SELECT '{{ . }}';", data: "prod", want: "SELECT 'prod';"},
		{name: "compact dot", sql: "SELECT '{{.}}';", data: "prod", want: "SELECT 'prod';"},
		{name: "trimmed dot", sql: "SELECT '{{- . -}}';", data: "prod", want: "SELECT 'prod';"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			fsys := fstest.MapFS{"001_prompt.sql": {Data: []byte(tt.sql)}}
			got, rendered, err := migrationfile.RenderAtlasTemplateSQL(fsys, "001_prompt.sql", tt.data)
			c.Assert(err, qt.IsNil)
			c.Assert(rendered, qt.IsTrue)
			c.Assert(got, qt.Equals, tt.want)
		})
	}
}
