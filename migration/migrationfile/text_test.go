package migrationfile_test

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/migrationfile"
)

// bom is the UTF-8 byte order mark, EF BB BF, as a file written by an editor
// that marks its encoding starts with it.
const bom = "\xEF\xBB\xBF"

func TestSQLText_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		content string
		want    string
	}{
		{name: "a leading mark is removed", content: bom + "CREATE TABLE t (id INT);\n", want: "CREATE TABLE t (id INT);\n"},
		{name: "content without a mark is unchanged", content: "CREATE TABLE t (id INT);\n", want: "CREATE TABLE t (id INT);\n"},
		{name: "a mark alone is empty text", content: bom, want: ""},
		{name: "a mark later in the file is content", content: "SELECT 1;\n" + bom + "SELECT 2;\n", want: "SELECT 1;\n" + bom + "SELECT 2;\n"},
		{name: "only one leading mark is removed", content: bom + bom + "SELECT 1;\n", want: bom + "SELECT 1;\n"},
		{name: "a leading mark after whitespace is content", content: "\n" + bom + "SELECT 1;\n", want: "\n" + bom + "SELECT 1;\n"},
		{name: "a truncated mark is content", content: "\xEF\xBBSELECT 1;\n", want: "\xEF\xBBSELECT 1;\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(migrationfile.SQLText(tt.content), qt.Equals, tt.want)
		})
	}
}

// Every file a template render reads is read through SQLText: the root file
// whether or not it is a template, and a shared template it includes, whose
// mark would otherwise land in the middle of the rendered SQL.
func TestRenderAtlasTemplateSQL_ByteOrderMark(t *testing.T) {
	tests := []struct {
		name         string
		fsys         fstest.MapFS
		wantSQL      string
		wantRendered bool
	}{
		{
			name:    "plain file",
			fsys:    fstest.MapFS{"001_users.sql": {Data: []byte(bom + "CREATE TABLE users (id INT);\n")}},
			wantSQL: "CREATE TABLE users (id INT);\n",
		},
		{
			name:         "template root",
			fsys:         fstest.MapFS{"001_users.sql": {Data: []byte(bom + "CREATE TABLE {{ .Env }}_users (id INT);\n")}},
			wantSQL:      "CREATE TABLE prod_users (id INT);\n",
			wantRendered: true,
		},
		{
			name: "shared template",
			fsys: fstest.MapFS{
				"001_users.sql":    {Data: []byte("{{ template \"shared/users\" . }}SELECT 1;\n")},
				"shared/users.sql": {Data: []byte(bom + "CREATE TABLE {{ .Env }}_users (id INT);\n")},
			},
			wantSQL:      "CREATE TABLE prod_users (id INT);\nSELECT 1;\n",
			wantRendered: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			got, rendered, err := migrationfile.RenderAtlasTemplateSQL(
				tt.fsys, "001_users.sql", migrationfile.AtlasTemplateData{Env: "prod"},
			)
			c.Assert(err, qt.IsNil)
			c.Assert(rendered, qt.Equals, tt.wantRendered)
			c.Assert(got, qt.Equals, tt.wantSQL)
		})
	}
}
