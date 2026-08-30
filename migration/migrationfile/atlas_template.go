package migrationfile

import (
	"bytes"
	"fmt"
	"io/fs"
	"path"
	"regexp"
	"strings"
	"text/template"
)

var atlasTemplateActionRe = regexp.MustCompile(`{{-?\s*(define|template|if|else|end|range|with|block|\$|\.)\b?`)

// AtlasTemplateData is the default data object used for Atlas SQL templates.
type AtlasTemplateData struct {
	Env string
}

// LooksAtlasTemplateSQL reports whether sql contains Go template actions.
func LooksAtlasTemplateSQL(sql string) bool {
	return atlasTemplateActionRe.MatchString(sql)
}

// RenderAtlasTemplateSQL renders an Atlas SQL template migration file.
//
// Shared templates may live anywhere in fsys, subdirectories included. One is
// referenced by its path with the .sql extension stripped, so
// {{ template "shared/users" . }} names shared/users.sql. A root file that
// uses no template action at all is returned unchanged with rendered=false.
//
// A nil data argument renders with a zero [AtlasTemplateData]. A key that
// map-typed data does not carry renders as the map's zero element value rather
// than failing the render; a field the data's struct type does not declare —
// [AtlasTemplateData] included — still fails the render.
func RenderAtlasTemplateSQL(fsys fs.FS, filename string, data any) (sql string, rendered bool, err error) {
	raw, err := fs.ReadFile(fsys, filename)
	if err != nil {
		return "", false, fmt.Errorf("failed to read migration file: %w", err)
	}
	if !LooksAtlasTemplateSQL(string(raw)) {
		return string(raw), false, nil
	}

	renderedSQL, err := renderAtlasTemplateSQL(fsys, filename, data)
	if err != nil {
		return "", false, err
	}
	return renderedSQL, true, nil
}

func renderAtlasTemplateSQL(fsys fs.FS, filename string, data any) (string, error) {
	rootName := path.Clean(filename)
	tmpl := template.New(rootName).Option("missingkey=zero")

	err := fs.WalkDir(fsys, ".", func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() || !strings.EqualFold(path.Ext(p), ".sql") {
			return nil
		}
		raw, err := fs.ReadFile(fsys, p)
		if err != nil {
			return fmt.Errorf("failed to read SQL template %s: %w", p, err)
		}
		if path.Clean(p) != rootName && !LooksAtlasTemplateSQL(string(raw)) {
			return nil
		}

		name := path.Clean(p)
		if name != rootName {
			name = atlasTemplateReferenceName(name)
		}
		if _, err := tmpl.New(name).Parse(string(raw)); err != nil {
			return fmt.Errorf("failed to parse SQL template %s: %w", p, err)
		}
		return nil
	})
	if err != nil {
		return "", err
	}

	var out bytes.Buffer
	if err := tmpl.ExecuteTemplate(&out, rootName, atlasTemplateData(data)); err != nil {
		return "", fmt.Errorf("failed to render SQL template %s: %w", filename, err)
	}
	return out.String(), nil
}

func atlasTemplateReferenceName(filename string) string {
	return strings.TrimSuffix(filename, path.Ext(filename))
}

func atlasTemplateData(data any) any {
	if data != nil {
		return data
	}
	return AtlasTemplateData{}
}
