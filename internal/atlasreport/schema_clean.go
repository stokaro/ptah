package atlasreport

import (
	"io"

	"github.com/stokaro/ptah/internal/schemaclean"
)

type SchemaCleanOptions struct {
	Driver  string
	URL     string
	DryRun  bool
	Applied bool
	Plan    schemaclean.Plan
}

type SchemaClean struct {
	Env     atlasEnv            `json:"Env"`
	DryRun  bool                `json:"DryRun"`
	Applied bool                `json:"Applied"`
	Objects []SchemaCleanObject `json:"Objects,omitempty"`
	Changes []SchemaCleanChange `json:"Changes,omitempty"`
}

type SchemaCleanObject struct {
	Type   string `json:"Type"`
	Schema string `json:"Schema,omitempty"`
	Table  string `json:"Table,omitempty"`
	Name   string `json:"Name"`
}

type SchemaCleanChange struct {
	Type   string `json:"Type"`
	Schema string `json:"Schema,omitempty"`
	Table  string `json:"Table,omitempty"`
	Name   string `json:"Name"`
	Cmd    string `json:"Cmd"`
}

func NewSchemaClean(opts SchemaCleanOptions) SchemaClean {
	return SchemaClean{
		Env: atlasEnv{
			Driver: opts.Driver,
			URL:    atlasRedactedURL(opts.URL),
		},
		DryRun:  opts.DryRun,
		Applied: opts.Applied,
		Objects: schemaCleanObjects(opts.Plan.Objects),
		Changes: schemaCleanChanges(opts.Plan.Changes),
	}
}

func WriteSchemaClean(w io.Writer, format string, result SchemaClean) error {
	return renderAtlasGoTemplate(w, "atlas-schema-clean-format", format, result)
}

func ValidateSchemaCleanTemplate(format string) error {
	return renderAtlasGoTemplate(io.Discard, "atlas-schema-clean-format", format, sampleSchemaClean())
}

func sampleSchemaClean() SchemaClean {
	return NewSchemaClean(SchemaCleanOptions{
		Driver:  "sqlite",
		URL:     "sqlite://sample.db?password=hidden",
		DryRun:  true,
		Applied: false,
		Plan: schemaclean.Plan{
			Objects: []schemaclean.Object{
				{Type: schemaclean.ObjectTypeTable, Name: "users"},
			},
			Changes: []schemaclean.Change{
				{Type: schemaclean.ObjectTypeTable, Name: "users", Cmd: `DROP TABLE IF EXISTS "users"`},
			},
		},
	})
}

func schemaCleanObjects(input []schemaclean.Object) []SchemaCleanObject {
	objects := make([]SchemaCleanObject, 0, len(input))
	for _, object := range input {
		objects = append(objects, SchemaCleanObject{
			Type:   object.Type,
			Schema: object.Schema,
			Table:  object.Table,
			Name:   object.Name,
		})
	}
	return objects
}

func schemaCleanChanges(input []schemaclean.Change) []SchemaCleanChange {
	changes := make([]SchemaCleanChange, 0, len(input))
	for _, change := range input {
		changes = append(changes, SchemaCleanChange{
			Type:   change.Type,
			Schema: change.Schema,
			Table:  change.Table,
			Name:   change.Name,
			Cmd:    change.Cmd,
		})
	}
	return changes
}
