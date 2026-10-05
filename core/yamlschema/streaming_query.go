package yamlschema

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/ydbstream"
)

type streamingQuerySpec struct {
	Name            stringScalar `yaml:"name"`
	Schema          stringScalar `yaml:"schema"`
	Text            stringScalar `yaml:"text"`
	Run             *bool        `yaml:"run"`
	ResourcePool    stringScalar `yaml:"resource_pool"`
	AllowStateReset bool         `yaml:"allow_state_reset"`
}

func (d document) addStreamingQueries(db *schemamodel.Database) error {
	for _, key := range sortedKeys(d.StreamingQueries) {
		entry := d.StreamingQueries[key]
		name := string(entry.Name)
		if name == "" {
			name = key
		}
		spec := ast.StreamingQuerySpec{Text: string(entry.Text), Run: entry.Run, ResourcePool: string(entry.ResourcePool)}
		if err := ydbstream.Validate(spec); err != nil {
			return fmt.Errorf("streaming query %q: %w", name, err)
		}
		db.StreamingQueries = append(db.StreamingQueries, schemamodel.StreamingQuery{Name: name, Schema: string(entry.Schema), Spec: spec.Clone(), AllowStateReset: entry.AllowStateReset})
	}
	return nil
}
