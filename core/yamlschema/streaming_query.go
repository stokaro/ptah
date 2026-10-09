package yamlschema

import (
	"fmt"

	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbstreaming"
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
		spec := ydbstreaming.Spec{Text: string(entry.Text), Run: entry.Run, ResourcePool: string(entry.ResourcePool)}
		if err := ydbstreaming.Validate(spec); err != nil {
			return fmt.Errorf("streaming query %q: %w", name, err)
		}
		object := ydbstreaming.DesiredObject(string(entry.Schema), name, "", spec, entry.AllowStateReset)
		if err := ydbstreaming.ValidateIdentity(object.Ref); err != nil {
			return err
		}
		var err error
		db.FeatureObjects, err = db.FeatureObjects.With(object)
		if err != nil {
			return err
		}
	}
	return nil
}
