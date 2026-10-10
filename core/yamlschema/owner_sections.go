package yamlschema

import (
	"bytes"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"

	"go.yaml.in/yaml/v3"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/yamlext"
)

// ownKeys are the top-level keys the frontend reads itself, from the yaml tags
// of [document]. An owner section that took one of them would never be
// handed to its owner.
var ownKeys = sync.OnceValue(func() []string {
	var keys []string
	for field := range reflect.TypeFor[document]().Fields() {
		name, _, _ := strings.Cut(field.Tag.Get("yaml"), ",")
		if name != "" {
			keys = append(keys, name)
		}
	}
	return keys
})

// checkOwnedKeys refuses a top-level key the frontend does not read and no
// selected owner reads, naming its line, as the decoder refuses an unknown key
// of the frontend's own types. It refuses an owner section that takes the
// name of one of the frontend's own keys too.
func checkOwnedKeys(owners yamlext.Set, data []byte, owned map[string]yaml.Node) error {
	for _, key := range owners.SectionKeys() {
		if slices.Contains(ownKeys(), key) {
			owner, _ := owners.Section(key)
			return fmt.Errorf("parse YAML schema: YAML key %q of %s takes the name of one of the frontend's own", key, owner)
		}
	}
	for _, key := range sortedKeys(owned) {
		if _, read := owners.Section(key); !read {
			return fmt.Errorf("parse YAML schema: line %d: unknown key %q", keyLine(data, key), key)
		}
	}
	return nil
}

// keyLine returns the line a top-level key is written on, or 0 when the
// document does not parse as a mapping.
func keyLine(data []byte, key string) int {
	var root yaml.Node
	if err := yaml.Unmarshal(data, &root); err != nil || len(root.Content) == 0 {
		return 0
	}
	mapping := root.Content[0]
	for index := 0; index+1 < len(mapping.Content); index += 2 {
		if mapping.Content[index].Value == key {
			return mapping.Content[index].Line
		}
	}
	return 0
}

// decodeSection unmarshals the value of the top-level key in data into
// target, refusing a key target's type does not declare. It decodes the whole
// document again, so a refusal names the line the author wrote.
func decodeSection(data []byte, key string, target any) error {
	value := reflect.ValueOf(target)
	if value.Kind() != reflect.Pointer || value.IsNil() {
		return fmt.Errorf("decode YAML key %q: the target is not a non-nil pointer", key)
	}
	wrapper := reflect.New(reflect.StructOf([]reflect.StructField{
		{Name: "Section", Type: value.Type().Elem(), Tag: reflect.StructTag(fmt.Sprintf("yaml:%q", key))},
		{Name: "Rest", Type: reflect.TypeFor[map[string]yaml.Node](), Tag: `yaml:",inline"`},
	}))
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(wrapper.Interface()); err != nil {
		return err
	}
	value.Elem().Set(wrapper.Elem().Field(0))
	return nil
}

// addOwnerSections hands each owner section of the document to the owner
// that reads it, and joins what the owner declares: an object to the
// document's feature objects, a facet to the table it names. data is the
// document, which the owner's decoder reads its section from.
func (d document) addOwnerSections(db *schemamodel.Database, owners yamlext.Set, data []byte) error {
	tables := d.documentTables(db)
	for _, key := range sortedKeys(d.Owned) {
		contributions, err := owners.Decode(key, func(target any) error { return decodeSection(data, key, target) }, tables)
		if err != nil {
			return err
		}
		for _, contribution := range contributions {
			if err := contribute(db, key, contribution); err != nil {
				return err
			}
		}
	}
	return nil
}

// documentTables are the document's tables as an owner reads them. db holds
// them in the order of their keys.
func (d document) documentTables(db *schemamodel.Database) yamlext.Tables {
	tables := make(yamlext.Tables, 0, len(db.Tables))
	for index, key := range sortedKeys(d.Tables) {
		table := db.Tables[index]
		tables = append(tables, yamlext.Table{Key: key, Schema: table.Schema, Name: table.Name, Struct: table.StructName})
	}
	return tables
}

// contribute joins what an owner declared to the document's schema. key
// names what declared it in a refusal.
func contribute(db *schemamodel.Database, key string, contribution yamlext.Contribution) error {
	if contribution.Object != nil {
		objects, err := db.FeatureObjects.With(*contribution.Object)
		if err != nil {
			return fmt.Errorf("%s: %s: %w", key, contribution.Label, err)
		}
		db.FeatureObjects = objects
		return nil
	}
	table := &db.Tables[contribution.Table]
	facets, err := table.Facets.With(contribution.Facet)
	if err != nil {
		return fmt.Errorf("%s: table %q declares %s twice: %w", key, table.Name, contribution.Label, schemaext.ErrDuplicate)
	}
	if len(contribution.Targets) > 0 {
		if facets, err = facets.WithTargetScope(contribution.Facet.Kind(), contribution.Targets...); err != nil {
			return err
		}
	}
	table.Facets = facets
	return nil
}
