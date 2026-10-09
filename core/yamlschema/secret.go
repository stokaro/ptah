package yamlschema

import (
	"errors"
	"fmt"
	"strings"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbsecret"
)

// secretSpec is one YDB secret in a YAML document: its directory and the
// environment variable its value comes from. Value is read only so that a
// document that writes the value is refused by name, rather than answered
// with the decoder's unknown-field error, and it is never kept or printed.
type secretSpec struct {
	Name     stringScalar  `yaml:"name"`
	Schema   stringScalar  `yaml:"schema"`
	ValueEnv stringScalar  `yaml:"value_env"`
	Value    *stringScalar `yaml:"value"`
}

// addSecrets reads the document's secrets, each checked by the rules the
// annotation parser reads a secret with.
func (d document) addSecrets(db *schemamodel.Database) error {
	for _, key := range sortedKeys(d.Secrets) {
		spec := d.Secrets[key]
		values := map[string]string{ydbsecret.AttributeValueEnv: string(spec.ValueEnv)}
		if spec.Value != nil {
			values[ydbsecret.AttributeValue] = ""
		}
		valueEnv, err := ydbsecret.ParseValueEnv(values)
		if err != nil {
			return fmt.Errorf("secret %q: %w", key, err)
		}
		name := valueOrDefault(spec.Name, key)
		if err := ydbsecret.CheckName(name); err != nil {
			return fmt.Errorf("secret %q: %w", key, err)
		}
		object := ydbsecret.DesiredObject(strings.Trim(strings.TrimSpace(string(spec.Schema)), "/"), name, "", valueEnv)
		if err := ydbsecret.ValidateIdentity(object.Ref); err != nil {
			return fmt.Errorf("secret %q: %w", key, err)
		}
		db.FeatureObjects, err = db.FeatureObjects.With(object)
		if errors.Is(err, schemaext.ErrDuplicate) {
			return fmt.Errorf("secret %q: secret %s is declared twice", key, ydbsecret.Display(object.Ref.Schema.Source, name))
		}
		if err != nil {
			return err
		}
	}
	return nil
}
