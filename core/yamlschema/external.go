package yamlschema

import (
	"fmt"
	"strings"

	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbexternal"
)

// externalDataSourceSpec is one YDB external data source in a YAML document.
type externalDataSourceSpec struct {
	Name       stringScalar            `yaml:"name"`
	Schema     stringScalar            `yaml:"schema"`
	SourceType stringScalar            `yaml:"source_type"`
	Location   stringScalar            `yaml:"location"`
	AuthMethod stringScalar            `yaml:"auth_method"`
	Options    map[string]stringScalar `yaml:"options"`
}

// externalTableSpec is one YDB external table in a YAML document.
type externalTableSpec struct {
	Name       stringScalar            `yaml:"name"`
	Schema     stringScalar            `yaml:"schema"`
	DataSource stringScalar            `yaml:"data_source"`
	Location   stringScalar            `yaml:"location"`
	Columns    []externalColumnSpec    `yaml:"columns"`
	Options    map[string]stringScalar `yaml:"options"`
}

// externalColumnSpec is one column of an external table.
type externalColumnSpec struct {
	Name    stringScalar `yaml:"name"`
	Type    stringScalar `yaml:"type"`
	NotNull bool         `yaml:"not_null"`
}

// addExternalObjects declares the document's external data sources and
// external tables as feature objects, each checked by the rules the
// annotation parser reads one with.
func (d document) addExternalObjects(db *schemamodel.Database) error {
	for _, key := range sortedKeys(d.ExternalDataSources) {
		spec := d.ExternalDataSources[key]
		options, err := ydbexternal.CheckOptions(scalarMap(spec.Options), ydbexternal.DataSourceReserved...)
		if err != nil {
			return fmt.Errorf("external data source %q: %w", key, err)
		}
		name, err := externalObjectName(spec.Name, key)
		if err != nil {
			return fmt.Errorf("external data source %q: %w", key, err)
		}
		objects, err := ydbexternal.DeclareSource(db.FeatureObjects, string(spec.Schema), name, "", ydbexternal.DataSource{
			SourceType: strings.TrimSpace(string(spec.SourceType)),
			Location:   strings.TrimSpace(string(spec.Location)),
			AuthMethod: strings.TrimSpace(string(spec.AuthMethod)),
			Options:    options,
		})
		if err != nil {
			return fmt.Errorf("external data source %q: %w", key, err)
		}
		db.FeatureObjects = objects
	}
	for _, key := range sortedKeys(d.ExternalTables) {
		if err := d.ExternalTables[key].declare(db, key); err != nil {
			return fmt.Errorf("external table %q: %w", key, err)
		}
	}
	return nil
}

// declare adds one external table to db, named key unless it names itself.
func (spec externalTableSpec) declare(db *schemamodel.Database, key string) error {
	options, err := ydbexternal.CheckOptions(scalarMap(spec.Options), ydbexternal.TableReserved...)
	if err != nil {
		return err
	}
	name, err := externalObjectName(spec.Name, key)
	if err != nil {
		return err
	}
	columns := make([]ydbexternal.Column, 0, len(spec.Columns))
	for _, column := range spec.Columns {
		columns = append(columns, ydbexternal.Column{
			Name: strings.TrimSpace(string(column.Name)), Type: strings.TrimSpace(string(column.Type)),
			NotNull: column.NotNull,
		})
	}
	if err := ydbexternal.CheckColumns(columns); err != nil {
		return err
	}
	objects, err := ydbexternal.DeclareTable(db.FeatureObjects, string(spec.Schema), name, "", ydbexternal.Table{
		DataSource: strings.TrimSpace(string(spec.DataSource)),
		Location:   strings.TrimSpace(string(spec.Location)),
		Columns:    columns,
		Options:    options,
	})
	if err != nil {
		return err
	}
	db.FeatureObjects = objects
	return nil
}

// externalObjectName is the name an external object's entry gives, or its
// key, and one path segment either way.
func externalObjectName(written stringScalar, key string) (string, error) {
	name := valueOrDefault(written, key)
	if strings.Contains(name, "/") {
		return "", &ydbexternal.DeclarationError{Attribute: ydbexternal.AttributeName,
			Reason: fmt.Sprintf("%q holds a slash; name the directory with %s", name, ydbexternal.AttributeSchema)}
	}
	return name, nil
}

// scalarMap is values with plain strings.
func scalarMap(values map[string]stringScalar) map[string]string {
	if values == nil {
		return nil
	}
	plain := make(map[string]string, len(values))
	for name, value := range values {
		plain[name] = string(value)
	}
	return plain
}
