package yamlschema

import (
	"fmt"
	"strings"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/ydbexternal"
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

// addExternalObjects reads the document's external data sources and external
// tables, each checked by the rules the annotation parser reads one with.
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
		db.ExternalDataSources = append(db.ExternalDataSources, schemamodel.ExternalDataSource{
			Name:       name,
			Schema:     strings.Trim(strings.TrimSpace(string(spec.Schema)), "/"),
			SourceType: strings.TrimSpace(string(spec.SourceType)),
			Location:   strings.TrimSpace(string(spec.Location)),
			AuthMethod: strings.TrimSpace(string(spec.AuthMethod)),
			Options:    options,
		})
	}
	for _, key := range sortedKeys(d.ExternalTables) {
		table, err := d.ExternalTables[key].toModel(key)
		if err != nil {
			return fmt.Errorf("external table %q: %w", key, err)
		}
		db.ExternalTables = append(db.ExternalTables, table)
	}
	return nil
}

// toModel reads one external table, named key unless it names itself.
func (spec externalTableSpec) toModel(key string) (schemamodel.ExternalTable, error) {
	options, err := ydbexternal.CheckOptions(scalarMap(spec.Options), ydbexternal.TableReserved...)
	if err != nil {
		return schemamodel.ExternalTable{}, err
	}
	name, err := externalObjectName(spec.Name, key)
	if err != nil {
		return schemamodel.ExternalTable{}, err
	}
	columns := make([]ydbexternal.Column, 0, len(spec.Columns))
	for _, column := range spec.Columns {
		columns = append(columns, ydbexternal.Column{
			Name: strings.TrimSpace(string(column.Name)), Type: strings.TrimSpace(string(column.Type)),
			NotNull: column.NotNull,
		})
	}
	if err := ydbexternal.CheckColumns(columns); err != nil {
		return schemamodel.ExternalTable{}, err
	}
	table := schemamodel.ExternalTable{
		Name:       name,
		Schema:     strings.Trim(strings.TrimSpace(string(spec.Schema)), "/"),
		DataSource: strings.TrimSpace(string(spec.DataSource)),
		Location:   strings.TrimSpace(string(spec.Location)),
		Options:    options,
	}
	for _, column := range columns {
		table.Columns = append(table.Columns, schemamodel.ExternalColumn{
			Name: column.Name, Type: column.Type, NotNull: column.NotNull,
		})
	}
	return table, nil
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
