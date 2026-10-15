package goschematogo

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"unicode/utf8"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemaproperties"
	"ptah.run/internal/annotationmeta"
	"ptah.run/internal/facetsplit"
)

func propertyAttrs(overrides map[string]map[string]string) []attr {
	var result []attr
	for _, target := range slices.Sorted(maps.Keys(overrides)) {
		for _, key := range slices.Sorted(maps.Keys(overrides[target])) {
			result = append(result, attr{name: "platform." + target + "." + key, value: overrides[target][key], set: true})
		}
	}
	return result
}

// fieldProperties are the platform properties a field's directive writes:
// its overrides, less the MySQL-family comment the Go annotation reader
// derives from the field's own comment attribute. Reading the directive back
// derives it again, so writing it would only repeat the comment.
func fieldProperties(field schemamodel.Field) map[string]map[string]string {
	properties := make(map[string]map[string]string, len(field.Overrides))
	for target, values := range field.Overrides {
		for key, value := range values {
			derived := field.Comment != "" && key == "comment" && value == field.Comment &&
				readsPropertyShortcuts(target)
			if derived {
				continue
			}
			if properties[target] == nil {
				properties[target] = make(map[string]string, len(values))
			}
			properties[target][key] = value
		}
	}
	return properties
}

// validateSourceProperties refuses a platform property of a table, a field
// or an index that a Go annotation cannot spell, rather than write it wrong
// or leave it out.
func validateSourceProperties(db *schemamodel.Database) error {
	for _, table := range db.Tables {
		if err := validateProperties("table", table.QualifiedName(), table.Overrides); err != nil {
			return err
		}
	}
	for _, field := range db.Fields {
		if err := validateProperties("field", field.StructName+"."+field.Name, fieldProperties(field)); err != nil {
			return err
		}
	}
	for _, index := range db.Indexes {
		if err := validateProperties("index", index.Name, index.Overrides); err != nil {
			return err
		}
	}
	return nil
}

func validateProperties(owner, name string, overrides map[string]map[string]string) error {
	for _, target := range slices.Sorted(maps.Keys(overrides)) {
		if strings.ContainsRune(target, '.') {
			return fmt.Errorf("%w: %s %q has an unrepresentable Go annotation target %q", ptaherr.ErrUnsupportedFeature, owner, name, target)
		}
	}
	for _, property := range propertyAttrs(overrides) {
		if !annotationmeta.IsPlatformAttribute(property.name) || !utf8.ValidString(property.value) || strings.ContainsRune(property.value, 0) {
			return fmt.Errorf("%w: %s %q has an unrepresentable Go annotation property %q", ptaherr.ErrUnsupportedFeature, owner, name, property.name)
		}
	}
	return nil
}

func prepareSourceProperties(requestContext context.Context, db *schemamodel.Database, opts Options) (*schemamodel.Database, error) {
	if requestContext == nil {
		return nil, fmt.Errorf("Go source rendering requires a context")
	}
	if err := requestContext.Err(); err != nil {
		return nil, err
	}
	if db == nil {
		return nil, fmt.Errorf("database schema is nil")
	}
	if opts.Runtime != nil {
		var err error
		db, err = schemaproperties.Decode(requestContext, db, opts.Dialect, opts.Runtime)
		if err != nil {
			return nil, err
		}
	}
	// A setting a selected owner encodes, such as a hypertable or a YDB
	// index's partitioning, is the owner's to write, as a directive of its
	// own or as attributes of the frontend's, and has no property spelling.
	owned := opts.Annotations.Encodes
	properties, annotated := facetsplit.SetAside(db, owned)
	properties, annotatedIndexes := facetsplit.SetAsideIndexes(properties, owned)
	if slices.ContainsFunc(properties.Tables, func(table schemamodel.Table) bool { return !table.Facets.IsZero() }) {
		var err error
		properties, err = schemaproperties.EncodeTables(requestContext, properties, opts.Dialect, opts.Runtime)
		if err != nil {
			return nil, err
		}
	}
	if slices.ContainsFunc(properties.Indexes, func(index schemamodel.Index) bool { return !index.Facets.IsZero() }) {
		var err error
		properties, err = schemaproperties.EncodeIndexes(requestContext, properties, opts.Dialect, opts.Runtime)
		if err != nil {
			return nil, err
		}
	}
	properties, err := facetsplit.RestoreIndexes(properties, annotatedIndexes)
	if err != nil {
		return nil, err
	}
	return facetsplit.Restore(properties, annotated)
}

// readsPropertyShortcuts reports whether target is the canonical name of a
// dialect whose descriptor reads a field's comment as a platform property
// (descriptor.Conversion.PropertyShortcuts), so the Go annotation reader
// derives it again from the field's comment attribute.
func readsPropertyShortcuts(target string) bool {
	described, found := platform.Lookup(target)
	return found && described.Name == target && described.Conversion.PropertyShortcuts
}
