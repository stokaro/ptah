package goschematogo

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"unicode/utf8"

	"ptah.run/core/schemamodel"
	"ptah.run/core/schemaproperties"
	"ptah.run/internal/annotationmeta"
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

func validateSourceProperties(db *schemamodel.Database) error {
	for _, table := range db.Tables {
		if err := validateProperties("table", table.QualifiedName(), table.Overrides); err != nil {
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
			return fmt.Errorf("%s %q has an unrepresentable Go annotation target %q", owner, name, target)
		}
	}
	for _, property := range propertyAttrs(overrides) {
		if !annotationmeta.IsPlatformAttribute(property.name) || !utf8.ValidString(property.value) || strings.ContainsRune(property.value, 0) {
			return fmt.Errorf("%s %q has an unrepresentable Go annotation property %q", owner, name, property.name)
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
		db, err = schemaproperties.DecodeTables(requestContext, db, opts.Dialect, opts.Runtime)
		if err != nil {
			return nil, err
		}
		db, err = schemaproperties.DecodeIndexes(requestContext, db, opts.Dialect, opts.Runtime)
		if err != nil {
			return nil, err
		}
	}
	if slices.ContainsFunc(db.Tables, func(table schemamodel.Table) bool { return !table.Facets.IsZero() }) {
		var err error
		db, err = schemaproperties.EncodeTables(requestContext, db, opts.Dialect, opts.Runtime)
		if err != nil {
			return nil, err
		}
	}
	if slices.ContainsFunc(db.Indexes, func(index schemamodel.Index) bool { return !index.Facets.IsZero() }) {
		var err error
		db, err = schemaproperties.EncodeIndexes(requestContext, db, opts.Dialect, opts.Runtime)
		if err != nil {
			return nil, err
		}
	}
	return db, nil
}
