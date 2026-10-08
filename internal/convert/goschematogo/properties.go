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

func tablePropertyAttrs(overrides map[string]map[string]string) []attr {
	var result []attr
	for _, target := range slices.Sorted(maps.Keys(overrides)) {
		for _, key := range slices.Sorted(maps.Keys(overrides[target])) {
			result = append(result, attr{name: "platform." + target + "." + key, value: overrides[target][key], set: true})
		}
	}
	return result
}

func validateTableProperties(tables []schemamodel.Table) error {
	for _, table := range tables {
		for _, target := range slices.Sorted(maps.Keys(table.Overrides)) {
			if strings.ContainsRune(target, '.') {
				return fmt.Errorf("table %q has an unrepresentable Go annotation target %q", table.QualifiedName(), target)
			}
		}
		for _, property := range tablePropertyAttrs(table.Overrides) {
			if !annotationmeta.IsPlatformAttribute(property.name) || !utf8.ValidString(property.value) || strings.ContainsRune(property.value, 0) {
				return fmt.Errorf("table %q has an unrepresentable Go annotation property %q", table.QualifiedName(), property.name)
			}
		}
	}
	return nil
}

func prepareSourceTables(requestContext context.Context, db *schemamodel.Database, opts Options) (*schemamodel.Database, error) {
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
	}
	if slices.ContainsFunc(db.Tables, func(table schemamodel.Table) bool { return !table.Facets.IsZero() }) {
		var err error
		db, err = schemaproperties.EncodeTables(requestContext, db, opts.Dialect, opts.Runtime)
		if err != nil {
			return nil, err
		}
	}
	return db, nil
}
