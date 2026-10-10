package goschematogo

import (
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mysql/mysqlschema"
	"ptah.run/dialect/mysql/mysqlsource"
)

// columnSettingsAttrs writes the MySQL owner's column settings as the platform
// properties the owner decodes, `platform.<target>.charset` and
// `platform.<target>.on_update`, once for each target the settings are bound
// to, and for every MySQL-family target when they are bound to none.
func columnSettingsAttrs(facets schemaext.Facets) []attr {
	settings, found, err := mysqlschema.Settings(facets)
	if err != nil || !found {
		// A value of another type is refused by refuseUnwrittenFacets.
		return nil
	}
	targets := facets.TargetScope(mysqlschema.ColumnSettingsKind)
	if len(targets) == 0 {
		targets = mysqlschema.Targets()
	}
	var attrs []attr
	for _, target := range targets {
		prefix := "platform." + target + "."
		attrs = append(attrs,
			attr{name: prefix + mysqlsource.CharsetProperty, value: settings.Charset, set: settings.Charset != ""},
			attr{name: prefix + mysqlsource.OnUpdateProperty, value: settings.OnUpdate, set: settings.OnUpdate != ""},
		)
	}
	return attrs
}

// isAnnotatedColumnFacet reports a column facet the export writes as
// attributes of the field's directive.
func isAnnotatedColumnFacet(kind schemaext.Kind) bool { return kind == mysqlschema.ColumnSettingsKind }
