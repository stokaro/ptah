package goschematogo

import (
	"strconv"

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

// mysqlIndexOptionAttrs writes the MySQL owner's index options as the platform
// property the owner decodes, `platform.<target>.parser`, once for each target
// the options are bound to, and for every MySQL-family target when they are
// bound to none. A SQL source binds them to both MySQL and MariaDB, which one
// target's property encoding cannot write.
func mysqlIndexOptionAttrs(facets schemaext.Facets) []attr {
	options, found, err := schemaext.FacetAs[*mysqlschema.DesiredIndex](facets, mysqlschema.IndexKind)
	if err != nil || !found || options.Parser == "" {
		// A value of another type is refused by refuseUnwrittenFacets.
		return nil
	}
	targets := facets.TargetScope(mysqlschema.IndexKind)
	if len(targets) == 0 {
		targets = mysqlschema.Targets()
	}
	attrs := make([]attr, 0, len(targets))
	for _, target := range targets {
		attrs = append(attrs, attr{name: "platform." + target + "." + mysqlsource.ParserProperty, value: options.Parser, set: true})
	}
	return attrs
}

// blockSizeAttr writes the MySQL owner's block-size hint as the index
// directive's own key_block_size attribute, which the owner's annotation
// extension reads back. A facet of another type is
// refused before the export writes anything.
func blockSizeAttr(facets schemaext.Facets) attr {
	size, _, _ := mysqlschema.IndexBlockSize(facets)
	return attr{name: "key_block_size", value: strconv.FormatUint(size, 10), set: size != 0}
}
