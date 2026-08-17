package atlasfilter

import (
	"slices"
	"strings"

	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/objectidentity"
)

// projectDatabase applies the schema universe and include selectors to the
// introspected database schema, mirroring projectGenerated so both comparison
// sides see one projection.
func (s *scopeSelection) projectDatabase(db *dbschematypes.DBSchema) *dbschematypes.DBSchema {
	out := cloneDatabase(db)
	keptTables := make(map[tableIdentity]struct{})
	out.Tables = keep(db.Tables, func(table dbschematypes.DBTable) bool {
		if !s.schemaAllowed(table.Schema) {
			return false
		}
		if !s.selectedNames(tableResourceTypes(table), s.nameCandidates(table.Schema, table.Name)...) {
			return false
		}
		keptTables[s.tableIdentity(table.Schema, table.Name)] = struct{}{}
		return true
	})

	out.Indexes = keep(db.Indexes, func(index dbschematypes.DBIndex) bool {
		return s.tableKept(keptTables, index.Schema, index.TableName)
	})
	out.Constraints = keep(db.Constraints, func(constraint dbschematypes.DBConstraint) bool {
		return s.tableKept(keptTables, constraint.Schema, constraint.TableName)
	})
	out.Triggers = keep(db.Triggers, func(trigger dbschematypes.DBTrigger) bool {
		return s.tableKept(keptTables, trigger.Schema, trigger.Table)
	})
	out.RLSPolicies = keep(db.RLSPolicies, func(policy dbschematypes.DBRLSPolicy) bool {
		schema, table := splitQualified(policy.Table)
		return s.tableKept(keptTables, schema, table)
	})

	s.projectDatabaseTopLevel(db, out, keptTables)
	s.projectDatabaseSupport(db, out)
	s.projectDatabaseExtensions(db, out)
	out.Schemas = s.keepDatabaseSchemas(db, out)
	return out
}

// projectDatabaseTopLevel selects independently includable top-level database
// resources and their riding grants and roles. Extensions are projected after
// support objects, when the selection knows whether a non-extension resource
// matched.
func (s *scopeSelection) projectDatabaseTopLevel(
	db, out *dbschematypes.DBSchema,
	keptTables map[tableIdentity]struct{},
) {
	out.Views = keep(db.Views, func(view dbschematypes.DBView) bool {
		return s.selected(typeList("view"), view.Schema, view.Name)
	})
	out.MatViews = keep(db.MatViews, func(view dbschematypes.DBMatView) bool {
		return s.selected(typeList("materialized_view"), view.Schema, view.Name)
	})
	out.Functions = keep(db.Functions, func(function dbschematypes.DBFunction) bool {
		return s.selected(typeList("function"), function.Schema, function.Name)
	})
	out.Sequences = keep(db.Sequences, func(sequence dbschematypes.DBSequence) bool {
		if s.selected(typeList("sequence"), sequence.Schema, sequence.Name) {
			return true
		}
		ownerSchema, ownerTable := sequenceOwnerTable(sequence.Schema, sequence.OwnedBy)
		return s.tableKept(keptTables, ownerSchema, ownerTable)
	})
	out.Grants = keep(db.Grants, func(grant dbschematypes.DBGrant) bool {
		return s.databaseGrantSelected(out, keptTables, grant)
	})
	out.Roles = keep(db.Roles, func(role dbschematypes.DBRole) bool {
		if s.selectedNames(typeList("role"), role.Name) {
			return true
		}
		return databaseGrantRoleReferenced(out.Grants, role.Name)
	})
}

func (s *scopeSelection) projectDatabaseExtensions(db, out *dbschematypes.DBSchema) {
	if s.extensionSupport || s.nonExtensionMatched {
		for _, extension := range db.Extensions {
			s.selectedExtension(extension.Schema, extension.Name)
		}
		out.Extensions = slices.Clone(db.Extensions)
		return
	}
	out.Extensions = keep(db.Extensions, func(extension dbschematypes.DBExtension) bool {
		return s.selectedExtension(extension.Schema, extension.Name)
	})
}

// projectDatabaseSupport retains type objects referenced by kept columns:
// enums via user-defined column types, and domains, composite types, and
// ranges via column type names.
func (s *scopeSelection) projectDatabaseSupport(db, out *dbschematypes.DBSchema) {
	referenced := databaseColumnTypeSet(out.Tables)
	out.Domains = keepTypeObjects(s, db.Domains, "domain", referenced,
		func(d dbschematypes.DBDomain) (string, string) { return d.Schema, d.Name })
	out.Composites = keepTypeObjects(s, db.Composites, "composite_type", referenced,
		func(c dbschematypes.DBComposite) (string, string) { return c.Schema, c.Name })
	out.Ranges = keepTypeObjects(s, db.Ranges, "range", referenced,
		func(r dbschematypes.DBRange) (string, string) { return r.Schema, r.Name })
	out.Enums = keepEnumObjects(s, db.Enums, referenced,
		func(e dbschematypes.DBEnum) (string, string) { return e.Schema, e.Name })
}

// databaseGrantSelected mirrors generatedGrantSelected for introspected
// grants.
func (s *scopeSelection) databaseGrantSelected(
	out *dbschematypes.DBSchema,
	keptTables map[tableIdentity]struct{},
	grant dbschematypes.DBGrant,
) bool {
	switch {
	case strings.EqualFold(grant.ObjectType, "SCHEMA"):
		if !s.schemaAllowed(grant.ObjectName) {
			return false
		}
		if len(s.selectors) == 0 {
			return true
		}
		_, named := s.allowed[strings.TrimSpace(grant.ObjectName)]
		return named
	case strings.EqualFold(grant.ObjectType, "SEQUENCE"):
		return databaseSequenceNameKept(out.Sequences, grant.Schema, grant.ObjectName)
	default:
		return s.tableKept(keptTables, grant.Schema, grant.ObjectName)
	}
}

// keepDatabaseSchemas keeps schema entries that own selected objects, so both
// sides agree on which schemas stay in the comparison.
func (s *scopeSelection) keepDatabaseSchemas(db, out *dbschematypes.DBSchema) []dbschematypes.DBSchemaInfo {
	owning := make(map[string]struct{})
	for _, table := range out.Tables {
		owning[s.effectiveSchema(table.Schema)] = struct{}{}
	}
	for _, sequence := range out.Sequences {
		owning[s.effectiveSchema(sequence.Schema)] = struct{}{}
	}
	for _, domain := range out.Domains {
		owning[s.effectiveSchema(domain.Schema)] = struct{}{}
	}
	for _, composite := range out.Composites {
		owning[s.effectiveSchema(composite.Schema)] = struct{}{}
	}
	for _, item := range out.Ranges {
		owning[s.effectiveSchema(item.Schema)] = struct{}{}
	}
	for _, view := range out.Views {
		owning[s.effectiveSchema(view.Schema)] = struct{}{}
	}
	for _, view := range out.MatViews {
		owning[s.effectiveSchema(view.Schema)] = struct{}{}
	}
	for _, function := range out.Functions {
		owning[s.effectiveSchema(function.Schema)] = struct{}{}
	}
	for _, enum := range out.Enums {
		owning[s.effectiveSchema(enum.Schema)] = struct{}{}
	}
	for _, extension := range out.Extensions {
		owning[s.effectiveExtensionSchema(extension.Schema)] = struct{}{}
	}
	return keep(db.Schemas, func(schema dbschematypes.DBSchemaInfo) bool {
		if !s.schemaAllowed(schema.Name) {
			return false
		}
		if len(s.selectors) == 0 {
			return true
		}
		_, ok := owning[schema.Name]
		return ok
	})
}

func (s *scopeSelection) tableIdentity(schema, table string) tableIdentity {
	return s.tableID(schema, table).Key()
}

// tableID is [scopeSelection.tableIdentity] for a caller that also has to
// PRINT the table, which is why the two values are separate: a diagnostic
// quotes the spelling the catalog reported, and a lookup uses the value
// equality is decided on. Rendering the comparison value would put this
// package's normalization into a message about the author's schema.
func (s *scopeSelection) tableID(schema, table string) objectidentity.ID {
	return exactIdentities.TableParts(s.effectiveSchema(schema), table)
}

func (s *scopeSelection) tableKept(kept map[tableIdentity]struct{}, schema, table string) bool {
	_, ok := kept[s.tableIdentity(schema, table)]
	return ok
}

// sequenceOwnerTable resolves the owning table of an OWNED BY association.
// The association is "table.column" or "schema.table.column"; unqualified
// owners default to the sequence's own schema.
func sequenceOwnerTable(sequenceSchema, ownedBy string) (schema, table string) {
	parts := strings.Split(strings.TrimSpace(ownedBy), ".")
	switch len(parts) {
	case 2:
		return sequenceSchema, strings.TrimSpace(parts[0])
	case 3:
		return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
	default:
		return "", ""
	}
}

func databaseSequenceNameKept(sequences []dbschematypes.DBSequence, schema, name string) bool {
	name = strings.TrimSpace(name)
	if name == "" {
		return false
	}
	for _, sequence := range sequences {
		if strings.EqualFold(sequence.Name, name) &&
			(schema == "" || strings.EqualFold(sequence.Schema, schema)) {
			return true
		}
	}
	return false
}

func databaseGrantRoleReferenced(grants []dbschematypes.DBGrant, role string) bool {
	for _, grant := range grants {
		if strings.EqualFold(grant.Role, role) {
			return true
		}
	}
	return false
}

// databaseColumnTypeSet collects lowercase column type spellings of the kept
// tables, including user-defined type names, so type objects those columns
// use can be retained as dependencies.
func databaseColumnTypeSet(tables []dbschematypes.DBTable) map[string]struct{} {
	types := make(map[string]struct{})
	for _, table := range tables {
		for _, column := range table.Columns {
			if normalized := normalizeTypeReference(column.DataType); normalized != "" {
				types[normalized] = struct{}{}
			}
			if ref, ok := databaseEnumColumnRef(column); ok {
				types[normalizeTypeReference(ref)] = struct{}{}
			}
		}
	}
	return types
}

// databaseEnumColumnRef resolves the user-defined type a column references,
// mirroring how introspected schemas mark enum and domain usage.
func databaseEnumColumnRef(column dbschematypes.DBColumn) (string, bool) {
	if column.UDTName == "" {
		return "", false
	}
	switch strings.ToUpper(column.DataType) {
	case "USER-DEFINED", "":
		return column.UDTName, true
	case "ARRAY":
		ref := strings.TrimPrefix(column.UDTName, "_")
		return ref, ref != ""
	default:
		return column.UDTName, true
	}
}
