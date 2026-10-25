package atlasfilter

import (
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

// projectDatabase applies the schema universe and include selectors to the
// introspected database schema, mirroring projectGenerated so both comparison
// sides see one projection.
func (s *scopeSelection) projectDatabase(db *catalog.Database) (*catalog.Database, error) {
	out := cloneDatabase(db)
	keptTables := make(map[tableIdentity]struct{})
	out.Tables = keep(db.Tables, func(table catalog.Table) bool {
		if !s.schemaAllowed(table.Schema) {
			return false
		}
		if !s.selectedNames(tableResourceTypes(table), s.nameCandidates(table.Schema, table.Name)...) {
			return false
		}
		keptTables[s.tableIdentity(table.Schema, table.Name)] = struct{}{}
		return true
	})
	var err error
	out.FeatureObjects, out.FeatureCoverage, err = s.bindings.Select(db.FeatureObjects, db.FeatureCoverage, func(schema, table string) bool {
		return s.tableKept(keptTables, schema, table)
	}, s.vocabulary.independentKinds()...)
	if err != nil {
		return nil, err
	}

	features, err := s.selectOwnerFeatures(
		out.FeatureObjects, out.FeatureCoverage, databaseColumnTypeSet(out.Tables),
	)
	if err != nil {
		return nil, err
	}
	out.FeatureObjects, out.FeatureCoverage = features.objects, features.coverage
	out.Indexes = keep(db.Indexes, func(index catalog.Index) bool {
		return s.tableKept(keptTables, index.Schema, index.TableName)
	})
	out.Constraints = keep(db.Constraints, func(constraint catalog.Constraint) bool {
		return s.tableKept(keptTables, constraint.Schema, constraint.TableName)
	})
	out.Triggers = keep(db.Triggers, func(trigger catalog.Trigger) bool {
		return s.tableKept(keptTables, trigger.Schema, trigger.Table)
	})
	out.RLSPolicies = keep(db.RLSPolicies, func(policy catalog.RLSPolicy) bool {
		schema, table := splitQualified(policy.Table)
		return s.tableKept(keptTables, schema, table)
	})

	s.projectDatabaseTopLevel(db, out, keptTables, features.roles)
	s.projectDatabaseSupport(db, out)
	extensionSchemas, err := s.projectExtensions(&out.FeatureObjects, &out.FeatureCoverage)
	if err != nil {
		return nil, err
	}
	out.Schemas = s.keepDatabaseSchemas(db, out, slices.Concat(extensionSchemas, features.schemas))
	return out, nil
}

// projectDatabaseTopLevel selects independently includable top-level database
// resources and their riding grants and roles. Extensions are projected after
// support objects, when the selection knows whether a non-extension resource
// matched.
func (s *scopeSelection) projectDatabaseTopLevel(
	db, out *catalog.Database,
	keptTables map[tableIdentity]struct{},
	featureRoles []string,
) {
	out.Views = keep(db.Views, func(view catalog.View) bool {
		return s.selected(typeList("view"), view.Schema, view.Name)
	})
	out.MatViews = keep(db.MatViews, func(view catalog.MaterializedView) bool {
		return s.selected(typeList("materialized_view"), view.Schema, view.Name)
	})
	// A YDB topic, external data source, external table, async replication
	// and transfer are feature objects; selectTopicFeatures,
	// selectExternalFeatures and selectReplicationFeatures select them.
	out.Functions = keep(db.Functions, func(function catalog.Function) bool {
		return s.selected(typeList("function"), function.Schema, function.Name)
	})
	out.Sequences = keep(db.Sequences, func(sequence catalog.Sequence) bool {
		if s.selected(typeList("sequence"), sequence.Schema, sequence.Name) {
			return true
		}
		ownerSchema, ownerTable := sequenceOwnerTable(sequence.Schema, sequence.OwnedBy)
		return s.tableKept(keptTables, ownerSchema, ownerTable)
	})
	columnSequences := s.databaseColumnSequences(db.Tables, keptTables)
	out.Grants = keep(db.Grants, func(grant catalog.Grant) bool {
		return s.databaseGrantSelected(out, keptTables, columnSequences, grant)
	})
	out.Roles = keep(db.Roles, func(role catalog.Role) bool {
		if s.selectedNames(typeList("role"), role.Name) {
			return true
		}
		return slices.Contains(featureRoles, role.Name) || databaseGrantRoleReferenced(out.Grants, role.Name)
	})
}

// projectDatabaseSupport retains the enums kept columns reference through a
// user-defined column type. An owner's selections keep the domains,
// composite types and ranges kept columns name.
func (s *scopeSelection) projectDatabaseSupport(db, out *catalog.Database) {
	referenced := databaseColumnTypeSet(out.Tables)
	out.Enums = keepEnumObjects(s, db.Enums, referenced,
		func(e catalog.Enum) (string, string) { return e.Schema, e.Name })
}

// databaseGrantSelected mirrors generatedGrantSelected for introspected
// grants. columnSequences holds the sequences that are part of a column and
// survive the selection; see [scopeSelection.databaseColumnSequences].
func (s *scopeSelection) databaseGrantSelected(
	out *catalog.Database,
	keptTables, columnSequences map[tableIdentity]struct{},
	grant catalog.Grant,
) bool {
	switch {
	case strings.EqualFold(grant.ObjectType, "DATABASE"):
		return s.selectedNames(typeList("role"), grant.Role)
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
		return databaseSequenceNameKept(out.Sequences, grant.Schema, grant.ObjectName) ||
			s.columnSequenceKept(columnSequences, grant.Schema, grant.ObjectName)
	case routineGrantObjectTypes[strings.ToUpper(grant.ObjectType)]:
		// A routine grant rides its routine, which the projection keeps by
		// name, every overload together.
		return slices.ContainsFunc(out.Functions, func(function catalog.Function) bool {
			return function.Schema == grant.Schema && function.Name == grant.ObjectName
		})
	case s.vocabulary.selections.OwnsGrantType(grant.ObjectType):
		return s.vocabulary.objectGrantKept(out.FeatureObjects, grant.ObjectType, grant.QualifiedTarget())
	default:
		return s.tableKept(keptTables, grant.Schema, grant.ObjectName)
	}
}

// objectGrantKept reports whether the projection keeps the owner's object a
// grant names, such as a YDB topic: the grant rides its object.
func (v vocabulary) objectGrantKept(objects schemaext.Objects, objectType, objectName string) bool {
	ref, ok := v.selections.GrantObject(objectType, objectName)
	if !ok {
		return false
	}
	_, kept, err := objects.Get(ref)
	return err == nil && kept
}

// keepDatabaseSchemas keeps schema entries that own selected objects, so both
// sides agree on which schemas stay in the comparison.
func (s *scopeSelection) keepDatabaseSchemas(db, out *catalog.Database, supportSchemas []string) []catalog.Schema {
	owning := make(map[string]struct{})
	for _, table := range out.Tables {
		owning[s.effectiveSchema(table.Schema)] = struct{}{}
	}
	for _, sequence := range out.Sequences {
		owning[s.effectiveSchema(sequence.Schema)] = struct{}{}
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
	for _, schema := range supportSchemas {
		owning[schema] = struct{}{}
	}
	return keep(db.Schemas, func(schema catalog.Schema) bool {
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

func databaseSequenceNameKept(sequences []catalog.Sequence, schema, name string) bool {
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

func databaseGrantRoleReferenced(grants []catalog.Grant, role string) bool {
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
func databaseColumnTypeSet(tables []catalog.Table) map[string]struct{} {
	types := make(map[string]struct{})
	for _, table := range tables {
		for _, column := range table.Columns {
			if normalized := normalizeTypeReference(column.DataType); normalized != "" {
				types[normalized] = struct{}{}
			}
			if ref, ok := databaseEnumColumnRef(column); ok {
				types[normalizeTypeReference(ref)] = struct{}{}
			}
			// A column declared with a domain reports the domain's base
			// type in data_type and udt_name alike; only domain_name names
			// the domain. Reading the other two alone left the domain out of
			// a selection that kept its column, and a scoped comparison then
			// planned CREATE DOMAIN for a domain the database holds.
			if domain := strings.TrimSpace(column.DomainName); domain != "" {
				types[normalizeTypeReference(domain)] = struct{}{}
				if schema := strings.TrimSpace(column.DomainSchema); schema != "" {
					types[normalizeTypeReference(schema+"."+domain)] = struct{}{}
				}
			}
		}
	}
	return types
}

// databaseEnumColumnRef resolves the user-defined type a column references,
// mirroring how introspected schemas mark enum and domain usage.
func databaseEnumColumnRef(column catalog.Column) (string, bool) {
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
