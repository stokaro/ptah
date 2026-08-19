package goannotationcleanup

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/annotationmeta"
	"go.5x5.cz/ptah/internal/annotationparse"
	"go.5x5.cz/ptah/internal/tableref"
)

// ValidateParsed proves that every planned removal came from a supported source
// attachment and that directives the parser can collapse or skip produced the
// corresponding schema object. Callers must run this against the same captured
// source view before destructive cleanup.
func (p *Plan) ValidateParsed(sourceDB, exportedDB *goschema.Database) error {
	var validationErrors []error
	for _, removal := range p.removals() {
		if err := annotationAttachmentError(removal); err != nil {
			validationErrors = append(validationErrors, err)
			continue
		}
		if !annotationRepresented(removal, sourceDB, exportedDB) {
			validationErrors = append(validationErrors, fmt.Errorf(
				"%w: %s:%d: //%s did not produce a schema object",
				ErrUnexportedAnnotation,
				removal.annotation.Path,
				removal.annotation.Line,
				removal.annotation.Directive,
			))
		}
	}
	return errors.Join(validationErrors...)
}

func (p *Plan) validateAttachments() error {
	var validationErrors []error
	for _, removal := range p.removals() {
		if err := annotationAttachmentError(removal); err != nil {
			validationErrors = append(validationErrors, err)
		}
	}
	return errors.Join(validationErrors...)
}

func (p *Plan) removals() []removedLine {
	var removals []removedLine
	for _, change := range p.changes {
		removals = append(removals, change.removed...)
	}
	return removals
}

func annotationAttachmentError(removal removedLine) error {
	directive, ok := annotationmeta.Lookup(removal.annotation.Directive)
	if !ok || !annotationmeta.AllowsScope(directive, removal.scope) {
		return annotationScopeError(removal, directive)
	}
	if removal.annotation.Directive == "ptah:schema:field" && !removal.namedField {
		return fmt.Errorf(
			"%w: %s:%d: //%s requires a named field",
			ErrUnexportedAnnotation,
			removal.annotation.Path,
			removal.annotation.Line,
			removal.annotation.Directive,
		)
	}
	return nil
}

func annotationScopeError(removal removedLine, directive annotationmeta.Directive) error {
	allowed := make([]string, len(directive.Scopes))
	for i, scope := range directive.Scopes {
		allowed[i] = string(scope)
	}
	return fmt.Errorf(
		"%w: %s:%d: //%s has %s scope; expected %s scope",
		ErrUnexportedAnnotation,
		removal.annotation.Path,
		removal.annotation.Line,
		removal.annotation.Directive,
		removal.scope,
		strings.Join(allowed, " or "),
	)
}

type representationMatcher func(removedLine, *goschema.Database, *goschema.Database) bool

var representationMatchers = map[string]representationMatcher{
	"ptah:schema:field":      fieldRepresented,
	"ptah:embedded":          embeddedRepresented,
	"ptah:schema:index":      indexRepresented,
	"ptah:schema:table":      tableRepresented,
	"ptah:schema:schema":     schemaRepresented,
	"ptah:schema:constraint": constraintRepresented,
	"ptah:schema:enum":       enumRepresented,
	"ptah:schema:extension":  extensionRepresented,
	"ptah:schema:function":   functionRepresented,
	"ptah:schema:sequence":   sequenceRepresented,
	"ptah:schema:domain":     domainRepresented,
	"ptah:schema:composite":  compositeRepresented,
	"ptah:schema:range":      rangeRepresented,
	"ptah:schema:view":       viewRepresented,
	"ptah:schema:matview":    materializedViewRepresented,
	"ptah:schema:trigger":    triggerRepresented,
	"ptah:schema:rls:policy": rlsPolicyDirectiveRepresented,
	"ptah:schema:rls:enable": rlsEnableRepresented,
	"ptah:schema:role":       roleRepresented,
	"ptah:schema:grant":      grantRepresented,
	"ptah:schema:data":       dataRepresented,
}

func annotationRepresented(removal removedLine, sourceDB, exportedDB *goschema.Database) bool {
	if sourceDB == nil || exportedDB == nil {
		return false
	}
	matcher, ok := representationMatchers[removal.annotation.Directive]
	if !ok {
		return false
	}
	return matcher(removal, sourceDB, exportedDB)
}

func schemaRepresented(removal removedLine, _, exportedDB *goschema.Database) bool {
	return slices.ContainsFunc(exportedDB.Schemas, func(schema goschema.Schema) bool {
		return schema.Name == removal.values["name"]
	})
}

func enumRepresented(removal removedLine, _, exportedDB *goschema.Database) bool {
	return slices.ContainsFunc(exportedDB.Enums, func(enum goschema.Enum) bool {
		return enum.Name == removal.values["name"] &&
			enum.Schema == removal.values["schema"] &&
			slices.Equal(enum.Values, splitAnnotationList(removal.values["values"]))
	})
}

func extensionRepresented(removal removedLine, _, exportedDB *goschema.Database) bool {
	return slices.ContainsFunc(exportedDB.Extensions, func(extension goschema.Extension) bool {
		return extension.Name == removal.values["name"]
	})
}

func functionRepresented(removal removedLine, _, exportedDB *goschema.Database) bool {
	return slices.ContainsFunc(exportedDB.Functions, func(function goschema.Function) bool {
		return function.Name == removal.values["name"] && function.Body == removal.values["body"]
	})
}

func sequenceRepresented(removal removedLine, _, exportedDB *goschema.Database) bool {
	return slices.ContainsFunc(exportedDB.Sequences, func(sequence goschema.Sequence) bool {
		return sameSchemaObject(sequence.Schema, sequence.Name, removal.values["schema"], removal.values["name"])
	})
}

func domainRepresented(removal removedLine, _, exportedDB *goschema.Database) bool {
	return slices.ContainsFunc(exportedDB.Domains, func(domain goschema.Domain) bool {
		return sameSchemaObject(domain.Schema, domain.Name, removal.values["schema"], removal.values["name"]) &&
			domain.BaseType == removal.values["type"]
	})
}

func compositeRepresented(removal removedLine, _, exportedDB *goschema.Database) bool {
	return slices.ContainsFunc(exportedDB.CompositeTypes, func(composite goschema.CompositeType) bool {
		return sameSchemaObject(composite.Schema, composite.Name, removal.values["schema"], removal.values["name"])
	})
}

func rangeRepresented(removal removedLine, _, exportedDB *goschema.Database) bool {
	return slices.ContainsFunc(exportedDB.Ranges, func(rangeType goschema.Range) bool {
		return sameSchemaObject(rangeType.Schema, rangeType.Name, removal.values["schema"], removal.values["name"]) &&
			rangeType.Subtype == removal.values["subtype"]
	})
}

func viewRepresented(removal removedLine, _, exportedDB *goschema.Database) bool {
	return slices.ContainsFunc(exportedDB.Views, func(view goschema.View) bool {
		return view.Name == removal.values["name"] && view.Body == removal.values["body"]
	})
}

func materializedViewRepresented(
	removal removedLine,
	_,
	exportedDB *goschema.Database,
) bool {
	return slices.ContainsFunc(exportedDB.MaterializedViews, func(view goschema.MaterializedView) bool {
		return view.Name == removal.values["name"] && view.Body == removal.values["body"]
	})
}

func triggerRepresented(removal removedLine, _, exportedDB *goschema.Database) bool {
	return slices.ContainsFunc(exportedDB.Triggers, func(trigger goschema.Trigger) bool {
		return trigger.Name == removal.values["name"] &&
			sameTableRef(trigger.Table, removal.values["table"])
	})
}

func rlsPolicyDirectiveRepresented(removal removedLine, _, exportedDB *goschema.Database) bool {
	return rlsPolicyRepresented(removal, exportedDB)
}

func roleRepresented(removal removedLine, _, exportedDB *goschema.Database) bool {
	return slices.ContainsFunc(exportedDB.Roles, func(role goschema.Role) bool {
		return role.Name == removal.values["name"]
	})
}

func grantRepresented(removal removedLine, _, exportedDB *goschema.Database) bool {
	return slices.ContainsFunc(exportedDB.Grants, func(grant goschema.Grant) bool {
		return grant.Role == removal.values["role"] &&
			slices.Equal(grant.Privileges, splitGrantPrivileges(removal.values)) &&
			grant.OnTable == removal.values["on_table"] &&
			grant.OnSchema == removal.values["on_schema"] &&
			grant.OnSequence == removal.values["on_sequence"]
	})
}

func dataRepresented(removal removedLine, _, exportedDB *goschema.Database) bool {
	return slices.ContainsFunc(exportedDB.ManagedData, func(data goschema.ManagedData) bool {
		return sameSchemaObject(data.Schema, data.Table, removal.values["schema"], removal.values["table"]) &&
			slices.Equal(data.Keys, splitAnnotationList(removal.values["key"])) &&
			data.File == removal.values["file"]
	})
}

func tableRepresented(removal removedLine, sourceDB, exportedDB *goschema.Database) bool {
	table, ok := sourceTableForStruct(sourceDB, removal.structName)
	if !ok {
		return false
	}
	_, ok = exportedTableForSource(exportedDB, table)
	return ok
}

func fieldRepresented(removal removedLine, sourceDB, exportedDB *goschema.Database) bool {
	fields := sourceFieldsForRemoval(sourceDB, removal)
	if len(fields) == 0 {
		return false
	}
	sourceTable, ok := sourceTableForStruct(sourceDB, removal.structName)
	if ok {
		exportedTable, ok := exportedTableForSource(exportedDB, sourceTable)
		if ok && fieldsRepresentedInTable(exportedDB, exportedTable.StructName, fields) {
			return true
		}
	}
	return fieldsRepresentedThroughEmbeddedUse(removal, sourceDB, exportedDB, fields)
}

func embeddedRepresented(removal removedLine, sourceDB, exportedDB *goschema.Database) bool {
	sourceTable, ok := sourceTableForStruct(sourceDB, removal.structName)
	if !ok {
		return false
	}
	exportedTable, ok := exportedTableForSource(exportedDB, sourceTable)
	if !ok {
		return false
	}
	if !sourceEmbeddedRepresented(sourceDB, removal) {
		return false
	}
	generatedFields := sourceGeneratedEmbeddedFields(sourceDB, removal.structName)
	if len(generatedFields) == 0 {
		return false
	}
	for _, field := range generatedFields {
		if !exportedFieldExists(exportedDB, exportedTable.StructName, field.Name) {
			return false
		}
	}
	return true
}

func indexRepresented(removal removedLine, sourceDB, exportedDB *goschema.Database) bool {
	indexes := sourceIndexesForRemoval(sourceDB, removal)
	if len(indexes) == 0 {
		return false
	}
	for _, index := range indexes {
		target := sourceIndexTarget(sourceDB, index)
		if target == "" || !exportedIndexExists(exportedDB, target, index.Name) {
			return false
		}
	}
	return true
}

func constraintRepresented(removal removedLine, sourceDB, exportedDB *goschema.Database) bool {
	constraints := sourceConstraintsForRemoval(sourceDB, removal)
	if len(constraints) == 0 {
		return false
	}
	for _, constraint := range constraints {
		target := sourceConstraintTarget(sourceDB, constraint)
		if target == "" || !exportedConstraintExists(exportedDB, target, constraint.Name, constraint.Type) {
			return false
		}
	}
	return true
}

func fieldsRepresentedInTable(database *goschema.Database, structName string, fields []goschema.Field) bool {
	for _, field := range fields {
		if !exportedFieldExists(database, structName, field.Name) {
			return false
		}
	}
	return true
}

func fieldsRepresentedThroughEmbeddedUse(
	removal removedLine,
	sourceDB, exportedDB *goschema.Database,
	fields []goschema.Field,
) bool {
	for _, sourceField := range fields {
		if !fieldRepresentedThroughEmbeddedUse(removal, sourceDB, exportedDB, sourceField) {
			return false
		}
	}
	return true
}

func fieldRepresentedThroughEmbeddedUse(
	removal removedLine,
	sourceDB, exportedDB *goschema.Database,
	sourceField goschema.Field,
) bool {
	for _, generated := range sourceDB.Fields {
		if !generated.GeneratedFromEmbedded || generated.FieldName != sourceField.FieldName {
			continue
		}
		if !inlineEmbeddedPathContains(sourceDB, generated.StructName, removal.structName) {
			continue
		}
		sourceTable, ok := sourceTableForStruct(sourceDB, generated.StructName)
		if !ok {
			continue
		}
		exportedTable, ok := exportedTableForSource(exportedDB, sourceTable)
		if ok && exportedFieldExists(exportedDB, exportedTable.StructName, generated.Name) {
			return true
		}
	}
	return false
}

func inlineEmbeddedPathContains(database *goschema.Database, ownerStruct, embeddedStruct string) bool {
	return inlineEmbeddedPathContainsSeen(database, ownerStruct, embeddedStruct, make(map[string]struct{}))
}

func inlineEmbeddedPathContainsSeen(
	database *goschema.Database,
	ownerStruct, embeddedStruct string,
	seen map[string]struct{},
) bool {
	if _, ok := seen[ownerStruct]; ok {
		return false
	}
	seen[ownerStruct] = struct{}{}
	for _, embedded := range database.EmbeddedFields {
		if embedded.StructName != ownerStruct || strings.EqualFold(embedded.Mode, "skip") {
			continue
		}
		if !embeddedModeMaterializesFields(embedded.Mode) {
			continue
		}
		if embedded.EmbeddedTypeName == embeddedStruct {
			return true
		}
		if inlineEmbeddedPathContainsSeen(database, embedded.EmbeddedTypeName, embeddedStruct, seen) {
			return true
		}
	}
	return false
}

func embeddedModeMaterializesFields(mode string) bool {
	return mode == "" || strings.EqualFold(mode, "inline")
}

func rlsPolicyRepresented(removal removedLine, database *goschema.Database) bool {
	return slices.ContainsFunc(database.RLSPolicies, func(policy goschema.RLSPolicy) bool {
		return policy.Name == removal.values["name"] &&
			sameTableRef(policy.Table, removal.values["table"]) &&
			policy.PolicyFor == canonicalRLSPolicyFor(removal.values["for"]) &&
			policy.ToRoles == removal.values["to"] &&
			policy.UsingExpression == removal.values["using"] &&
			policy.WithCheckExpression == removal.values["with_check"] &&
			policy.Comment == removal.values["comment"]
	})
}

func canonicalRLSPolicyFor(value string) string {
	if value == "" {
		return "ALL"
	}
	return strings.ToUpper(value)
}

func rlsEnableRepresented(removal removedLine, sourceDB, exportedDB *goschema.Database) bool {
	tables := sourceRLSEnabledTablesForRemoval(sourceDB, removal)
	if len(tables) == 0 {
		return false
	}
	for _, table := range tables {
		if !exportedRLSEnabledTableExists(exportedDB, table) {
			return false
		}
	}
	return true
}

func sourceRLSEnabledTablesForRemoval(database *goschema.Database, removal removedLine) []goschema.RLSEnabledTable {
	var tables []goschema.RLSEnabledTable
	for _, table := range database.RLSEnabledTables {
		if !sameTableRef(table.Table, removal.values["table"]) {
			continue
		}
		tables = append(tables, table)
	}
	return tables
}

func exportedRLSEnabledTableExists(database *goschema.Database, source goschema.RLSEnabledTable) bool {
	return slices.ContainsFunc(database.RLSEnabledTables, func(table goschema.RLSEnabledTable) bool {
		return sameTableRef(table.Table, source.Table) && table.Comment == source.Comment
	})
}

func sourceTableForStruct(database *goschema.Database, structName string) (goschema.Table, bool) {
	return findFunc(database.Tables, func(table goschema.Table) bool {
		return table.StructName == structName
	})
}

func exportedTableForSource(database *goschema.Database, source goschema.Table) (goschema.Table, bool) {
	return findFunc(database.Tables, func(table goschema.Table) bool {
		return sameTableRef(table.QualifiedName(), source.QualifiedName())
	})
}

func sourceFieldsForRemoval(database *goschema.Database, removal removedLine) []goschema.Field {
	var fields []goschema.Field
	for _, fieldName := range removal.fieldNames {
		for _, field := range database.Fields {
			if field.StructName == removal.structName && field.FieldName == fieldName {
				fields = append(fields, field)
			}
		}
	}
	return fields
}

func sourceGeneratedEmbeddedFields(database *goschema.Database, structName string) []goschema.Field {
	var fields []goschema.Field
	for _, field := range database.Fields {
		if field.StructName == structName && field.GeneratedFromEmbedded {
			fields = append(fields, field)
		}
	}
	return fields
}

func sourceEmbeddedRepresented(database *goschema.Database, removal removedLine) bool {
	return slices.ContainsFunc(database.EmbeddedFields, func(embedded goschema.EmbeddedField) bool {
		return embedded.StructName == removal.structName &&
			embedded.EmbeddedTypeName == removal.embeddedTypeName &&
			!strings.EqualFold(embedded.Mode, "skip")
	})
}

func exportedFieldExists(database *goschema.Database, structName, name string) bool {
	return slices.ContainsFunc(database.Fields, func(field goschema.Field) bool {
		return field.StructName == structName && field.Name == name
	})
}

func sourceIndexesForRemoval(database *goschema.Database, removal removedLine) []goschema.Index {
	var indexes []goschema.Index
	for _, index := range database.Indexes {
		if index.StructName == removal.structName && index.Name == removal.values["name"] {
			indexes = append(indexes, index)
		}
	}
	return indexes
}

func sourceIndexTarget(database *goschema.Database, index goschema.Index) string {
	if strings.TrimSpace(index.TableName) != "" {
		return canonicalTableRef(index.TableName)
	}
	table, ok := sourceTableForStruct(database, index.StructName)
	if !ok {
		return ""
	}
	return table.QualifiedName()
}

func exportedIndexExists(database *goschema.Database, tableName, name string) bool {
	return slices.ContainsFunc(database.Indexes, func(index goschema.Index) bool {
		return index.Name == name && sameTableRef(index.TableName, tableName)
	})
}

func sourceConstraintsForRemoval(database *goschema.Database, removal removedLine) []goschema.Constraint {
	var constraints []goschema.Constraint
	for _, constraint := range database.Constraints {
		if constraint.StructName == removal.structName &&
			constraint.Name == removal.values["name"] &&
			constraint.Type == strings.ToUpper(removal.values["type"]) {
			constraints = append(constraints, constraint)
		}
	}
	return constraints
}

func sourceConstraintTarget(database *goschema.Database, constraint goschema.Constraint) string {
	if strings.TrimSpace(constraint.Table) != "" {
		return canonicalTableRef(constraint.Table)
	}
	table, ok := sourceTableForStruct(database, constraint.StructName)
	if !ok {
		return ""
	}
	return table.QualifiedName()
}

func exportedConstraintExists(database *goschema.Database, tableName, name, constraintType string) bool {
	return slices.ContainsFunc(database.Constraints, func(constraint goschema.Constraint) bool {
		return constraint.Name == name &&
			constraint.Type == constraintType &&
			sameTableRef(constraint.Table, tableName)
	})
}

func sameSchemaObject(actualSchema, actualName, expectedSchema, expectedName string) bool {
	return actualSchema == expectedSchema && actualName == expectedName
}

func sameTableRef(left, right string) bool {
	return canonicalTableRef(left) == canonicalTableRef(right)
}

func canonicalTableRef(value string) string {
	value = strings.TrimSpace(value)
	ref, ok := tableref.Parse(value)
	if !ok {
		return value
	}
	return goschema.QualifyTableName(ref.Schema, ref.Name)
}

func splitGrantPrivileges(values map[string]string) []string {
	if privileges := splitAnnotationList(values["privilege"]); len(privileges) > 0 {
		return privileges
	}
	return splitAnnotationList(values["privileges"])
}

func findFunc[S ~[]E, E any](values S, predicate func(E) bool) (E, bool) {
	for _, value := range values {
		if predicate(value) {
			return value, true
		}
	}
	var zero E
	return zero, false
}

func splitAnnotationList(value string) []string {
	parts := strings.Split(value, ",")
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		if value := strings.TrimSpace(part); value != "" {
			values = append(values, value)
		}
	}
	return values
}

func annotationAttributes(comment string) ([]string, map[string]string) {
	annotations := annotationparse.Scan(comment)
	if len(annotations) == 0 {
		return nil, nil
	}
	attributes := make([]string, len(annotations[0].Attributes))
	values := make(map[string]string, len(annotations[0].Attributes))
	for i, attribute := range annotations[0].Attributes {
		attributes[i] = attribute.Name
		values[attribute.Name] = attribute.DecodedValue
	}
	return attributes, values
}
