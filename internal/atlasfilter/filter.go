// Package atlasfilter applies Atlas-style resource filters to introspected schemas.
package atlasfilter

import (
	"fmt"
	"path"
	"slices"
	"strings"

	dbschematypes "github.com/stokaro/ptah/dbschema/types"
)

// ExcludeDatabase returns a shallow copy of schema with resources matching
// Atlas-style exclude globs removed.
func ExcludeDatabase(schema *dbschematypes.DBSchema, patterns []string) (*dbschematypes.DBSchema, error) {
	filters, err := parsePatterns(patterns)
	if err != nil {
		return nil, err
	}
	if schema == nil || len(filters) == 0 {
		return schema, nil
	}

	state := newExclusionState(filters)
	filtered := cloneDatabase(schema)
	filtered.Tables = state.filterTables(filtered.Tables)
	filtered.Enums = state.filterEnums(filtered.Enums)
	filtered.Indexes = state.filterIndexes(filtered.Indexes)
	filtered.Constraints = state.filterConstraints(filtered.Constraints)
	filtered.Extensions = state.filterExtensions(filtered.Extensions)
	filtered.Functions = state.filterFunctions(filtered.Functions)
	filtered.Views = state.filterViews(filtered.Views)
	filtered.MatViews = state.filterMatViews(filtered.MatViews)
	filtered.Triggers = state.filterTriggers(filtered.Triggers)
	filtered.RLSPolicies = state.filterRLSPolicies(filtered.RLSPolicies)
	filtered.Roles = state.filterRoles(filtered.Roles)
	filtered.Grants = state.filterGrants(filtered.Grants)
	return filtered, nil
}

type resourcePattern struct {
	glob  string
	types map[string]struct{}
}

func parsePatterns(values []string) ([]resourcePattern, error) {
	var patterns []resourcePattern
	for _, value := range values {
		for part := range strings.SplitSeq(value, ",") {
			pattern, err := parsePattern(part)
			if err != nil {
				return nil, err
			}
			if pattern.glob != "" {
				patterns = append(patterns, pattern)
			}
		}
	}
	return patterns, nil
}

func parsePattern(value string) (resourcePattern, error) {
	raw := strings.TrimSpace(value)
	if raw == "" {
		return resourcePattern{}, nil
	}
	glob := raw
	types := map[string]struct{}{}
	if open := strings.LastIndex(raw, "[type="); open >= 0 {
		parsedGlob, parsedTypes, err := parseTypeSelector(raw, open)
		if err != nil {
			return resourcePattern{}, err
		}
		glob = parsedGlob
		types = parsedTypes
	} else if selector, ok := selectorLikeSuffix(raw); ok {
		return resourcePattern{}, fmt.Errorf("unsupported Atlas exclude selector %q", selector)
	}
	if _, err := path.Match(glob, "ptah_match_probe"); err != nil {
		return resourcePattern{}, fmt.Errorf("invalid Atlas exclude glob %q: %w", raw, err)
	}
	return resourcePattern{glob: glob, types: types}, nil
}

func parseTypeSelector(raw string, open int) (glob string, types map[string]struct{}, err error) {
	if !strings.HasSuffix(raw, "]") {
		return "", nil, fmt.Errorf("unsupported Atlas exclude field selector %q", raw)
	}
	selector := raw[open+1 : len(raw)-1]
	glob = strings.TrimSpace(raw[:open])
	if glob == "" {
		glob = "*"
	}
	selectorName, selectorValue, ok := strings.Cut(selector, "=")
	if !ok || strings.TrimSpace(selectorName) != "type" {
		return "", nil, fmt.Errorf("unsupported Atlas exclude selector %q", selector)
	}
	types = map[string]struct{}{}
	for item := range strings.SplitSeq(selectorValue, "|") {
		item = strings.ToLower(strings.TrimSpace(item))
		if item != "" {
			types[item] = struct{}{}
		}
	}
	if len(types) == 0 {
		return "", nil, fmt.Errorf("empty Atlas exclude type selector %q", selector)
	}
	return glob, types, nil
}

func (p resourcePattern) matches(resourceType string, names ...string) bool {
	if len(p.types) > 0 {
		if _, ok := p.types[strings.ToLower(resourceType)]; !ok {
			return false
		}
	}
	for _, name := range names {
		if globMatch(p.glob, name) {
			return true
		}
	}
	return false
}

func globMatch(pattern, name string) bool {
	ok, err := path.Match(pattern, name)
	return err == nil && ok
}

func selectorLikeSuffix(raw string) (selector string, ok bool) {
	open := strings.LastIndex(raw, "[")
	if open < 0 || !strings.HasSuffix(raw, "]") {
		return "", false
	}
	selector = raw[open+1 : len(raw)-1]
	return selector, strings.Contains(selector, "=")
}

type exclusionState struct {
	patterns        []resourcePattern
	excludedTables  map[string]struct{}
	excludedColumns map[string]struct{}
}

func newExclusionState(patterns []resourcePattern) *exclusionState {
	return &exclusionState{
		patterns:        patterns,
		excludedTables:  map[string]struct{}{},
		excludedColumns: map[string]struct{}{},
	}
}

func (s *exclusionState) filterTables(tables []dbschematypes.DBTable) []dbschematypes.DBTable {
	result := make([]dbschematypes.DBTable, 0, len(tables))
	for _, table := range tables {
		table = cloneTable(table)
		tableNames := tableNameCandidates(table.Schema, table.Name)
		if s.matchesAny(tableResourceTypes(table), tableNames...) {
			s.excludeTable(table.Schema, table.Name)
			continue
		}
		table.Columns = s.filterColumns(table, table.Columns)
		result = append(result, table)
	}
	return result
}

func (s *exclusionState) filterColumns(table dbschematypes.DBTable, columns []dbschematypes.DBColumn) []dbschematypes.DBColumn {
	result := make([]dbschematypes.DBColumn, 0, len(columns))
	for _, column := range columns {
		columnNames := tableChildNameCandidates(table.Schema, table.Name, column.Name)
		if s.matches("column", columnNames...) {
			s.excludeColumn(table.Schema, table.Name, column.Name)
			continue
		}
		result = append(result, column)
	}
	return result
}

func (s *exclusionState) filterEnums(enums []dbschematypes.DBEnum) []dbschematypes.DBEnum {
	return keep(enums, func(value dbschematypes.DBEnum) bool {
		return !s.matches("enum", value.Name)
	})
}

func (s *exclusionState) filterIndexes(indexes []dbschematypes.DBIndex) []dbschematypes.DBIndex {
	return keep(indexes, func(index dbschematypes.DBIndex) bool {
		if s.tableExcluded(index.Schema, index.TableName) || s.anyColumnExcluded(index.Schema, index.TableName, index.Columns) {
			return false
		}
		return !s.matches("index", tableChildNameCandidates(index.Schema, index.TableName, index.Name)...)
	})
}

func (s *exclusionState) filterConstraints(constraints []dbschematypes.DBConstraint) []dbschematypes.DBConstraint {
	return keep(constraints, func(constraint dbschematypes.DBConstraint) bool {
		foreignSchema := foreignSchemaOrLocal(constraint)
		if s.tableExcluded(constraint.Schema, constraint.TableName) ||
			s.tableExcluded(foreignSchema, derefString(constraint.ForeignTable)) ||
			s.anyColumnExcluded(constraint.Schema, constraint.TableName, constraint.ColumnNamesOrDefault()) ||
			s.anyColumnExcluded(foreignSchema, derefString(constraint.ForeignTable), constraint.ForeignColumnsOrDefault()) {
			return false
		}
		return !s.matchesAny(constraintResourceTypes(constraint), tableChildNameCandidates(constraint.Schema, constraint.TableName, constraint.Name)...)
	})
}

func (s *exclusionState) filterExtensions(extensions []dbschematypes.DBExtension) []dbschematypes.DBExtension {
	return keep(extensions, func(extension dbschematypes.DBExtension) bool {
		return !s.matches("extension", qualifiedNameCandidates(extension.Schema, extension.Name)...)
	})
}

func (s *exclusionState) filterFunctions(functions []dbschematypes.DBFunction) []dbschematypes.DBFunction {
	return keep(functions, func(function dbschematypes.DBFunction) bool {
		return !s.matches("function", function.Name)
	})
}

func (s *exclusionState) filterViews(views []dbschematypes.DBView) []dbschematypes.DBView {
	return keep(views, func(view dbschematypes.DBView) bool {
		excluded := s.matches("view", qualifiedNameCandidates(view.Schema, view.Name)...)
		if excluded {
			s.excludeTable(view.Schema, view.Name)
		}
		return !excluded
	})
}

func (s *exclusionState) filterMatViews(views []dbschematypes.DBMatView) []dbschematypes.DBMatView {
	return keep(views, func(view dbschematypes.DBMatView) bool {
		excluded := s.matches("materialized_view", qualifiedNameCandidates(view.Schema, view.Name)...)
		if excluded {
			s.excludeTable(view.Schema, view.Name)
		}
		return !excluded
	})
}

func (s *exclusionState) filterTriggers(triggers []dbschematypes.DBTrigger) []dbschematypes.DBTrigger {
	return keep(triggers, func(trigger dbschematypes.DBTrigger) bool {
		if s.tableExcluded(trigger.Schema, trigger.Table) {
			return false
		}
		return !s.matches("trigger", tableChildNameCandidates(trigger.Schema, trigger.Table, trigger.Name)...)
	})
}

func (s *exclusionState) filterRLSPolicies(policies []dbschematypes.DBRLSPolicy) []dbschematypes.DBRLSPolicy {
	return keep(policies, func(policy dbschematypes.DBRLSPolicy) bool {
		schema, table := splitQualified(policy.Table)
		if s.tableExcluded(schema, table) {
			return false
		}
		return !s.matches("policy", tableChildNameCandidates(schema, table, policy.Name)...)
	})
}

func (s *exclusionState) filterRoles(roles []dbschematypes.DBRole) []dbschematypes.DBRole {
	return keep(roles, func(role dbschematypes.DBRole) bool {
		return !s.matches("role", role.Name)
	})
}

func (s *exclusionState) filterGrants(grants []dbschematypes.DBGrant) []dbschematypes.DBGrant {
	return keep(grants, func(grant dbschematypes.DBGrant) bool {
		if strings.EqualFold(grant.ObjectType, "TABLE") && s.tableExcluded(grant.Schema, grant.ObjectName) {
			return false
		}
		return !s.matches("grant", grant.QualifiedTarget(), grant.Role+"."+grant.QualifiedTarget())
	})
}

func (s *exclusionState) matches(resourceType string, names ...string) bool {
	return s.matchesAny([]string{resourceType}, names...)
}

func (s *exclusionState) matchesAny(resourceTypes []string, names ...string) bool {
	for _, pattern := range s.patterns {
		for _, resourceType := range resourceTypes {
			if pattern.matches(resourceType, names...) {
				return true
			}
		}
	}
	return false
}

func (s *exclusionState) excludeTable(schema, table string) {
	for _, key := range tableKeys(schema, table) {
		s.excludedTables[key] = struct{}{}
	}
}

func (s *exclusionState) tableExcluded(schema, table string) bool {
	for _, key := range tableKeys(schema, table) {
		if _, ok := s.excludedTables[key]; ok {
			return true
		}
	}
	return false
}

func (s *exclusionState) excludeColumn(schema, table, column string) {
	for _, key := range columnKeys(schema, table, column) {
		s.excludedColumns[key] = struct{}{}
	}
}

func (s *exclusionState) anyColumnExcluded(schema, table string, columns []string) bool {
	for _, column := range columns {
		for _, key := range columnKeys(schema, table, column) {
			if _, ok := s.excludedColumns[key]; ok {
				return true
			}
		}
	}
	return false
}

func cloneDatabase(schema *dbschematypes.DBSchema) *dbschematypes.DBSchema {
	return &dbschematypes.DBSchema{
		Tables:      slices.Clone(schema.Tables),
		Enums:       slices.Clone(schema.Enums),
		Indexes:     slices.Clone(schema.Indexes),
		Constraints: slices.Clone(schema.Constraints),
		Extensions:  slices.Clone(schema.Extensions),
		Functions:   slices.Clone(schema.Functions),
		Views:       slices.Clone(schema.Views),
		MatViews:    slices.Clone(schema.MatViews),
		Triggers:    slices.Clone(schema.Triggers),
		RLSPolicies: slices.Clone(schema.RLSPolicies),
		Roles:       slices.Clone(schema.Roles),
		Grants:      slices.Clone(schema.Grants),
	}
}

func cloneTable(table dbschematypes.DBTable) dbschematypes.DBTable {
	table.Columns = slices.Clone(table.Columns)
	return table
}

func keep[T any](values []T, keepValue func(T) bool) []T {
	result := make([]T, 0, len(values))
	for _, value := range values {
		if keepValue(value) {
			result = append(result, value)
		}
	}
	return result
}

func tableNameCandidates(schema, table string) []string {
	return qualifiedNameCandidates(schema, table)
}

func tableChildNameCandidates(schema, table, child string) []string {
	table = strings.TrimSpace(table)
	child = strings.TrimSpace(child)
	if table == "" || child == "" {
		return nil
	}
	qualifiedTable := dbschematypes.QualifyTableName(schema, table)
	if qualifiedTable == table {
		return []string{table + "." + child}
	}
	return []string{table + "." + child, qualifiedTable + "." + child}
}

func qualifiedNameCandidates(schema, name string) []string {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil
	}
	qualified := dbschematypes.QualifyTableName(schema, name)
	if qualified == name {
		return []string{name}
	}
	return []string{name, qualified}
}

func tableKeys(schema, table string) []string {
	table = strings.TrimSpace(table)
	if table == "" {
		return nil
	}
	schema = strings.TrimSpace(schema)
	if schema == "" {
		return []string{table}
	}
	return []string{schema + "." + table}
}

func columnKeys(schema, table, column string) []string {
	table = strings.TrimSpace(table)
	column = strings.TrimSpace(column)
	if table == "" || column == "" {
		return nil
	}
	schema = strings.TrimSpace(schema)
	if schema == "" {
		return []string{table + "." + column}
	}
	return []string{schema + "." + table + "." + column}
}

func splitQualified(value string) (schema, name string) {
	before, after, ok := strings.Cut(value, ".")
	if !ok {
		return "", value
	}
	return before, after
}

func derefString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func foreignSchemaOrLocal(constraint dbschematypes.DBConstraint) string {
	if strings.TrimSpace(constraint.ForeignSchema) != "" {
		return constraint.ForeignSchema
	}
	return constraint.Schema
}

func tableResourceTypes(table dbschematypes.DBTable) []string {
	types := []string{"table"}
	tableType := strings.ToLower(strings.TrimSpace(table.Type))
	tableType = strings.ReplaceAll(tableType, " ", "_")
	if tableType != "" && tableType != "table" {
		types = append(types, tableType)
	}
	return types
}

func constraintResourceTypes(constraint dbschematypes.DBConstraint) []string {
	types := []string{"constraint"}
	if strings.EqualFold(constraint.Type, "FOREIGN KEY") {
		types = append(types, "foreign_key", "foreign-key")
	}
	return types
}
