package generator

import (
	"fmt"
	"maps"
	"sort"
	"strings"

	"go.5x5.cz/ptah/migration/schemadiff/types"
)

func describeShadowDiff(diff *types.SchemaDiff) string {
	mismatches := collectShadowMismatches(diff)
	if len(mismatches) > 0 {
		return mismatches[0].Message
	}
	return "schema differs"
}

func collectModifiedTableMismatches(table types.TableDiff) []ShadowMismatch {
	var mismatches []ShadowMismatch
	for _, columnName := range sortedStrings(table.ColumnsAdded) {
		message := fmt.Sprintf("missing column %s.%s", table.TableName, columnName)
		mismatches = append(mismatches, ShadowMismatch{Kind: "missing_column", Table: table.TableName, Column: columnName, Object: table.TableName + "." + columnName, Message: message})
	}
	for _, constraintName := range sortedStrings(table.ConstraintsAdded) {
		message := fmt.Sprintf("missing constraint %s.%s", table.TableName, constraintName)
		mismatches = append(mismatches, ShadowMismatch{Kind: "missing_constraint", Table: table.TableName, Constraint: constraintName, Object: table.TableName + "." + constraintName, Message: message})
	}
	for _, column := range sortedColumnDiffs(table.ColumnsModified) {
		message := fmt.Sprintf("column mismatch %s.%s: %s", table.TableName, column.ColumnName, describeChanges(column.Changes))
		mismatches = append(mismatches, ShadowMismatch{Kind: "column_mismatch", Table: table.TableName, Column: column.ColumnName, Object: table.TableName + "." + column.ColumnName, Changes: maps.Clone(column.Changes), Message: message})
	}
	for _, columnName := range sortedStrings(table.ColumnsRemoved) {
		message := fmt.Sprintf("extra column %s.%s", table.TableName, columnName)
		mismatches = append(mismatches, ShadowMismatch{Kind: "extra_column", Table: table.TableName, Column: columnName, Object: table.TableName + "." + columnName, Message: message})
	}
	for _, constraintName := range sortedStrings(table.ConstraintsRemoved) {
		message := fmt.Sprintf("extra constraint %s.%s", table.TableName, constraintName)
		mismatches = append(mismatches, ShadowMismatch{Kind: "extra_constraint", Table: table.TableName, Constraint: constraintName, Object: table.TableName + "." + constraintName, Message: message})
	}
	return mismatches
}

func collectShadowMismatches(diff *types.SchemaDiff) []ShadowMismatch {
	if diff == nil {
		return []ShadowMismatch{{Kind: "schema", Message: "schema differs"}}
	}

	var mismatches []ShadowMismatch
	mismatches = append(mismatches, collectTableMismatches(diff)...)
	mismatches = append(mismatches, collectEnumAndIndexMismatches(diff)...)
	mismatches = append(mismatches, collectSchemaObjectMismatches(diff)...)
	mismatches = append(mismatches, collectAccessControlMismatches(diff)...)
	mismatches = append(mismatches, collectTopLevelConstraintMismatches(diff)...)
	if len(mismatches) == 0 {
		return []ShadowMismatch{{Kind: "schema", Message: "schema differs"}}
	}
	return mismatches
}

func collectTableMismatches(diff *types.SchemaDiff) []ShadowMismatch {
	var mismatches []ShadowMismatch
	mismatches = append(mismatches, tableMismatches(diff.TablesAdded, "missing_table", "missing table")...)
	mismatches = append(mismatches, tableMismatches(diff.TablesRemoved, "extra_table", "extra table")...)
	for _, table := range sortedTableDiffs(diff.TablesModified) {
		mismatches = append(mismatches, collectModifiedTableMismatches(table)...)
	}
	return mismatches
}

func collectSchemaObjectMismatches(diff *types.SchemaDiff) []ShadowMismatch {
	var mismatches []ShadowMismatch
	mismatches = append(mismatches, namedMismatches(diff.ExtensionsAdded, "missing_extension", "missing extension")...)
	mismatches = append(mismatches, namedMismatches(diff.ExtensionsRemoved, "extra_extension", "extra extension")...)
	mismatches = append(mismatches, namedMismatches(diff.FunctionsAdded, "missing_function", "missing function")...)
	mismatches = append(mismatches, namedMismatches(diff.FunctionsRemoved, "extra_function", "extra function")...)
	mismatches = append(mismatches, changedObjectMismatches(
		diff.FunctionsModified,
		"function_mismatch",
		"function",
		func(value types.FunctionDiff) string { return value.FunctionName },
		func(value types.FunctionDiff) map[string]string { return value.Changes },
	)...)
	mismatches = append(mismatches, namedMismatches(diff.SequencesAdded, "missing_sequence", "missing sequence")...)
	mismatches = append(mismatches, namedMismatches(diff.SequencesRemoved, "extra_sequence", "extra sequence")...)
	mismatches = append(mismatches, changedObjectMismatches(
		diff.SequencesModified,
		"sequence_mismatch",
		"sequence",
		func(value types.SequenceDiff) string { return value.SequenceName },
		func(value types.SequenceDiff) map[string]string { return value.Changes },
	)...)
	mismatches = append(mismatches, collectUserTypeMismatches(diff)...)
	mismatches = append(mismatches, collectViewMismatches(diff)...)
	mismatches = append(mismatches, collectTriggerMismatches(diff)...)
	return mismatches
}

func collectUserTypeMismatches(diff *types.SchemaDiff) []ShadowMismatch {
	var mismatches []ShadowMismatch
	mismatches = append(mismatches, namedMismatches(diff.DomainsAdded, "missing_domain", "missing domain")...)
	mismatches = append(mismatches, namedMismatches(diff.DomainsRemoved, "extra_domain", "extra domain")...)
	mismatches = append(mismatches, changedObjectMismatches(
		diff.DomainsModified,
		"domain_mismatch",
		"domain",
		func(value types.DomainDiff) string { return value.DomainName },
		func(value types.DomainDiff) map[string]string { return value.Changes },
	)...)
	mismatches = append(mismatches, namedMismatches(diff.CompositeTypesAdded, "missing_composite_type", "missing composite type")...)
	mismatches = append(mismatches, namedMismatches(diff.CompositeTypesRemoved, "extra_composite_type", "extra composite type")...)
	mismatches = append(mismatches, changedObjectMismatches(
		diff.CompositeTypesModified,
		"composite_type_mismatch",
		"composite type",
		func(value types.CompositeTypeDiff) string { return value.TypeName },
		func(value types.CompositeTypeDiff) map[string]string { return value.Changes },
	)...)
	mismatches = append(mismatches, namedMismatches(diff.RangesAdded, "missing_range", "missing range")...)
	mismatches = append(mismatches, namedMismatches(diff.RangesRemoved, "extra_range", "extra range")...)
	return mismatches
}

func collectViewMismatches(diff *types.SchemaDiff) []ShadowMismatch {
	var mismatches []ShadowMismatch
	mismatches = append(mismatches, namedMismatches(diff.ViewsAdded, "missing_view", "missing view")...)
	mismatches = append(mismatches, namedMismatches(diff.ViewsRemoved, "extra_view", "extra view")...)
	mismatches = append(mismatches, changedObjectMismatches(
		diff.ViewsModified,
		"view_mismatch",
		"view",
		func(value types.ViewDiff) string { return value.ViewName },
		func(value types.ViewDiff) map[string]string { return value.Changes },
	)...)
	mismatches = append(mismatches, namedMismatches(diff.MaterializedViewsAdded, "missing_materialized_view", "missing materialized view")...)
	mismatches = append(mismatches, namedMismatches(diff.MaterializedViewsRemoved, "extra_materialized_view", "extra materialized view")...)
	mismatches = append(mismatches, changedObjectMismatches(
		diff.MaterializedViewsModified,
		"materialized_view_mismatch",
		"materialized view",
		func(value types.MaterializedViewDiff) string { return value.ViewName },
		func(value types.MaterializedViewDiff) map[string]string { return value.Changes },
	)...)
	return mismatches
}

func collectTriggerMismatches(diff *types.SchemaDiff) []ShadowMismatch {
	var mismatches []ShadowMismatch
	for _, ref := range sortedTriggerRefs(diff.TriggersAdded) {
		object := qualifiedObject(ref.TableName, ref.TriggerName)
		mismatches = append(mismatches, ShadowMismatch{Kind: "missing_trigger", Table: ref.TableName, Object: object, Message: "missing trigger " + object})
	}
	for _, ref := range sortedTriggerRefs(diff.TriggersRemoved) {
		object := qualifiedObject(ref.TableName, ref.TriggerName)
		mismatches = append(mismatches, ShadowMismatch{Kind: "extra_trigger", Table: ref.TableName, Object: object, Message: "extra trigger " + object})
	}
	for _, trigger := range sortedTriggerDiffs(diff.TriggersModified) {
		object := qualifiedObject(trigger.TableName, trigger.TriggerName)
		message := fmt.Sprintf("trigger mismatch %s: %s", object, describeChanges(trigger.Changes))
		mismatches = append(mismatches, ShadowMismatch{Kind: "trigger_mismatch", Table: trigger.TableName, Object: object, Changes: maps.Clone(trigger.Changes), Message: message})
	}
	return mismatches
}

func collectAccessControlMismatches(diff *types.SchemaDiff) []ShadowMismatch {
	var mismatches []ShadowMismatch
	for _, ref := range sortedRLSPolicyRefs(diff.RLSPoliciesAdded) {
		object := qualifiedObject(ref.TableName, ref.PolicyName)
		mismatches = append(mismatches, ShadowMismatch{Kind: "missing_rls_policy", Table: ref.TableName, Object: object, Message: "missing RLS policy " + object})
	}
	for _, ref := range sortedRLSPolicyRefs(diff.RLSPoliciesRemoved) {
		object := qualifiedObject(ref.TableName, ref.PolicyName)
		mismatches = append(mismatches, ShadowMismatch{Kind: "extra_rls_policy", Table: ref.TableName, Object: object, Message: "extra RLS policy " + object})
	}
	for _, policy := range sortedRLSPolicyDiffs(diff.RLSPoliciesModified) {
		object := qualifiedObject(policy.TableName, policy.PolicyName)
		message := fmt.Sprintf("RLS policy mismatch %s: %s", object, describeChanges(policy.Changes))
		mismatches = append(mismatches, ShadowMismatch{Kind: "rls_policy_mismatch", Table: policy.TableName, Object: object, Changes: maps.Clone(policy.Changes), Message: message})
	}
	mismatches = append(mismatches, tableMismatches(diff.RLSEnabledTablesAdded, "missing_rls_enablement", "missing RLS enablement")...)
	mismatches = append(mismatches, tableMismatches(diff.RLSEnabledTablesRemoved, "extra_rls_enablement", "extra RLS enablement")...)
	mismatches = append(mismatches, namedMismatches(diff.RolesAdded, "missing_role", "missing role")...)
	mismatches = append(mismatches, namedMismatches(diff.RolesRemoved, "extra_role", "extra role")...)
	mismatches = append(mismatches, changedObjectMismatches(
		diff.RolesModified,
		"role_mismatch",
		"role",
		func(value types.RoleDiff) string { return value.RoleName },
		func(value types.RoleDiff) map[string]string { return value.Changes },
	)...)
	mismatches = append(mismatches, grantMismatches(diff.GrantsAdded, "missing_grant", "missing grant")...)
	mismatches = append(mismatches, grantMismatches(diff.GrantsRemoved, "extra_grant", "extra grant")...)
	mismatches = append(mismatches, grantMismatches(diff.GrantOptionsAdded, "missing_grant_option", "missing grant option")...)
	mismatches = append(mismatches, grantMismatches(diff.GrantOptionsRevoked, "extra_grant_option", "extra grant option")...)
	return mismatches
}

func collectTopLevelConstraintMismatches(diff *types.SchemaDiff) []ShadowMismatch {
	var mismatches []ShadowMismatch
	mismatches = append(mismatches, constraintAdditionMismatches(diff)...)
	mismatches = append(mismatches, constraintRemovalMismatches(diff)...)
	return mismatches
}

func constraintAdditionMismatches(diff *types.SchemaDiff) []ShadowMismatch {
	represented := make(map[string]struct{}, len(diff.ConstraintsAddedWithTables))
	var mismatches []ShadowMismatch
	for _, info := range sortedConstraintAdditions(diff.ConstraintsAddedWithTables) {
		object := qualifiedObject(info.TableName, info.Name)
		mismatches = append(mismatches, ShadowMismatch{Kind: "missing_constraint", Table: info.TableName, Constraint: info.Name, Object: object, Message: "missing constraint " + object})
		represented[info.Name] = struct{}{}
	}
	for _, name := range sortedStrings(diff.ConstraintsAdded) {
		if _, ok := represented[name]; !ok {
			mismatches = append(mismatches, ShadowMismatch{Kind: "missing_constraint", Constraint: name, Object: name, Message: "missing constraint " + name})
		}
	}
	return mismatches
}

func constraintRemovalMismatches(diff *types.SchemaDiff) []ShadowMismatch {
	represented := make(map[string]struct{}, len(diff.ConstraintsRemovedWithTables))
	var mismatches []ShadowMismatch
	for _, info := range sortedConstraintRemovals(diff.ConstraintsRemovedWithTables) {
		object := qualifiedObject(info.TableName, info.Name)
		mismatches = append(mismatches, ShadowMismatch{Kind: "extra_constraint", Table: info.TableName, Constraint: info.Name, Object: object, Message: "extra constraint " + object})
		represented[info.Name] = struct{}{}
	}
	for _, name := range sortedStrings(diff.ConstraintsRemoved) {
		if _, ok := represented[name]; !ok {
			mismatches = append(mismatches, ShadowMismatch{Kind: "extra_constraint", Constraint: name, Object: name, Message: "extra constraint " + name})
		}
	}
	return mismatches
}

func tableMismatches(names []string, kind, label string) []ShadowMismatch {
	var mismatches []ShadowMismatch
	for _, name := range sortedStrings(names) {
		mismatches = append(mismatches, ShadowMismatch{Kind: kind, Table: name, Object: name, Message: label + " " + name})
	}
	return mismatches
}

func namedMismatches(names []string, kind, label string) []ShadowMismatch {
	var mismatches []ShadowMismatch
	for _, name := range sortedStrings(names) {
		mismatches = append(mismatches, ShadowMismatch{Kind: kind, Object: name, Message: label + " " + name})
	}
	return mismatches
}

func indexMismatches(refs []types.IndexRef, kind, label string) []ShadowMismatch {
	var mismatches []ShadowMismatch
	for _, ref := range sortedIndexRefs(refs) {
		object := qualifiedObject(ref.TableName, ref.Name)
		mismatches = append(mismatches, ShadowMismatch{Kind: kind, Table: ref.TableName, Object: object, Message: label + " " + object})
	}
	return mismatches
}

func changedObjectMismatches[T any](
	values []T,
	kind string,
	label string,
	object func(T) string,
	changes func(T) map[string]string,
) []ShadowMismatch {
	sorted := append([]T(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return object(sorted[i]) < object(sorted[j]) })
	mismatches := make([]ShadowMismatch, 0, len(sorted))
	for _, value := range sorted {
		name := object(value)
		objectChanges := changes(value)
		message := fmt.Sprintf("%s mismatch %s: %s", label, name, describeChanges(objectChanges))
		mismatches = append(mismatches, ShadowMismatch{Kind: kind, Object: name, Changes: maps.Clone(objectChanges), Message: message})
	}
	return mismatches
}

func grantMismatches(refs []types.GrantRef, kind, label string) []ShadowMismatch {
	var mismatches []ShadowMismatch
	for _, ref := range sortedGrantRefs(refs) {
		object := fmt.Sprintf("%s %s ON %s %s", ref.Role, ref.Privilege, ref.ObjectType, ref.ObjectName)
		mismatches = append(mismatches, ShadowMismatch{Kind: kind, Object: object, Message: label + " " + object})
	}
	return mismatches
}

func qualifiedObject(namespace, name string) string {
	if namespace == "" {
		return name
	}
	return namespace + "." + name
}

func collectEnumAndIndexMismatches(diff *types.SchemaDiff) []ShadowMismatch {
	var mismatches []ShadowMismatch
	mismatches = append(mismatches, namedMismatches(diff.EnumsAdded, "missing_enum", "missing enum")...)
	mismatches = append(mismatches, namedMismatches(diff.EnumsRemoved, "extra_enum", "extra enum")...)
	for _, enum := range sortedEnumDiffs(diff.EnumsModified) {
		for _, value := range sortedStrings(enum.ValuesAdded) {
			message := fmt.Sprintf("missing enum value %s.%s", enum.EnumName, value)
			mismatches = append(mismatches, ShadowMismatch{Kind: "missing_enum_value", Object: enum.EnumName + "." + value, Message: message})
		}
		for _, value := range sortedStrings(enum.ValuesRemoved) {
			message := fmt.Sprintf("extra enum value %s.%s", enum.EnumName, value)
			mismatches = append(mismatches, ShadowMismatch{Kind: "extra_enum_value", Object: enum.EnumName + "." + value, Message: message})
		}
	}
	mismatches = append(mismatches, indexMismatches(diff.IndexAdditions(), "missing_index", "missing index")...)
	mismatches = append(mismatches, indexMismatches(diff.IndexRemovals(), "extra_index", "extra index")...)
	return mismatches
}

func sortedIndexRefs(refs []types.IndexRef) []types.IndexRef {
	sorted := append([]types.IndexRef(nil), refs...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].TableName != sorted[j].TableName {
			return sorted[i].TableName < sorted[j].TableName
		}
		return sorted[i].Name < sorted[j].Name
	})
	return sorted
}

func sortedTriggerRefs(refs []types.TriggerRef) []types.TriggerRef {
	sorted := append([]types.TriggerRef(nil), refs...)
	sort.Slice(sorted, func(i, j int) bool {
		return compareQualified(
			sorted[i].TableName, sorted[i].TriggerName,
			sorted[j].TableName, sorted[j].TriggerName,
		) < 0
	})
	return sorted
}

func sortedTriggerDiffs(values []types.TriggerDiff) []types.TriggerDiff {
	sorted := append([]types.TriggerDiff(nil), values...)
	sort.Slice(sorted, func(i, j int) bool {
		return compareQualified(
			sorted[i].TableName, sorted[i].TriggerName,
			sorted[j].TableName, sorted[j].TriggerName,
		) < 0
	})
	return sorted
}

func sortedRLSPolicyRefs(refs []types.RLSPolicyRef) []types.RLSPolicyRef {
	sorted := append([]types.RLSPolicyRef(nil), refs...)
	sort.Slice(sorted, func(i, j int) bool {
		return compareQualified(
			sorted[i].TableName, sorted[i].PolicyName,
			sorted[j].TableName, sorted[j].PolicyName,
		) < 0
	})
	return sorted
}

func sortedRLSPolicyDiffs(values []types.RLSPolicyDiff) []types.RLSPolicyDiff {
	sorted := append([]types.RLSPolicyDiff(nil), values...)
	sort.Slice(sorted, func(i, j int) bool {
		return compareQualified(
			sorted[i].TableName, sorted[i].PolicyName,
			sorted[j].TableName, sorted[j].PolicyName,
		) < 0
	})
	return sorted
}

func sortedGrantRefs(refs []types.GrantRef) []types.GrantRef {
	sorted := append([]types.GrantRef(nil), refs...)
	sort.Slice(sorted, func(i, j int) bool {
		return compareGrantRefs(sorted[i], sorted[j]) < 0
	})
	return sorted
}

func compareGrantRefs(left, right types.GrantRef) int {
	for _, values := range [][2]string{
		{left.Role, right.Role},
		{left.Privilege, right.Privilege},
		{left.ObjectType, right.ObjectType},
		{left.ObjectName, right.ObjectName},
	} {
		if compared := strings.Compare(values[0], values[1]); compared != 0 {
			return compared
		}
	}
	return 0
}

func sortedConstraintAdditions(values []types.ConstraintAdditionInfo) []types.ConstraintAdditionInfo {
	sorted := append([]types.ConstraintAdditionInfo(nil), values...)
	sort.Slice(sorted, func(i, j int) bool {
		return compareQualified(sorted[i].TableName, sorted[i].Name, sorted[j].TableName, sorted[j].Name) < 0
	})
	return sorted
}

func sortedConstraintRemovals(values []types.ConstraintRemovalInfo) []types.ConstraintRemovalInfo {
	sorted := append([]types.ConstraintRemovalInfo(nil), values...)
	sort.Slice(sorted, func(i, j int) bool {
		return compareQualified(sorted[i].TableName, sorted[i].Name, sorted[j].TableName, sorted[j].Name) < 0
	})
	return sorted
}

func compareQualified(leftNamespace, leftName, rightNamespace, rightName string) int {
	if compared := strings.Compare(leftNamespace, rightNamespace); compared != 0 {
		return compared
	}
	return strings.Compare(leftName, rightName)
}

func sortedStrings(values []string) []string {
	out := append([]string(nil), values...)
	sort.Strings(out)
	return out
}

func sortedTableDiffs(values []types.TableDiff) []types.TableDiff {
	out := append([]types.TableDiff(nil), values...)
	sort.Slice(out, func(i, j int) bool {
		return out[i].TableName < out[j].TableName
	})
	return out
}

func sortedColumnDiffs(values []types.ColumnDiff) []types.ColumnDiff {
	out := append([]types.ColumnDiff(nil), values...)
	sort.Slice(out, func(i, j int) bool {
		return out[i].ColumnName < out[j].ColumnName
	})
	return out
}

func sortedEnumDiffs(values []types.EnumDiff) []types.EnumDiff {
	out := append([]types.EnumDiff(nil), values...)
	sort.Slice(out, func(i, j int) bool {
		return out[i].EnumName < out[j].EnumName
	})
	return out
}

func describeChanges(changes map[string]string) string {
	if len(changes) == 0 {
		return "unknown change"
	}

	keys := make([]string, 0, len(changes))
	for key := range changes {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+" "+changes[key])
	}
	return strings.Join(parts, ", ")
}
