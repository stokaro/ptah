package shadow

import (
	"fmt"
	"maps"
	"sort"
	"strings"

	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

func describeDiff(diff *difftypes.SchemaDiff) string {
	mismatches := collectMismatches(diff)
	if len(mismatches) > 0 {
		return mismatches[0].Message
	}
	return "schema differs"
}

func newSchemaMismatchError(diff *difftypes.SchemaDiff) *VerificationError {
	return &VerificationError{Result: VerificationResult{
		Stage:      "schema-match",
		Mismatches: collectMismatches(diff),
	}}
}

func collectModifiedTableMismatches(table difftypes.TableDiff) []Mismatch {
	var mismatches []Mismatch
	for _, columnName := range sortedStrings(table.ColumnsAdded.Names()) {
		message := fmt.Sprintf("missing column %s.%s", table.TableName, columnName)
		mismatches = append(mismatches, Mismatch{Kind: "missing_column", Table: table.TableName, Column: columnName, Object: table.TableName + "." + columnName, Message: message})
	}
	for _, constraintName := range sortedStrings(table.ConstraintsAdded) {
		message := fmt.Sprintf("missing constraint %s.%s", table.TableName, constraintName)
		mismatches = append(mismatches, Mismatch{Kind: "missing_constraint", Table: table.TableName, Constraint: constraintName, Object: table.TableName + "." + constraintName, Message: message})
	}
	for _, column := range sortedColumnDiffs(table.ColumnsModified) {
		message := fmt.Sprintf("column mismatch %s.%s: %s", table.TableName, column.ColumnName, describeChanges(column.Changes))
		mismatches = append(mismatches, Mismatch{Kind: "column_mismatch", Table: table.TableName, Column: column.ColumnName, Object: table.TableName + "." + column.ColumnName, Changes: maps.Clone(column.Changes), Message: message})
	}
	for _, columnName := range sortedStrings(table.ColumnsRemoved.Names()) {
		message := fmt.Sprintf("extra column %s.%s", table.TableName, columnName)
		mismatches = append(mismatches, Mismatch{Kind: "extra_column", Table: table.TableName, Column: columnName, Object: table.TableName + "." + columnName, Message: message})
	}
	for _, constraintName := range sortedStrings(table.ConstraintsRemoved) {
		message := fmt.Sprintf("extra constraint %s.%s", table.TableName, constraintName)
		mismatches = append(mismatches, Mismatch{Kind: "extra_constraint", Table: table.TableName, Constraint: constraintName, Object: table.TableName + "." + constraintName, Message: message})
	}
	return mismatches
}

func collectMismatches(diff *difftypes.SchemaDiff) []Mismatch {
	if diff == nil {
		return []Mismatch{{Kind: "schema", Message: "schema differs"}}
	}

	var mismatches []Mismatch
	mismatches = append(mismatches, collectTableMismatches(diff)...)
	mismatches = append(mismatches, collectEnumAndIndexMismatches(diff)...)
	mismatches = append(mismatches, collectSchemaObjectMismatches(diff)...)
	mismatches = append(mismatches, collectAccessControlMismatches(diff)...)
	mismatches = append(mismatches, collectTopLevelConstraintMismatches(diff)...)
	if len(mismatches) == 0 {
		return []Mismatch{{Kind: "schema", Message: "schema differs"}}
	}
	return mismatches
}

func collectTableMismatches(diff *difftypes.SchemaDiff) []Mismatch {
	var mismatches []Mismatch
	mismatches = append(mismatches, tableMismatches(diff.TablesAdded, "missing_table", "missing table")...)
	mismatches = append(mismatches, tableMismatches(diff.TablesRemoved, "extra_table", "extra table")...)
	for _, table := range sortedTableDiffs(diff.TablesModified) {
		mismatches = append(mismatches, collectModifiedTableMismatches(table)...)
	}
	return mismatches
}

func collectSchemaObjectMismatches(diff *difftypes.SchemaDiff) []Mismatch {
	var mismatches []Mismatch
	mismatches = append(mismatches, namedMismatches(diff.ExtensionsAdded.Names(), "missing_extension", "missing extension")...)
	mismatches = append(mismatches, namedMismatches(diff.ExtensionsRemoved.Names(), "extra_extension", "extra extension")...)
	mismatches = append(mismatches, changedObjectMismatches(
		diff.ExtensionsModified,
		"extension_mismatch",
		"extension",
		func(value difftypes.ExtensionDiff) string { return value.Name },
		func(value difftypes.ExtensionDiff) map[string]string {
			return map[string]string{"schema": value.FromSchema + " -> " + value.ToSchema}
		},
	)...)
	mismatches = append(mismatches, namedMismatches(diff.FunctionsAdded.Names(), "missing_function", "missing function")...)
	mismatches = append(mismatches, namedMismatches(diff.FunctionsRemoved.Names(), "extra_function", "extra function")...)
	mismatches = append(mismatches, changedObjectMismatches(
		diff.FunctionsModified,
		"function_mismatch",
		"function",
		func(value difftypes.FunctionDiff) string { return value.FunctionName },
		func(value difftypes.FunctionDiff) map[string]string { return value.Changes },
	)...)
	mismatches = append(mismatches, namedMismatches(diff.SequencesAdded.Names(), "missing_sequence", "missing sequence")...)
	mismatches = append(mismatches, namedMismatches(diff.SequencesRemoved.Names(), "extra_sequence", "extra sequence")...)
	mismatches = append(mismatches, changedObjectMismatches(
		diff.SequencesModified,
		"sequence_mismatch",
		"sequence",
		func(value difftypes.SequenceDiff) string { return value.SequenceName },
		func(value difftypes.SequenceDiff) map[string]string { return value.Changes },
	)...)
	mismatches = append(mismatches, collectUserTypeMismatches(diff)...)
	mismatches = append(mismatches, collectViewMismatches(diff)...)
	mismatches = append(mismatches, collectTriggerMismatches(diff)...)
	return mismatches
}

func collectUserTypeMismatches(diff *difftypes.SchemaDiff) []Mismatch {
	var mismatches []Mismatch
	mismatches = append(mismatches, namedMismatches(diff.DomainsAdded.Names(), "missing_domain", "missing domain")...)
	mismatches = append(mismatches, namedMismatches(diff.DomainsRemoved.Names(), "extra_domain", "extra domain")...)
	mismatches = append(mismatches, changedObjectMismatches(
		diff.DomainsModified,
		"domain_mismatch",
		"domain",
		func(value difftypes.DomainDiff) string { return value.DomainName },
		func(value difftypes.DomainDiff) map[string]string { return value.Changes },
	)...)
	mismatches = append(mismatches, namedMismatches(diff.CompositeTypesAdded.Names(), "missing_composite_type", "missing composite type")...)
	mismatches = append(mismatches, namedMismatches(diff.CompositeTypesRemoved.Names(), "extra_composite_type", "extra composite type")...)
	mismatches = append(mismatches, changedObjectMismatches(
		diff.CompositeTypesModified,
		"composite_type_mismatch",
		"composite type",
		func(value difftypes.CompositeTypeDiff) string { return value.TypeName },
		func(value difftypes.CompositeTypeDiff) map[string]string { return value.Changes },
	)...)
	mismatches = append(mismatches, namedMismatches(diff.RangesAdded.Names(), "missing_range", "missing range")...)
	mismatches = append(mismatches, namedMismatches(diff.RangesRemoved.Names(), "extra_range", "extra range")...)
	mismatches = append(mismatches, changedObjectMismatches(
		diff.RangesModified,
		"range_mismatch",
		"range type",
		func(value difftypes.RangeDiff) string { return value.RangeName },
		func(value difftypes.RangeDiff) map[string]string { return value.Changes },
	)...)
	return mismatches
}

func collectViewMismatches(diff *difftypes.SchemaDiff) []Mismatch {
	var mismatches []Mismatch
	mismatches = append(mismatches, namedMismatches(diff.ViewsAdded.Names(), "missing_view", "missing view")...)
	mismatches = append(mismatches, namedMismatches(diff.ViewsRemoved.Names(), "extra_view", "extra view")...)
	mismatches = append(mismatches, changedObjectMismatches(
		diff.ViewsModified,
		"view_mismatch",
		"view",
		func(value difftypes.ViewDiff) string { return value.ViewName },
		func(value difftypes.ViewDiff) map[string]string { return value.Changes },
	)...)
	mismatches = append(mismatches, namedMismatches(diff.MaterializedViewsAdded.Names(), "missing_materialized_view", "missing materialized view")...)
	mismatches = append(mismatches, namedMismatches(diff.MaterializedViewsRemoved.Names(), "extra_materialized_view", "extra materialized view")...)
	mismatches = append(mismatches, changedObjectMismatches(
		diff.MaterializedViewsModified,
		"materialized_view_mismatch",
		"materialized view",
		func(value difftypes.MaterializedViewDiff) string { return value.ViewName },
		func(value difftypes.MaterializedViewDiff) map[string]string { return value.Changes },
	)...)
	return mismatches
}

func collectTriggerMismatches(diff *difftypes.SchemaDiff) []Mismatch {
	var mismatches []Mismatch
	for _, ref := range sortedTriggerRefs(diff.TriggersAdded) {
		object := qualifiedObject(ref.TableName, ref.TriggerName)
		mismatches = append(mismatches, Mismatch{Kind: "missing_trigger", Table: ref.TableName, Object: object, Message: "missing trigger " + object})
	}
	for _, ref := range sortedTriggerRefs(diff.TriggersRemoved) {
		object := qualifiedObject(ref.TableName, ref.TriggerName)
		mismatches = append(mismatches, Mismatch{Kind: "extra_trigger", Table: ref.TableName, Object: object, Message: "extra trigger " + object})
	}
	for _, trigger := range sortedTriggerDiffs(diff.TriggersModified) {
		object := qualifiedObject(trigger.TableName, trigger.TriggerName)
		message := fmt.Sprintf("trigger mismatch %s: %s", object, describeChanges(trigger.Changes))
		mismatches = append(mismatches, Mismatch{Kind: "trigger_mismatch", Table: trigger.TableName, Object: object, Changes: maps.Clone(trigger.Changes), Message: message})
	}
	return mismatches
}

func collectAccessControlMismatches(diff *difftypes.SchemaDiff) []Mismatch {
	var mismatches []Mismatch
	for _, ref := range sortedRLSPolicyRefs(diff.RLSPoliciesAdded) {
		object := qualifiedObject(ref.TableName, ref.PolicyName)
		mismatches = append(mismatches, Mismatch{Kind: "missing_rls_policy", Table: ref.TableName, Object: object, Message: "missing RLS policy " + object})
	}
	for _, ref := range sortedRLSPolicyRefs(diff.RLSPoliciesRemoved) {
		object := qualifiedObject(ref.TableName, ref.PolicyName)
		mismatches = append(mismatches, Mismatch{Kind: "extra_rls_policy", Table: ref.TableName, Object: object, Message: "extra RLS policy " + object})
	}
	for _, policy := range sortedRLSPolicyDiffs(diff.RLSPoliciesModified) {
		object := qualifiedObject(policy.TableName, policy.PolicyName)
		message := fmt.Sprintf("RLS policy mismatch %s: %s", object, describeChanges(policy.Changes))
		mismatches = append(mismatches, Mismatch{Kind: "rls_policy_mismatch", Table: policy.TableName, Object: object, Changes: maps.Clone(policy.Changes), Message: message})
	}
	mismatches = append(mismatches, tableMismatches(diff.RLSEnabledTablesAdded, "missing_rls_enablement", "missing RLS enablement")...)
	mismatches = append(mismatches, tableMismatches(diff.RLSEnabledTablesRemoved, "extra_rls_enablement", "extra RLS enablement")...)
	mismatches = append(mismatches, namedMismatches(diff.RolesAdded.Names(), "missing_role", "missing role")...)
	mismatches = append(mismatches, namedMismatches(diff.RolesRemoved.Names(), "extra_role", "extra role")...)
	mismatches = append(mismatches, changedObjectMismatches(
		diff.RolesModified,
		"role_mismatch",
		"role",
		func(value difftypes.RoleDiff) string { return value.RoleName },
		func(value difftypes.RoleDiff) map[string]string { return value.Changes },
	)...)
	mismatches = append(mismatches, grantMismatches(diff.GrantsAdded, "missing_grant", "missing grant")...)
	mismatches = append(mismatches, grantMismatches(diff.GrantsRemoved, "extra_grant", "extra grant")...)
	mismatches = append(mismatches, grantMismatches(diff.GrantOptionsAdded, "missing_grant_option", "missing grant option")...)
	mismatches = append(mismatches, grantMismatches(diff.GrantOptionsRevoked, "extra_grant_option", "extra grant option")...)
	return mismatches
}

func collectTopLevelConstraintMismatches(diff *difftypes.SchemaDiff) []Mismatch {
	var mismatches []Mismatch
	mismatches = append(mismatches, constraintAdditionMismatches(diff)...)
	mismatches = append(mismatches, constraintRemovalMismatches(diff)...)
	return mismatches
}

func constraintAdditionMismatches(diff *difftypes.SchemaDiff) []Mismatch {
	represented := make(map[string]struct{}, len(diff.ConstraintsAddedWithTables))
	var mismatches []Mismatch
	for _, info := range sortedConstraintAdditions(diff.ConstraintsAddedWithTables) {
		object := qualifiedObject(info.TableName, info.Name)
		mismatches = append(mismatches, Mismatch{Kind: "missing_constraint", Table: info.TableName, Constraint: info.Name, Object: object, Message: "missing constraint " + object})
		represented[info.Name] = struct{}{}
	}
	for _, name := range sortedStrings(diff.ConstraintsAdded) {
		if _, ok := represented[name]; !ok {
			mismatches = append(mismatches, Mismatch{Kind: "missing_constraint", Constraint: name, Object: name, Message: "missing constraint " + name})
		}
	}
	return mismatches
}

func constraintRemovalMismatches(diff *difftypes.SchemaDiff) []Mismatch {
	represented := make(map[string]struct{}, len(diff.ConstraintsRemovedWithTables))
	var mismatches []Mismatch
	for _, info := range sortedConstraintRemovals(diff.ConstraintsRemovedWithTables) {
		object := qualifiedObject(info.TableName, info.Name)
		mismatches = append(mismatches, Mismatch{Kind: "extra_constraint", Table: info.TableName, Constraint: info.Name, Object: object, Message: "extra constraint " + object})
		represented[info.Name] = struct{}{}
	}
	for _, name := range sortedStrings(diff.ConstraintsRemoved) {
		if _, ok := represented[name]; !ok {
			mismatches = append(mismatches, Mismatch{Kind: "extra_constraint", Constraint: name, Object: name, Message: "extra constraint " + name})
		}
	}
	return mismatches
}

func tableMismatches(names []string, kind, label string) []Mismatch {
	var mismatches []Mismatch
	for _, name := range sortedStrings(names) {
		mismatches = append(mismatches, Mismatch{Kind: kind, Table: name, Object: name, Message: label + " " + name})
	}
	return mismatches
}

func namedMismatches(names []string, kind, label string) []Mismatch {
	var mismatches []Mismatch
	for _, name := range sortedStrings(names) {
		mismatches = append(mismatches, Mismatch{Kind: kind, Object: name, Message: label + " " + name})
	}
	return mismatches
}

func indexMismatches(refs []difftypes.IndexRef, kind, label string) []Mismatch {
	var mismatches []Mismatch
	for _, ref := range sortedIndexRefs(refs) {
		object := qualifiedObject(ref.TableName, ref.Name)
		mismatches = append(mismatches, Mismatch{Kind: kind, Table: ref.TableName, Object: object, Message: label + " " + object})
	}
	return mismatches
}

func changedObjectMismatches[T any](
	values []T,
	kind,
	label string,
	object func(T) string,
	changes func(T) map[string]string,
) []Mismatch {
	sorted := append([]T(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return object(sorted[i]) < object(sorted[j]) })
	mismatches := make([]Mismatch, 0, len(sorted))
	for _, value := range sorted {
		name := object(value)
		objectChanges := changes(value)
		message := fmt.Sprintf("%s mismatch %s: %s", label, name, describeChanges(objectChanges))
		mismatches = append(mismatches, Mismatch{Kind: kind, Object: name, Changes: maps.Clone(objectChanges), Message: message})
	}
	return mismatches
}

func grantMismatches(refs []difftypes.GrantRef, kind, label string) []Mismatch {
	var mismatches []Mismatch
	for _, ref := range sortedGrantRefs(refs) {
		object := fmt.Sprintf("%s %s ON %s %s", ref.Role, ref.Privilege, ref.ObjectType, ref.ObjectName)
		mismatches = append(mismatches, Mismatch{Kind: kind, Object: object, Message: label + " " + object})
	}
	return mismatches
}

func qualifiedObject(namespace, name string) string {
	if namespace == "" {
		return name
	}
	return namespace + "." + name
}

func collectEnumAndIndexMismatches(diff *difftypes.SchemaDiff) []Mismatch {
	var mismatches []Mismatch
	mismatches = append(mismatches, namedMismatches(diff.EnumsAdded.Names(), "missing_enum", "missing enum")...)
	mismatches = append(mismatches, namedMismatches(diff.EnumsRemoved.Names(), "extra_enum", "extra enum")...)
	for _, enum := range sortedEnumDiffs(diff.EnumsModified) {
		for _, value := range sortedStrings(enum.ValuesAdded) {
			message := fmt.Sprintf("missing enum value %s.%s", enum.EnumName, value)
			mismatches = append(mismatches, Mismatch{Kind: "missing_enum_value", Object: enum.EnumName + "." + value, Message: message})
		}
		for _, value := range sortedStrings(enum.ValuesRemoved) {
			message := fmt.Sprintf("extra enum value %s.%s", enum.EnumName, value)
			mismatches = append(mismatches, Mismatch{Kind: "extra_enum_value", Object: enum.EnumName + "." + value, Message: message})
		}
	}
	mismatches = append(mismatches, indexMismatches(diff.IndexAdditions(), "missing_index", "missing index")...)
	mismatches = append(mismatches, indexMismatches(diff.IndexRemovals(), "extra_index", "extra index")...)
	return mismatches
}

func sortedIndexRefs(refs []difftypes.IndexRef) []difftypes.IndexRef {
	sorted := append([]difftypes.IndexRef(nil), refs...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].TableName != sorted[j].TableName {
			return sorted[i].TableName < sorted[j].TableName
		}
		return sorted[i].Name < sorted[j].Name
	})
	return sorted
}

func sortedTriggerRefs(refs []difftypes.TriggerRef) []difftypes.TriggerRef {
	sorted := append([]difftypes.TriggerRef(nil), refs...)
	sort.Slice(sorted, func(i, j int) bool {
		return compareQualified(
			sorted[i].TableName, sorted[i].TriggerName,
			sorted[j].TableName, sorted[j].TriggerName,
		) < 0
	})
	return sorted
}

func sortedTriggerDiffs(values []difftypes.TriggerDiff) []difftypes.TriggerDiff {
	sorted := append([]difftypes.TriggerDiff(nil), values...)
	sort.Slice(sorted, func(i, j int) bool {
		return compareQualified(
			sorted[i].TableName, sorted[i].TriggerName,
			sorted[j].TableName, sorted[j].TriggerName,
		) < 0
	})
	return sorted
}

func sortedRLSPolicyRefs(refs []difftypes.RLSPolicyRef) []difftypes.RLSPolicyRef {
	sorted := append([]difftypes.RLSPolicyRef(nil), refs...)
	sort.Slice(sorted, func(i, j int) bool {
		return compareQualified(
			sorted[i].TableName, sorted[i].PolicyName,
			sorted[j].TableName, sorted[j].PolicyName,
		) < 0
	})
	return sorted
}

func sortedRLSPolicyDiffs(values []difftypes.RLSPolicyDiff) []difftypes.RLSPolicyDiff {
	sorted := append([]difftypes.RLSPolicyDiff(nil), values...)
	sort.Slice(sorted, func(i, j int) bool {
		return compareQualified(
			sorted[i].TableName, sorted[i].PolicyName,
			sorted[j].TableName, sorted[j].PolicyName,
		) < 0
	})
	return sorted
}

func sortedGrantRefs(refs []difftypes.GrantRef) []difftypes.GrantRef {
	sorted := append([]difftypes.GrantRef(nil), refs...)
	sort.Slice(sorted, func(i, j int) bool {
		return compareGrantRefs(sorted[i], sorted[j]) < 0
	})
	return sorted
}

func compareGrantRefs(left, right difftypes.GrantRef) int {
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

func sortedConstraintAdditions(values []difftypes.ConstraintAdditionInfo) []difftypes.ConstraintAdditionInfo {
	sorted := append([]difftypes.ConstraintAdditionInfo(nil), values...)
	sort.Slice(sorted, func(i, j int) bool {
		return compareQualified(sorted[i].TableName, sorted[i].Name, sorted[j].TableName, sorted[j].Name) < 0
	})
	return sorted
}

func sortedConstraintRemovals(values []difftypes.ConstraintRemovalInfo) []difftypes.ConstraintRemovalInfo {
	sorted := append([]difftypes.ConstraintRemovalInfo(nil), values...)
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

func sortedTableDiffs(values []difftypes.TableDiff) []difftypes.TableDiff {
	out := append([]difftypes.TableDiff(nil), values...)
	sort.Slice(out, func(i, j int) bool {
		return out[i].TableName < out[j].TableName
	})
	return out
}

func sortedColumnDiffs(values []difftypes.ColumnDiff) []difftypes.ColumnDiff {
	out := append([]difftypes.ColumnDiff(nil), values...)
	sort.Slice(out, func(i, j int) bool {
		return out[i].ColumnName < out[j].ColumnName
	})
	return out
}

func sortedEnumDiffs(values []difftypes.EnumDiff) []difftypes.EnumDiff {
	out := append([]difftypes.EnumDiff(nil), values...)
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
