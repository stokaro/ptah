package ydb

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/ydbcolumn"
	"ptah.run/internal/ydbindex"
	"ptah.run/internal/ydbttl"
)

func (r *Renderer) columnTableSettings(node *ast.CreateTableNode, store *ydbschema.DesiredColumnStore, key []string, types map[string]string) ([]string, error) {
	spec := store.ColumnStore
	subject := fmt.Sprintf("column table %q", node.Name)
	if !r.caps.Has(capability.ColumnStoreTables) {
		return nil, refuseKey(capability.ColumnStoreTables, subject)
	}
	if err := ydbschema.CheckColumnStore(spec); err != nil {
		return nil, refuseFact(subject, err.Error())
	}
	if err := columnTableShape(node, spec, key, types); err != nil {
		return nil, err
	}
	settings := []string{"STORE = COLUMN"}
	if spec.Partitions != 0 {
		settings = append(settings, "AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = "+strconv.FormatUint(spec.Partitions, 10))
	}
	if spec.TTL != nil {
		if err := r.columnTTLSettings(node, spec.TTL, types); err != nil {
			return nil, err
		}
		settings = append(settings, "TTL = "+ydbcolumn.TTLClause(spec.TTL, quote))
	}
	return settings, nil
}

func columnHashClause(spec *ydbschema.DesiredColumnStore) string {
	if spec == nil || len(spec.HashColumns) == 0 {
		return ""
	}
	columns := make([]string, len(spec.HashColumns))
	for i, name := range spec.HashColumns {
		columns[i] = quote(name)
	}
	return " PARTITION BY HASH (" + strings.Join(columns, ", ") + ")"
}

func columnTableShape(node *ast.CreateTableNode, spec ydbschema.ColumnStore, key []string, types map[string]string) error {
	subject := fmt.Sprintf("column table %q", node.Name)
	if slices.Contains(node.Facets.Kinds(), ydbschema.TablePartitioningKind) || slices.Contains(node.Facets.Kinds(), ydbschema.ColumnFamiliesKind) ||
		node.OwnedObjects.Len() > 0 {
		return refuseFact(subject, "row-table partitioning, column families and changefeeds cannot be applied to a column table")
	}
	for _, constraint := range node.Constraints {
		if constraint.Type == ast.UniqueConstraint {
			return refuseFact(subject, "column tables do not support UNIQUE constraints")
		}
	}
	for _, column := range node.Columns {
		if column.Unique {
			return refuseFact(subject, "column tables do not support UNIQUE constraints")
		}
		if column.Default != nil || column.AutoInc || strings.Contains(strings.ToLower(types[column.Name]), "serial") {
			return refuseFact(subject, "column tables do not support defaults or Serial columns")
		}
	}
	for _, name := range spec.HashColumns {
		if !slices.Contains(key, name) {
			return refuseFact(subject, fmt.Sprintf("hash column %q must belong to the primary key", name))
		}
	}
	for _, index := range node.Indexes {
		kind, err := ydbindex.KindOf(index.Type)
		if err != nil {
			return refuseFact(subject, err.Error())
		}
		if !kind.IsLocal() {
			return refuseFact(subject, "column tables require LOCAL indexes; YDB may silently omit a GLOBAL index")
		}
	}
	return columnTTLKey(node, spec.TTL, key, subject)
}

func (r *Renderer) columnTTLSettings(node *ast.CreateTableNode, policy *ydbschema.TieredTTL, types map[string]string) error {
	subject := fmt.Sprintf("column table %q", node.Name)
	rowTTL, err := declaredTTL(node.Facets)
	if err != nil {
		return err
	}
	if rowTTL != nil {
		return refuseFact(subject, "declare either tiered TTL or a TTL that deletes rows, not both")
	}
	if !r.caps.Has(capability.TieredTTL) {
		return refuseKey(capability.TieredTTL, subject)
	}
	columnType, exists := types[policy.Column]
	if !exists {
		return refuseFact(subject, "the TTL column is not declared")
	}
	unit, err := ydbttl.Unit(policy.Unit)
	if err != nil {
		return refuseFact(subject, err.Error())
	}
	if reason := ydbttl.ColumnRefusal(policy.Column, columnType, unit); reason != "" {
		return refuseFact(subject, reason)
	}
	return nil
}

func columnTTLKey(node *ast.CreateTableNode, policy *ydbschema.TieredTTL, key []string, subject string) error {
	var minMaxColumns []string
	for _, index := range node.Indexes {
		if kind, _ := ydbindex.KindOf(index.Type); kind == ydbindex.LocalMinMax {
			minMaxColumns = append(minMaxColumns, index.Columns...)
		}
	}
	ttlColumn := ""
	rowTTL, err := declaredTTL(node.Facets)
	if err != nil {
		return err
	}
	if policy != nil {
		ttlColumn = policy.Column
	} else if rowTTL != nil {
		ttlColumn = rowTTL.Policy.Column
	}
	if reason := ydbcolumn.TTLColumnRefusal(ttlColumn, key, minMaxColumns); reason != "" {
		return refuseFact(subject, reason)
	}

	return nil
}
