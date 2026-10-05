package ydbcolumn

import (
	"fmt"
	"slices"
)

// TTLColumnRefusal checks how a column shard finds the maximum TTL value.
// YDB requires either the first primary-key column or a local min-max index.
func TTLColumnRefusal(column string, key, minMaxColumns []string) string {
	if column == "" || (len(key) > 0 && key[0] == column) || slices.Contains(minMaxColumns, column) {
		return ""
	}
	return fmt.Sprintf("TTL column %q must be first in the primary key or have a LOCAL min_max index", column)
}
