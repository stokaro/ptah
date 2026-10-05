package goschematogo

import (
	"encoding/json"
	"strconv"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/internal/ydbcolumn"
)

func columnStoreAttrs(spec *ast.YDBColumnTableSpec) []attr {
	if spec == nil {
		return nil
	}
	attrs := []attr{
		{name: ydbcolumn.AttributeStore, value: "column", set: true},
		{name: ydbcolumn.AttributeHash, value: strings.Join(spec.HashColumns, ","), set: len(spec.HashColumns) > 0},
		{name: ydbcolumn.AttributeShards, value: strconv.FormatUint(spec.Partitions, 10), set: spec.Partitions != 0},
	}
	if spec.TTL != nil {
		// The spec contains only strings and a slice, so JSON encoding cannot fail.
		encoded, _ := json.Marshal(spec.TTL)
		attrs = append(attrs, attr{name: ydbcolumn.AttributeTTL, value: string(encoded), set: true})
	}
	return attrs
}
