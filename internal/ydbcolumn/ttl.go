package ydbcolumn

import (
	"strconv"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/internal/ydbttl"
)

// TTLClause renders a validated retention policy with an injected identifier quoter.
func TTLClause(spec *ast.YDBTieredTTLSpec, quote func(string) string) string {
	parts := make([]string, 0, len(spec.Tiers))
	for _, tier := range spec.Tiers {
		seconds, _ := ydbttl.IntervalSeconds(tier.Interval)
		part := `Interval("PT` + strconv.FormatUint(seconds, 10) + `S")`
		if tier.ExternalSource == "" {
			part += " DELETE"
		} else {
			part += " TO EXTERNAL DATA SOURCE " + quote(tier.ExternalSource)
		}
		parts = append(parts, part)
	}
	clause := strings.Join(parts, ", ") + " ON " + quote(spec.Column)
	if unit, _ := ydbttl.Unit(spec.Unit); unit != "" {
		clause += " AS " + unit
	}
	return clause
}
