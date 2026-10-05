package ydbcolumn

import (
	"slices"

	"ptah.run/core/ast"
	"ptah.run/internal/ydbttl"
)

// Equal compares column storage, treating an omitted hash key or shard count
// in desired as the server's held layout. TTL remains declarative: omitting a
// policy removes it, just as omitting a row deletion policy does.
func Equal(desired, current *ast.YDBColumnTableSpec) bool {
	if desired == nil || current == nil {
		return desired == current
	}
	if desired.Partitions != 0 && desired.Partitions != current.Partitions {
		return false
	}
	if len(desired.HashColumns) > 0 && !slices.Equal(desired.HashColumns, current.HashColumns) {
		return false
	}
	return TTLEqual(desired.TTL, current.TTL)
}

// TTLEqual compares retention policies by their effective intervals and units.
func TTLEqual(a, b *ast.YDBTieredTTLSpec) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.Column != b.Column || len(a.Tiers) != len(b.Tiers) {
		return false
	}
	au, ae := ydbttl.Unit(a.Unit)
	bu, be := ydbttl.Unit(b.Unit)
	if ae != nil || be != nil || au != bu {
		return false
	}
	for i, tier := range a.Tiers {
		as, ae := ydbttl.IntervalSeconds(tier.Interval)
		bs, be := ydbttl.IntervalSeconds(b.Tiers[i].Interval)
		if ae != nil || be != nil || as != bs || tier.ExternalSource != b.Tiers[i].ExternalSource {
			return false
		}
	}
	return true
}
