// Package rowdeletion is what the two row deletion policy owners share: the
// property names a declaration uses, the reading of those properties, and the
// comparison of a table's policy against the one the database holds.
//
// Spanner's ROW DELETION POLICY (`TTL INTERVAL '30 days' ON created_at`) and
// YDB's TTL (`TTL = Interval("P30D") ON created_at`) are one idea with two
// spellings, and each is owned by its target: the Spanner owner reads and
// writes Spanner's interval, the YDB owner YDB's, and each binds its facet to
// its own target. This package knows neither spelling and imports no owner.
// Nothing here translates a policy written for one target into the other's.
package rowdeletion

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"ptah.run/core/schemaext"
)

// The property names of a row deletion policy. Each owner reads them from its
// own platform group -- platform.spanner.row_deletion_interval and
// platform.ydb.row_deletion_interval are two declarations -- and the value of
// the interval is written in that target's spelling.
const (
	// ColumnProperty names the column the interval is measured from.
	ColumnProperty = "row_deletion_column"
	// IntervalProperty is the interval after which a row is deleted.
	IntervalProperty = "row_deletion_interval"
	// UnitProperty is what an integer column counts since the Unix epoch,
	// for a target whose policy can read one.
	UnitProperty = "row_deletion_unit"
	// PropertyPrefix is the prefix an owner claims, so that a misspelled or
	// miscased property is refused by name rather than left unread.
	PropertyPrefix = "row_deletion"
)

// Declaration is a policy as a source declares it, before its owner reads the
// interval: the column, the interval as written, and the unit, empty for a
// date or time column.
type Declaration struct {
	Column   string
	Interval string
	Unit     string
}

// Decode reads one table's declaration from its properties. properties holds
// only the keys the owner claimed; managed lists the names the owner takes, in
// the order a refusal lists them. A name outside managed is refused, naming
// the lower-case spelling when that is managed. A policy needs both its column
// and its interval, and a blank value is refused. Each refusal wraps
// schemaext.ErrInvalidValue. The values are trimmed; the owner reads the
// interval and the unit.
func Decode(properties map[string]string, managed []string) (Declaration, error) {
	for _, name := range slices.Sorted(maps.Keys(properties)) {
		if slices.Contains(managed, name) {
			continue
		}
		if lower := strings.ToLower(name); lower != name && slices.Contains(managed, lower) {
			return Declaration{}, fmt.Errorf("%w: unknown row deletion property %q: property names are lower case, as %q", schemaext.ErrInvalidValue, name, lower)
		}
		return Declaration{}, fmt.Errorf("%w: unknown row deletion property %q: the policy takes %s", schemaext.ErrInvalidValue, name, strings.Join(managed, ", "))
	}
	for _, name := range slices.Sorted(maps.Keys(properties)) {
		if strings.TrimSpace(properties[name]) == "" {
			return Declaration{}, fmt.Errorf("%w: %s is empty; remove it to leave the policy undeclared", schemaext.ErrInvalidValue, name)
		}
	}
	declaration := Declaration{
		Column:   strings.TrimSpace(properties[ColumnProperty]),
		Interval: strings.TrimSpace(properties[IntervalProperty]),
		Unit:     strings.TrimSpace(properties[UnitProperty]),
	}
	switch {
	case declaration.Column == "":
		return Declaration{}, fmt.Errorf("%w: a row deletion policy needs %s, the column its interval is measured from", schemaext.ErrInvalidValue, ColumnProperty)
	case declaration.Interval == "":
		return Declaration{}, fmt.Errorf("%w: a row deletion policy needs %s, the interval after which a row is deleted", schemaext.ErrInvalidValue, IntervalProperty)
	}
	return declaration, nil
}

// Encode writes a declaration as its properties: the column and the interval,
// and the unit when one is set.
func Encode(declaration Declaration) map[string]string {
	properties := map[string]string{ColumnProperty: declaration.Column, IntervalProperty: declaration.Interval}
	if declaration.Unit != "" {
		properties[UnitProperty] = declaration.Unit
	}
	return properties
}
