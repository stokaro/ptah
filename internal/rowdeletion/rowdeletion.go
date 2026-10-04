// Package rowdeletion is a table's row deletion policy as Ptah declares and
// compares it, across the engines that have one: Spanner's `TTL INTERVAL '30
// days' ON created_at` and YDB's `TTL = Interval("P30D") ON created_at`.
//
// The policy is [ast.RowDeletionPolicySpec]. Each engine writes its interval
// in its own spelling and reads it back rewritten, so what an interval means
// belongs to the package that owns the engine's spelling --
// [ptah.run/internal/spannerttl] and [ptah.run/internal/ydbttl] -- and this
// package holds what the two share: the attributes that declare a policy in a
// Go annotation or a YAML schema, and the comparison that picks the right
// reading of the interval.
package rowdeletion

import (
	"fmt"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/internal/spannerttl"
	"ptah.run/internal/ydbttl"
)

// The attributes that declare a table's row deletion policy. The annotation
// parser, the YAML reader, the annotation registry and the Go exporter all
// read these names, so an attribute one of them accepts is one the others
// read.
const (
	// AttributeColumn names the column the interval is measured from.
	AttributeColumn = "row_deletion_column"
	// AttributeInterval is the interval, in the target's own spelling.
	AttributeInterval = "row_deletion_interval"
	// AttributeUnit is what an integer column counts since the Unix epoch.
	AttributeUnit = "row_deletion_unit"
)

// Attributes lists the attributes in the order a declaration's errors are
// reported.
func Attributes() []string {
	return []string{AttributeColumn, AttributeInterval, AttributeUnit}
}

// ParseDeclaration reads the row deletion policy of one table declaration out
// of values, keyed by attribute name, and ignores every other key. It returns
// nil where none of the attributes is present.
//
// A policy needs its column and its interval, so one without the other is
// refused where it was written. The unit is checked against the four YDB
// takes and written back in capitals; the interval is left as written,
// because what spellings are valid is the target's question, which its
// renderer answers.
func ParseDeclaration(table string, values map[string]string) (*ast.RowDeletionPolicySpec, error) {
	spec := ast.RowDeletionPolicySpec{
		Column:   strings.TrimSpace(values[AttributeColumn]),
		Interval: strings.TrimSpace(values[AttributeInterval]),
	}
	unit, declaredUnit := values[AttributeUnit]
	if spec.Column == "" && spec.Interval == "" && !declaredUnit {
		return nil, nil
	}
	switch {
	case spec.Column == "":
		return nil, fmt.Errorf("table %q declares %s without %s: a row deletion policy needs the column its interval "+
			"is measured from", table, presentAttribute(values), AttributeColumn)
	case spec.Interval == "":
		return nil, fmt.Errorf("table %q declares %s without %s: a row deletion policy needs the interval after which "+
			"a row is deleted", table, presentAttribute(values), AttributeInterval)
	}
	parsed, err := ydbttl.Unit(unit)
	if err != nil {
		return nil, fmt.Errorf("table %q declares %s: %w", table, AttributeUnit, err)
	}
	spec.Unit = parsed
	return &spec, nil
}

// presentAttribute names the first declared attribute, for a refusal of a
// declaration that is missing another.
func presentAttribute(values map[string]string) string {
	for _, attribute := range Attributes() {
		if strings.TrimSpace(values[attribute]) != "" {
			return attribute
		}
	}
	return AttributeUnit
}

// Equal reports whether two policies delete the same rows on the same
// schedule, reading each interval in the spelling it is written in: an ISO
// 8601 duration, or a policy naming a unit, is YDB's and is compared by
// [ydbttl.Equal]; anything else is Spanner's and is compared by
// [spannerttl.Equal]. The two spellings cannot be mistaken for each other: a
// YDB interval begins with P, and a Spanner one is a number and a word.
//
// columnKey is the target's identifier rule for a column name, which every
// column comparison uses.
func Equal(a, b *ast.RowDeletionPolicySpec, columnKey func(string) string) bool {
	if equal, ydb := ydbttl.Equal(a, b, columnKey); ydb {
		return equal
	}
	return spannerttl.Equal(a, b, columnKey)
}
