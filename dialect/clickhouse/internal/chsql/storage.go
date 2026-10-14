package chsql

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chdiff"
	"ptah.run/dialect/clickhouse/chresolve"
	"ptah.run/dialect/clickhouse/chschema"
)

// Clause names one storage clause of a ClickHouse table as CREATE TABLE spells
// it.
type Clause string

// The storage clauses, in the order CREATE TABLE writes them.
const (
	Engine      Clause = "ENGINE"
	PartitionBy Clause = "PARTITION BY"
	PrimaryKey  Clause = "PRIMARY KEY"
	OrderBy     Clause = "ORDER BY"
	SampleBy    Clause = "SAMPLE BY"
	TTL         Clause = "TTL"
	Settings    Clause = "SETTINGS"
)

// Changed returns the clauses in which the declared storage differs from the
// current one, in the order CREATE TABLE writes them, compared by the rules
// [SameTable] uses.
func Changed(a, b *chschema.ObservedTable) []Clause {
	var changed []Clause
	for _, clause := range []struct {
		name Clause
		same bool
	}{
		{Engine, SameExpression(a.Engine, b.Engine)},
		{PartitionBy, SameKey(a.PartitionBy, b.PartitionBy)},
		{PrimaryKey, SameKey(a.PrimaryKey, b.PrimaryKey)},
		{OrderBy, SameKey(a.OrderBy, b.OrderBy)},
		{SampleBy, SameExpression(a.SampleBy, b.SampleBy)},
		{TTL, SameExpression(a.TTL, b.TTL)},
		{Settings, sameSettings(a.Settings, b.Settings)},
	} {
		if !clause.same {
			changed = append(changed, clause.name)
		}
	}
	return changed
}

// ValidateStorageChange checks a transition before planning, encoding,
// rendering and reversal, so no stage can acknowledge a change another would
// refuse. ClickHouse has no ALTER TABLE that changes a table's engine, its
// PARTITION BY or its PRIMARY KEY, and those are refused by name with
// ptaherr.ErrUnsupportedFeature, as is any change to a table whose engine is
// not in the MergeTree family. The sorting key, the sampling key and the
// settings must also pass [SortingKey], [SampleKey] and [SettingsChange].
// Whether a new sorting-key expression uses only columns the same ALTER TABLE
// adds depends on the table's columns, which the planner checks. Operands with
// no change, and a TTL rule of only whitespace, wrap schemaext.ErrInvalidValue.
func ValidateStorageChange(change *chdiff.Table) error {
	if err := chdiff.Validate(change); err != nil {
		return err
	}
	after, err := change.After.Observed()
	if err != nil {
		return err
	}
	for _, rule := range []string{change.Before.TTL, after.TTL} {
		if rule != "" && strings.TrimSpace(rule) == "" {
			return fmt.Errorf("%w: a ClickHouse TTL rule cannot contain only whitespace; use an empty string to remove it", schemaext.ErrInvalidValue)
		}
	}
	changed := Changed(change.Before, after)
	if len(changed) == 0 {
		return fmt.Errorf("%w: ClickHouse storage operands contain no change", schemaext.ErrInvalidValue)
	}
	for _, clause := range changed {
		if clause == Engine || clause == PartitionBy || clause == PrimaryKey {
			return fmt.Errorf("%w: ClickHouse has no ALTER TABLE that changes a table's %s; create the table again and copy its rows", ptaherr.ErrUnsupportedFeature, clause)
		}
	}
	if !chresolve.IsMergeTree(after.Engine) {
		return fmt.Errorf("%w: changing a table's %s in place requires a MergeTree engine", ptaherr.ErrUnsupportedFeature, clauseList(changed))
	}
	if slices.Contains(changed, OrderBy) {
		if _, err := SortingKey(change.Before.OrderBy, after.OrderBy, after.PrimaryKey); err != nil {
			return err
		}
	}
	if slices.Contains(changed, SampleBy) {
		if err := SampleKey(after.SampleBy, after.PrimaryKey); err != nil {
			return err
		}
	}
	if slices.Contains(changed, Settings) {
		if _, _, err := SettingsChange(change.Before.Settings, after.Settings); err != nil {
			return err
		}
	}
	return nil
}

func clauseList(clauses []Clause) string {
	names := make([]string, len(clauses))
	for i, clause := range clauses {
		names[i] = string(clause)
	}
	return strings.Join(names, ", ")
}

// SortingKeyChange relates a new sorting key to the old one.
type SortingKeyChange struct {
	// Added holds the expressions of the new key that do not keep an
	// expression of the old one, as the new key writes them.
	Added []string
	// Dropped counts the trailing expressions of the old key the new key no
	// longer holds.
	Dropped int
}

// SortingKey reads a new sorting key against the old one the way MODIFY ORDER
// BY does. Walking the new key, an expression equal to the next one of the old
// key keeps it, and any other expression is added. Measured on 24.10, 25.8 and
// 26.9:
//   - an added expression may use only columns the same ALTER TABLE adds,
//     without a default value; `Existing column c is used in the expression
//     that was added to the sorting key` answers a column added by an earlier
//     ALTER TABLE, and `Newly added column e has a default expression` one
//     with a DEFAULT;
//   - expressions may be added between kept ones and after them, and trailing
//     ones dropped;
//   - the primary key stays a prefix of the sorting key, so an expression may
//     be added only after the primary key's last one, and the primary key's own
//     expressions cannot be dropped.
//
// An added expression that the old key holds is refused here: it keeps an
// existing column, so the key reordered or lost an expression other than its
// last. Every refusal wraps ptaherr.ErrUnsupportedFeature.
func SortingKey(before, after, primaryKey string) (SortingKeyChange, error) {
	old, current := keyItems(before), keyItems(after)
	var change SortingKeyChange
	kept := 0
	for _, item := range current {
		if kept < len(old) && slices.Equal(item.tokens, old[kept].tokens) {
			kept++
			continue
		}
		if slices.ContainsFunc(old, func(previous listItem) bool { return slices.Equal(previous.tokens, item.tokens) }) {
			return SortingKeyChange{}, fmt.Errorf("%w: MODIFY ORDER BY can only drop the last expressions of a sorting key and add expressions over columns the same ALTER TABLE adds, and %s is already in the key %s",
				ptaherr.ErrUnsupportedFeature, item.text, before)
		}
		change.Added = append(change.Added, item.text)
	}
	change.Dropped = len(old) - kept
	primary := keyItems(primaryKey)
	if len(primary) > len(current) || !slices.EqualFunc(primary, current[:len(primary)], func(a, b listItem) bool { return slices.Equal(a.tokens, b.tokens) }) {
		return SortingKeyChange{}, fmt.Errorf("%w: ClickHouse keeps the primary key %s a prefix of the sorting key, and the new ORDER BY %s does not start with it; create the table again and copy its rows",
			ptaherr.ErrUnsupportedFeature, primaryKey, after)
	}
	return change, nil
}

// SampleKey checks a sampling key against the primary key it must belong to.
// Measured on 24.10, 25.8 and 26.9, MODIFY SAMPLE BY answers `Sampling
// expression must be present in the primary key` for an expression the primary
// key does not hold, and allow_sampling_expression_not_in_primary_key, which
// would lift the rule, is not a table setting on any of them. An empty key is
// REMOVE SAMPLE BY, which has no such rule. The refusal wraps
// ptaherr.ErrUnsupportedFeature.
func SampleKey(sample, primaryKey string) error {
	if strings.TrimSpace(sample) == "" {
		return nil
	}
	tokens := keyTokens(sample)
	if slices.ContainsFunc(keyItems(primaryKey), func(item listItem) bool { return slices.Equal(keyTokens(item.text), tokens) }) {
		return nil
	}
	return fmt.Errorf("%w: ClickHouse requires the sampling key %s to be an expression of the primary key %s", ptaherr.ErrUnsupportedFeature, sample, primaryKey)
}

// KeyExpression spells a key for MODIFY ORDER BY: an empty key as tuple(), a
// key of several expressions inside one pair of parentheses, and anything else
// as written.
func KeyExpression(key string) string {
	items := keyItems(key)
	switch {
	case len(items) == 0:
		return "tuple()"
	case len(items) > 1 && !enclosesKey(expressionTokens(key)):
		return "(" + strings.TrimSpace(key) + ")"
	default:
		return strings.TrimSpace(key)
	}
}

// MergesByKey reports whether a MergeTree-family engine combines rows that
// share a sorting key when it merges parts. Every one does except MergeTree
// itself and its replicated and shared forms: Replacing, Summing, Aggregating,
// Collapsing, VersionedCollapsing, Graphite and Coalescing.
func MergesByKey(engine string) bool {
	if !chresolve.IsMergeTree(engine) {
		return false
	}
	name, _, _ := strings.Cut(engine, "(")
	name = strings.ToUpper(strings.TrimSpace(name))
	for _, prefix := range []string{"REPLICATED", "SHARED"} {
		name = strings.TrimPrefix(name, prefix)
	}
	return name != "MERGETREE"
}
