// Package chast defines owned ClickHouse operations for common AST extension
// envelopes. Its versioned payloads carry data without rendering or I/O.
package chast

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chdiff"
	"ptah.run/dialect/clickhouse/internal/chsql"
	"ptah.run/internal/chkey"
)

// AlterStorageKind identifies an in-place change to a table's storage clauses.
const AlterStorageKind schemaext.Kind = "ptah.run/clickhouse/alter-storage"

// AlterStorage changes the sorting key, sampling key, TTL rules and settings
// of a surviving MergeTree table in place. Change retains the complete
// transition, so the operation renders every changed clause or is refused, and
// cannot acknowledge a change it does not make. A change to the engine, PARTITION
// BY or PRIMARY KEY is refused: ClickHouse has no ALTER TABLE for them.
//
// Columns lists the columns the operation adds in the same ALTER TABLE as a
// new sorting key, which ClickHouse requires of a column a new key expression
// uses. It is empty unless the sorting key gains an expression, and a column a
// new key expression uses has no Value. An explicit empty TTL removes the
// rules, and a TTL rule of only whitespace is invalid on either operand.
type AlterStorage struct {
	Change  chdiff.Table `json:"change"`
	Columns []Column     `json:"columns,omitempty"`
}

// Column is one column an [AlterStorage] operation adds. Type is the
// ClickHouse type as written, after any Nullable wrap. Value is DEFAULT,
// MATERIALIZED or ALIAS and Expression its expression; both are empty for a
// column that declares neither. An empty Comment writes no COMMENT.
type Column struct {
	Name       string `json:"name"`
	Type       string `json:"type"`
	Value      string `json:"value,omitempty"`
	Expression string `json:"expression,omitempty"`
	Comment    string `json:"comment,omitempty"`
}

// Kind returns the stable operation identity.
func (*AlterStorage) Kind() schemaext.Kind { return AlterStorageKind }

// CloneExtension returns independent operands; a nil receiver remains typed nil.
func (v *AlterStorage) CloneExtension() ast.ExtensionPayload {
	if v == nil {
		return (*AlterStorage)(nil)
	}
	return &AlterStorage{Change: *v.Change.CloneChange().(*chdiff.Table), Columns: slices.Clone(v.Columns)}
}

// Validate checks the transition and the columns the operation adds. A clause
// ClickHouse cannot change in place, and a transition its rules refuse, wrap
// ptaherr.ErrUnsupportedFeature; malformed operands and columns wrap
// schemaext.ErrInvalidValue. Whether a new sorting-key expression uses a column
// the table already has depends on the table, which the planner checks.
func (v *AlterStorage) Validate() error {
	if v == nil {
		return fmt.Errorf("%w: expected a ClickHouse storage operation", schemaext.ErrInvalidValue)
	}
	if err := chsql.ValidateStorageChange(&v.Change); err != nil {
		return err
	}
	if len(v.Columns) == 0 {
		return nil
	}
	after, err := v.Change.After.Observed()
	if err != nil {
		return err
	}
	key, err := chsql.SortingKey(v.Change.Before.OrderBy, after.OrderBy, after.PrimaryKey)
	if err != nil || len(key.Added) == 0 {
		return fmt.Errorf("%w: a ClickHouse storage operation adds columns only with a new sorting-key expression", schemaext.ErrInvalidValue)
	}
	names := make([]string, 0, len(v.Columns))
	for _, column := range v.Columns {
		if err := column.validate(); err != nil {
			return err
		}
		if slices.Contains(names, column.Name) {
			return fmt.Errorf("%w: ClickHouse storage operation adds column %s twice", schemaext.ErrInvalidValue, column.Name)
		}
		names = append(names, column.Name)
	}
	for _, expression := range key.Added {
		for name := range chkey.ReferencedColumns(expression, names) {
			if column := v.Columns[slices.Index(names, name)]; column.Value != "" {
				return fmt.Errorf("%w: column %s joins the sorting key, and ClickHouse refuses a %s value for a column a new sorting-key expression uses", schemaext.ErrInvalidValue, name, column.Value)
			}
		}
	}
	return nil
}

func (c Column) validate() error {
	if strings.TrimSpace(c.Name) == "" || strings.TrimSpace(c.Type) == "" {
		return fmt.Errorf("%w: a column a ClickHouse storage operation adds requires a name and a type", schemaext.ErrInvalidValue)
	}
	switch c.Value {
	case "":
		if c.Expression != "" {
			return fmt.Errorf("%w: column %s has an expression without DEFAULT, MATERIALIZED or ALIAS", schemaext.ErrInvalidValue, c.Name)
		}
	case "DEFAULT", "MATERIALIZED", "ALIAS":
		if strings.TrimSpace(c.Expression) == "" {
			return fmt.Errorf("%w: column %s names %s without an expression", schemaext.ErrInvalidValue, c.Name, c.Value)
		}
	default:
		return fmt.Errorf("%w: column %s has value kind %q; expected DEFAULT, MATERIALIZED or ALIAS", schemaext.ErrInvalidValue, c.Name, c.Value)
	}
	return nil
}

// Effect reports the strongest consequence of the clauses the operation
// changes, with a reason for each. Dropping an expression from the sorting key
// of an engine that merges rows by it, such as ReplacingMergeTree, is
// destructive: rows that differed only in that expression can merge into one.
// Every other change is behavioral. An invalid operation reports no effect.
func (v *AlterStorage) Effect() schemaext.Effect {
	if v == nil || v.Validate() != nil {
		return schemaext.Effect{}
	}
	after, _ := v.Change.After.Observed()
	effect := schemaext.Effect{Impact: schemaext.Behavioral}
	var reasons []string
	for _, clause := range chsql.Changed(v.Change.Before, after) {
		switch clause {
		case chsql.OrderBy:
			if !chsql.MergesByKey(after.Engine) {
				reasons = append(reasons, "a new sorting key changes how parts written from now on are sorted")
				break
			}
			reasons = append(reasons, "a new sorting key changes how parts written from now on are sorted and which rows the engine merges together")
			if key, _ := chsql.SortingKey(v.Change.Before.OrderBy, after.OrderBy, after.PrimaryKey); key.Dropped > 0 {
				effect.Impact = schemaext.Destructive
				reasons = append(reasons, "rows that differ only in a dropped sorting-key expression can merge into one, which restoring the key cannot undo")
			}
		case chsql.SampleBy:
			reasons = append(reasons, "a new sampling key changes which rows SAMPLE queries read")
		case chsql.TTL:
			reasons = append(reasons, "TTL changes can expire, move, or aggregate existing data; restoring the rule cannot recover that data")
		case chsql.Settings:
			reasons = append(reasons, "table settings change how the server merges, stores and expires data; restoring them does not undo work done under them")
		}
	}
	effect.Reason = strings.Join(reasons, "; ")
	return effect
}
