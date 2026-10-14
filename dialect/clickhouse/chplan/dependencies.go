package chplan

import (
	"fmt"
	"maps"
	"slices"

	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chast"
	"ptah.run/dialect/clickhouse/chdiff"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/dialect/clickhouse/internal/chkey"
	"ptah.run/dialect/clickhouse/internal/chsql"
	"ptah.run/internal/chcolumn"
)

func assessParent(request featureplan.Request, table featureplan.Table) (string, error) {
	before, after, err := capturedStorage(table)
	if err != nil {
		return "", err
	}
	if table.Action == featureplan.DropTable {
		return "remove storage settings with the table; all table data is lost", nil
	}
	if table.Action != featureplan.AlterTable {
		return "", fmt.Errorf("ClickHouse storage has no plan for parent action %q", table.Action)
	}
	if !chsql.SameTable(before, after) && !slices.ContainsFunc(request.Changes, func(change schemaext.ChangeRecord) bool {
		return change.Subject.Key() == table.Subject.Key() && change.Value.Kind() == chdiff.TableKind
	}) {
		return "", fmt.Errorf("captured storage changed without a corresponding feature change")
	}
	columns := capturedColumns(request, table)
	// Planned clauses order their own column changes; what the table keeps
	// after them is what no column change may touch.
	retained := referencedColumns(columns, after.Engine, after.PrimaryKey, after.PartitionBy, after.OrderBy, after.SampleBy)
	ttl := chkey.ReferencedColumns(after.TTL, columns)
	for _, common := range request.CommonSteps {
		if common.Parent.Key() != table.Subject.Key() {
			continue
		}
		if len(common.Effects) == 0 {
			return "", fmt.Errorf("common operation %v has unknown effects on a ClickHouse table", common.ID)
		}
		for _, effect := range common.Effects {
			if !columnOf(effect.Subject, table.Subject) || effect.Action == plangraph.Read || effect.Action == plangraph.Create {
				continue
			}
			name := effect.Subject.Name.Source
			if usesColumn(request, table, retained, effect.Subject) {
				return "", fmt.Errorf("column %q is used by ClickHouse storage; its removal or modification requires a storage rewrite", name)
			}
			if usesColumn(request, table, ttl, effect.Subject) {
				return "", fmt.Errorf("column %q is used by the retained TTL; remove or replace that rule before changing the column", name)
			}
		}
	}
	return "retain storage settings and order common column changes around planned storage changes", nil
}

// referencedColumns returns the columns any of clauses uses.
func referencedColumns(columns []string, clauses ...string) map[string]bool {
	result := make(map[string]bool)
	for _, clause := range clauses {
		maps.Copy(result, chkey.ReferencedColumns(clause, columns))
	}
	return result
}

// additions are the common column additions a new sorting key takes over: the
// steps it replaces, the columns its ALTER TABLE adds, their effects, and the
// notes the host would have written before each.
type additions struct {
	steps   []plangraph.StepID
	columns []chast.Column
	effects []plangraph.Effect
	notes   []string
}

// keyColumnAdditions takes over the table's column additions when the sorting
// key gains an expression. Measured on 24.10, 25.8 and 26.9, MODIFY ORDER BY
// accepts a new expression only over columns the same ALTER TABLE adds:
// `ALTER TABLE t ADD COLUMN c Int32` followed by `ALTER TABLE t MODIFY ORDER BY
// (a, c)` answers `Existing column c is used in the expression that was added
// to the sorting key`. Every addition of the table is taken over, not only the
// ones the key uses, because the host runs a table's additions as one run of
// steps, and a replacement that skipped one in the middle of it would have to
// run both before and after it.
//
// A column a new expression uses must be NOT NULL and have no DEFAULT,
// MATERIALIZED or ALIAS value: the server answers `Sorting key contains
// nullable columns` for the one and `Newly added column e has a default
// expression` for the other. A column the table already has is refused by
// name.
func keyColumnAdditions(request featureplan.Request, table featureplan.Table, before, after *chschema.ObservedTable) (additions, error) {
	if chsql.SameKey(before.OrderBy, after.OrderBy) {
		return additions{}, nil
	}
	key, err := chsql.SortingKey(before.OrderBy, after.OrderBy, after.PrimaryKey)
	if err != nil || len(key.Added) == 0 {
		return additions{}, err
	}
	columns := capturedColumns(request, table)
	keyed := referencedColumns(columns, key.Added...)
	var result additions
	added := make(map[objectidentity.Key]bool)
	for _, common := range request.CommonSteps {
		if common.Parent.Key() != table.Subject.Key() {
			continue
		}
		var created []plangraph.Effect
		for _, effect := range common.Effects {
			if columnOf(effect.Subject, table.Subject) && effect.Action == plangraph.Create {
				created = append(created, effect)
			}
		}
		if len(created) == 0 {
			continue
		}
		uses := slices.ContainsFunc(created, func(effect plangraph.Effect) bool { return usesColumn(request, table, keyed, effect.Subject) })
		column, notes, err := addedColumn(common, created[0], uses)
		if err != nil {
			return additions{}, err
		}
		result.columns = append(result.columns, column)
		result.notes = append(result.notes, notes...)
		result.steps = append(result.steps, common.ID)
		result.effects = append(result.effects, common.Effects...)
		for _, effect := range created {
			added[effect.Subject.Key()] = true
		}
	}
	builder := objectidentity.NewBuilder(request.Identifiers)
	for _, name := range slices.Sorted(maps.Keys(keyed)) {
		if !added[builder.ColumnParts(table.Subject.Schema.Source, table.Subject.Name.Source, name).Key()] {
			return additions{}, fmt.Errorf("%w: column %q is already in the table, and ClickHouse adds to the sorting key only expressions over columns the same ALTER TABLE adds; create the table again and copy its rows",
				ptaherr.ErrUnsupportedFeature, name)
		}
	}
	return result, nil
}

// addedColumn spells one column the storage operation takes over. keyed
// says whether a new sorting-key expression uses it.
func addedColumn(common featureplan.CommonStep, created plangraph.Effect, keyed bool) (chast.Column, []string, error) {
	column := common.AddedColumn
	if column == nil {
		return chast.Column{}, nil, fmt.Errorf("%w: the plan adds column %q without its definition, which the ALTER TABLE that changes the sorting key needs",
			schemaext.ErrInvalidValue, created.Subject.Name.Source)
	}
	keyword, expression := chcolumn.Value(column)
	typeOf := chcolumn.Type
	switch {
	case keyed && chcolumn.Nullable(column):
		return chast.Column{}, nil, fmt.Errorf("%w: column %q joins the sorting key and must be NOT NULL: ClickHouse refuses a Nullable sorting-key column", ptaherr.ErrUnsupportedFeature, column.Name)
	case keyed && keyword != "":
		return chast.Column{}, nil, fmt.Errorf("%w: column %q joins the sorting key, and ClickHouse refuses a %s value for a column a new sorting-key expression uses", ptaherr.ErrUnsupportedFeature, column.Name, keyword)
	case keyed:
		typeOf = chcolumn.KeyType
	}
	mapping, err := typeOf(column)
	if err != nil {
		return chast.Column{}, nil, err
	}
	var notes []string
	if mapping.Notice != "" {
		notes = append(notes, fmt.Sprintf("CLICKHOUSE: column %q %s", column.Name, mapping.Notice))
	}
	return chast.Column{Name: column.Name, Type: mapping.Type, Value: keyword, Expression: expression, Comment: column.Comment}, notes, nil
}

// withEffects appends each extra effect whose subject effects does not name
// yet. A step names a subject once.
func withEffects(effects []plangraph.Effect, extra ...plangraph.Effect) []plangraph.Effect {
	for _, effect := range extra {
		if !slices.ContainsFunc(effects, func(existing plangraph.Effect) bool { return existing.Subject.Key() == effect.Subject.Key() }) {
			effects = append(effects, effect)
		}
	}
	return effects
}

func capturedColumns(request featureplan.Request, table featureplan.Table) []string {
	var columns []string
	for _, column := range table.Current.Table.Columns {
		columns = append(columns, column.Name)
	}
	for _, column := range table.Desired.Fields {
		columns = append(columns, column.Name)
	}
	for _, common := range request.CommonSteps {
		for _, effect := range common.Effects {
			if columnOf(effect.Subject, table.Subject) {
				columns = append(columns, effect.Subject.Name.Source)
			}
		}
	}
	slices.Sort(columns)
	return slices.Compact(columns)
}

// storageDependencies orders the storage operation against the common column
// operations it does not take over. A column a new sorting key, sampling key
// or TTL uses is added before it and cannot be removed or modified; a column
// only the old clauses use is removed or modified after it.
func storageDependencies(request featureplan.Request, table featureplan.Table, before, after *chschema.ObservedTable, id plangraph.StepID, claimed []plangraph.StepID) ([]plangraph.Dependency, []plangraph.Effect, error) {
	columns := capturedColumns(request, table)
	previous := referencedColumns(columns, before.OrderBy, before.SampleBy, before.TTL)
	desired := referencedColumns(columns, after.OrderBy, after.SampleBy, after.TTL)
	var edges []plangraph.Dependency
	var effects []plangraph.Effect
	builder := objectidentity.NewBuilder(request.Identifiers)
	for _, column := range columns {
		if previous[column] || desired[column] {
			effects = append(effects, plangraph.Effect{Subject: builder.ColumnParts(table.Subject.Schema.Source, table.Subject.Name.Source, column), Action: plangraph.Read})
		}
	}
	for _, common := range request.CommonSteps {
		if common.Parent.Key() != table.Subject.Key() || slices.Contains(claimed, common.ID) {
			continue
		}
		for _, effect := range common.Effects {
			if !columnOf(effect.Subject, table.Subject) {
				continue
			}
			name := effect.Subject.Name.Source
			switch effect.Action {
			case plangraph.Create:
				if usesColumn(request, table, desired, effect.Subject) {
					edges = append(edges, plangraph.Dependency{Before: common.ID, After: id})
				}
			case plangraph.Drop, plangraph.Alter:
				if usesColumn(request, table, desired, effect.Subject) {
					return nil, nil, fmt.Errorf("%w: column %q is used by the table's storage clauses after the change; its %s cannot be scheduled", ptaherr.ErrUnsupportedFeature, name, effect.Action)
				}
				if usesColumn(request, table, previous, effect.Subject) {
					edges = append(edges, plangraph.Dependency{Before: id, After: common.ID})
				}
			}
		}
	}
	return edges, effects, nil
}

func columnOf(column, table objectidentity.ID) bool {
	return column.Kind == objectidentity.KindColumn && column.Catalog.Normalized == table.Catalog.Normalized &&
		column.Schema.Normalized == table.Schema.Normalized && column.Parent.Normalized == table.Name.Normalized
}

func usesColumn(request featureplan.Request, table featureplan.Table, references map[string]bool, column objectidentity.ID) bool {
	builder := objectidentity.NewBuilder(request.Identifiers)
	for name := range references {
		if builder.ColumnParts(table.Subject.Schema.Source, table.Subject.Name.Source, name).Key() == column.Key() {
			return true
		}
	}
	return false
}
