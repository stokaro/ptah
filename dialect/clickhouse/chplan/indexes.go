package chplan

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/clickhouse/chast"
	"ptah.run/dialect/clickhouse/chdiff"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/dialect/clickhouse/internal/chkey"
	"ptah.run/dialect/clickhouse/internal/chsql"
	"ptah.run/internal/schemaprep"
)

// IndexService plans changes to the type and granularity of surviving
// data-skipping indexes and accounts for captured index settings through
// parent operations. ClickHouse changes neither setting in place, so a change
// replaces the index: DROP INDEX, then ADD INDEX with the declared key
// expression and the desired settings. A declared key that names a column the
// table will not have is refused, because the old index is gone before the new
// one is added. When the common plan already replaces the same index, that
// replacement carries the desired settings and this service contributes no
// second one. Its zero value supports concurrent use without database access.
type IndexService struct{}

// PlanFeatures returns complete receipts or a completed refusal with no usable
// prefix. Errors describe invalid requests or cancellation. A successful reply
// must join the host graph before any operation is rendered or executed.
func (IndexService) PlanFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
	if ctx == nil {
		return featureplan.Result{}, fmt.Errorf("%w: index planning requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return featureplan.Result{}, err
	}
	if request.Target != platform.ClickHouse {
		return featureplan.Result{}, fmt.Errorf("%w: ClickHouse index planning on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if len(request.ParentKinds) > 0 && !slices.Equal(request.ParentKinds, []schemaext.Kind{chschema.IndexKind}) {
		return featureplan.Result{}, fmt.Errorf("%w: unsupported ClickHouse index parent kinds", schemaext.ErrInvalidValue)
	}
	result := featureplan.Result{Complete: true}
	for i, record := range request.Changes {
		if err := ctx.Err(); err != nil {
			return featureplan.Result{}, err
		}
		contribution, plan, err := planIndexChange(request, record, i)
		if err != nil {
			if canceled := ctx.Err(); canceled != nil {
				return featureplan.Result{}, canceled
			}
			return indexRefusal(chdiff.IndexKind, err, new(i), nil), nil
		}
		if len(contribution.Steps) > 0 {
			result.Contributions = append(result.Contributions, contribution)
		}
		result.Changes = append(result.Changes, plan)
	}
	for i, table := range request.Tables {
		if table.Action == "" || len(request.ParentKinds) == 0 {
			continue
		}
		if err := ctx.Err(); err != nil {
			return featureplan.Result{}, err
		}
		strategy, err := assessIndexParent(request, table)
		if err != nil {
			return indexRefusal(chschema.IndexKind, err, nil, new(i)), nil
		}
		result.Parents = append(result.Parents, featureplan.ParentPlan{Subject: table.Subject, Kind: chschema.IndexKind, Action: table.Action, Strategy: strategy})
	}
	if err := ctx.Err(); err != nil {
		return featureplan.Result{}, err
	}
	return result, nil
}

func indexRefusal(kind schemaext.Kind, err error, change, parent *int) featureplan.Result {
	result := refusal(kind, err, change, parent)
	result.Diagnostics[0].Problem.Feature = "ClickHouse skipping-index planning"
	return result
}

// capturedIndex is one index as both captured sides state it.
type capturedIndex struct {
	subject objectidentity.ID
	table   featureplan.Table
	current catalog.Index
	desired schemamodel.Index
}

func planIndexChange(request featureplan.Request, record schemaext.ChangeRecord, position int) (plangraph.Contribution[featureplan.Operation], featureplan.ChangePlan, error) {
	var contribution plangraph.Contribution[featureplan.Operation]
	plan := featureplan.ChangePlan{Subject: record.Subject, Kind: chdiff.IndexKind}
	change, ok := record.Value.(*chdiff.Index)
	if !ok || record.Subject.Kind != objectidentity.KindIndex || record.Subject.Name.Empty() || record.Subject.Parent.Empty() {
		return contribution, plan, fmt.Errorf("%w: expected a ClickHouse change on a table-owned index", schemaext.ErrInvalidValue)
	}
	if err := chdiff.ValidateIndex(change); err != nil {
		return contribution, plan, err
	}
	captured, err := captureIndex(request, record.Subject)
	if err != nil {
		return contribution, plan, err
	}
	if err := captured.matches(change); err != nil {
		return contribution, plan, err
	}
	switch replaced, err := commonReplacement(request, record.Subject); {
	case err != nil:
		return contribution, plan, err
	case replaced:
		plan.Strategy = "the common index replacement recreates the index with the desired settings"
		return contribution, plan, nil
	}
	after, err := change.After.Observed()
	if err != nil {
		return contribution, plan, err
	}
	// The replacement builds the declared index, as a rebuild does on every
	// other target. The captured expression only orders the old index's
	// removal around changes to the columns it reads.
	previous, err := indexExpression(captured.current.Name, captured.current.Columns)
	if err != nil {
		return contribution, plan, err
	}
	expression, err := indexExpression(captured.desired.Name, schemaprep.IndexKeyParts(captured.desired))
	if err != nil {
		return contribution, plan, err
	}
	if err := refuseUndeclaredKeyColumns(request, captured, expression); err != nil {
		return contribution, plan, err
	}
	contribution.Owner = "ptah.run/clickhouse"
	table := captured.table.Subject
	drop := &chast.DropSkippingIndex{Name: captured.current.Name}
	add := &chast.AddSkippingIndex{Name: captured.current.Name, Expression: expression, IndexType: after.IndexType, Granularity: after.Granularity}
	dropID := plangraph.StepID{Owner: contribution.Owner, Name: fmt.Sprintf("index-settings/%d/drop", position)}
	addID := plangraph.StepID{Owner: contribution.Owner, Name: fmt.Sprintf("index-settings/%d/add", position)}
	reads, dependencies, err := indexColumnDependencies(request, captured.table, previous, expression, dropID, addID)
	if err != nil {
		return plangraph.Contribution[featureplan.Operation]{}, plan, err
	}
	contribution.Steps = []plangraph.Step[featureplan.Operation]{
		{
			ID: dropID, Payload: featureplan.Operation{Role: ast.AlterExtension, Parent: table, Payload: drop},
			Effects:     []plangraph.Effect{{Subject: table, Action: plangraph.Read}, {Subject: record.Subject, Action: plangraph.Drop}},
			Transaction: plangraph.TransactionForbidden, Impact: drop.Effect(),
		},
		{
			ID: addID, Payload: featureplan.Operation{Role: ast.AlterExtension, Parent: table, Payload: add},
			Effects:     append([]plangraph.Effect{{Subject: table, Action: plangraph.Read}, {Subject: record.Subject, Action: plangraph.Create}}, reads...),
			Transaction: plangraph.TransactionForbidden, Impact: add.Effect(),
		},
	}
	contribution.Dependencies = append([]plangraph.Dependency{{Before: dropID, After: addID}}, dependencies...)
	plan.Strategy = "replace the index with its declared key expression and the desired settings; data built for existing parts is discarded"
	plan.Steps = []plangraph.StepID{dropID, addID}
	return contribution, plan, nil
}

// captureIndex finds the index on both captured sides. A settings change is
// planned only for a surviving index of a surviving table.
func captureIndex(request featureplan.Request, subject objectidentity.ID) (capturedIndex, error) {
	position := slices.IndexFunc(request.Tables, func(table featureplan.Table) bool { return indexOf(subject, table.Subject) })
	if position < 0 {
		return capturedIndex{}, fmt.Errorf("%w: index change requires captured parent state", schemaext.ErrInvalidValue)
	}
	result := capturedIndex{subject: subject, table: request.Tables[position]}
	if result.table.Action != "" && result.table.Action != featureplan.AlterTable {
		return capturedIndex{}, fmt.Errorf("%w: an index settings change requires a surviving table outside a rebuild", schemaext.ErrInvalidValue)
	}
	if !result.table.Current.HasTable() || !result.table.Desired.HasTable() {
		return capturedIndex{}, fmt.Errorf("%w: an index settings change requires both captured table sides", schemaext.ErrInvalidValue)
	}
	builder := objectidentity.NewBuilder(request.Identifiers)
	current := slices.IndexFunc(result.table.Current.Indexes, func(index catalog.Index) bool {
		return builder.IndexParts(index.Schema, index.TableName, index.Name).Key() == subject.Key()
	})
	desired := slices.IndexFunc(result.table.Desired.Indexes, func(index schemamodel.Index) bool {
		return builder.IndexParts(result.table.Subject.Schema.Source, result.table.Subject.Name.Source, index.Name).Key() == subject.Key()
	})
	if current < 0 || desired < 0 {
		return capturedIndex{}, fmt.Errorf("%w: index %s is not captured on both sides of its table", schemaext.ErrInvalidValue, subject)
	}
	result.current, result.desired = result.table.Current.Indexes[current], result.table.Desired.Indexes[desired]
	return result, nil
}

// matches requires the change to agree with the captured operands, so a
// replacement cannot silently apply settings the comparison did not decide.
func (c capturedIndex) matches(change *chdiff.Index) error {
	if knowledge, found := c.table.Current.FeatureCoverage.SubjectKnowledge(chschema.IndexKind, c.subject); found && knowledge.State != schemaext.Complete {
		return fmt.Errorf("%w: captured ClickHouse index settings have a knowledge limit: %s", schemaext.ErrInvalidValue, knowledge.Reason)
	}
	current, found, err := schemaext.FacetAs[*chschema.ObservedIndex](c.current.Facets, chschema.IndexKind)
	if err != nil || !found {
		return fmt.Errorf("%w: captured ClickHouse index observation is missing or invalid", schemaext.ErrInvalidValue)
	}
	desired, found, err := schemaext.FacetAs[*chschema.DesiredIndex](c.desired.Facets, chschema.IndexKind)
	if err != nil || !found {
		return fmt.Errorf("%w: captured ClickHouse index declaration is missing or invalid", schemaext.ErrInvalidValue)
	}
	resolved, err := desired.Observed()
	if err != nil {
		return err
	}
	projected, err := change.After.Observed()
	if err != nil {
		return err
	}
	if !chsql.SameIndex(current, change.Before) || !chsql.SameIndex(resolved, projected) {
		return fmt.Errorf("%w: index settings change disagrees with captured index operands", schemaext.ErrInvalidValue)
	}
	return nil
}

// commonReplacement reports whether host operations drop and create the index.
// A host operation that only drops or only creates a surviving index
// contradicts the comparison that produced the settings change.
func commonReplacement(request featureplan.Request, subject objectidentity.ID) (bool, error) {
	var dropped, created bool
	for _, step := range request.CommonSteps {
		for _, effect := range step.Effects {
			if effect.Subject.Key() != subject.Key() {
				continue
			}
			switch effect.Action {
			case plangraph.Drop:
				dropped = true
			case plangraph.Create:
				created = true
			case plangraph.Alter:
				return false, fmt.Errorf("%w: a common operation alters index %s whose settings change", schemaext.ErrInvalidValue, subject)
			}
		}
	}
	if dropped != created {
		return false, fmt.Errorf("%w: the common plan removes or adds index %s whose settings change", schemaext.ErrInvalidValue, subject)
	}
	return dropped, nil
}

// indexExpression writes key parts the way the ClickHouse renderer does.
func indexExpression(name string, parts []string) (string, error) {
	if len(parts) == 0 || slices.ContainsFunc(parts, func(part string) bool { return strings.TrimSpace(part) == "" }) {
		return "", fmt.Errorf("%w: index %q has no key expression", schemaext.ErrInvalidValue, name)
	}
	return chast.SkippingIndexExpression(parts), nil
}

// refuseUndeclaredKeyColumns refuses a declared key that names a column the
// table will not have once the plan runs. The replacement drops the old index
// outside a transaction before it adds the new one, so an ADD INDEX the server
// refuses for a missing column would leave the table without the index. Only
// names that can stand for nothing but a column are checked; see
// chkey.PlainColumnReferences.
func refuseUndeclaredKeyColumns(request featureplan.Request, captured capturedIndex, expression string) error {
	builder := objectidentity.NewBuilder(request.Identifiers)
	table := captured.table.Subject
	declared := make(map[objectidentity.Key]bool, len(captured.table.Desired.Fields))
	for _, field := range captured.table.Desired.Fields {
		declared[builder.ColumnParts(table.Schema.Source, table.Name.Source, field.Name).Key()] = true
	}
	for _, column := range chkey.PlainColumnReferences(expression) {
		if !declared[builder.ColumnParts(table.Schema.Source, table.Name.Source, column).Key()] {
			return fmt.Errorf("%w: index %q reads column %q, which table %s does not declare; the settings replacement drops the index before it adds it, so it is refused",
				schemaext.ErrInvalidValue, captured.desired.Name, column, table.Name.Source)
		}
	}
	return nil
}

// indexColumnDependencies orders the replacement around common changes to the
// columns either expression reads: the old index goes before a column it reads
// changes or disappears, and the new one comes after a column it reads is added
// or changed. Removing a column the new expression reads cannot be scheduled.
func indexColumnDependencies(request featureplan.Request, table featureplan.Table, previous, expression string, drop, add plangraph.StepID) ([]plangraph.Effect, []plangraph.Dependency, error) {
	columns := capturedColumns(request, table)
	before, after := chkey.ReferencedColumns(previous, columns), chkey.ReferencedColumns(expression, columns)
	builder := objectidentity.NewBuilder(request.Identifiers)
	var reads []plangraph.Effect
	for _, column := range slices.Sorted(maps.Keys(after)) {
		reads = append(reads, plangraph.Effect{Subject: builder.ColumnParts(table.Subject.Schema.Source, table.Subject.Name.Source, column), Action: plangraph.Read})
	}
	var dependencies []plangraph.Dependency
	for _, common := range request.CommonSteps {
		if common.Parent.Key() != table.Subject.Key() {
			continue
		}
		for _, effect := range common.Effects {
			if !columnOf(effect.Subject, table.Subject) {
				continue
			}
			old, current := usesColumn(request, table, before, effect.Subject), usesColumn(request, table, after, effect.Subject)
			if effect.Action == plangraph.Drop && current {
				return nil, nil, fmt.Errorf("column %q is used by the expression of an index whose settings change; its removal cannot be scheduled", effect.Subject.Name.Source)
			}
			if old && (effect.Action == plangraph.Alter || effect.Action == plangraph.Drop) {
				dependencies = append(dependencies, plangraph.Dependency{Before: drop, After: common.ID})
			}
			if current && (effect.Action == plangraph.Alter || effect.Action == plangraph.Create) {
				dependencies = append(dependencies, plangraph.Dependency{Before: common.ID, After: add})
			}
		}
	}
	return reads, dependencies, nil
}

// assessIndexParent accounts for captured index settings through a parent
// operation. A surviving table keeps them unless a planned change or a common
// replacement covers the difference the comparison found.
func assessIndexParent(request featureplan.Request, table featureplan.Table) (string, error) {
	switch table.Action {
	case featureplan.DropTable:
		return "remove skipping-index settings with the table; the table's rows and index data are lost", nil
	case featureplan.AlterTable:
	default:
		return "", fmt.Errorf("ClickHouse skipping-index settings have no plan for parent action %q", table.Action)
	}
	if !table.Current.HasTable() || !table.Desired.HasTable() {
		return "retain skipping-index settings through common column changes", nil
	}
	builder := objectidentity.NewBuilder(request.Identifiers)
	for _, current := range table.Current.Indexes {
		observed, found, err := schemaext.FacetAs[*chschema.ObservedIndex](current.Facets, chschema.IndexKind)
		if err != nil {
			return "", err
		}
		if !found {
			continue
		}
		subject := builder.IndexParts(current.Schema, current.TableName, current.Name)
		position := slices.IndexFunc(table.Desired.Indexes, func(index schemamodel.Index) bool {
			return builder.IndexParts(table.Subject.Schema.Source, table.Subject.Name.Source, index.Name).Key() == subject.Key()
		})
		if position < 0 {
			continue
		}
		desired, found, err := schemaext.FacetAs[*chschema.DesiredIndex](table.Desired.Indexes[position].Facets, chschema.IndexKind)
		if err != nil {
			return "", err
		}
		if !found {
			continue
		}
		resolved, err := desired.Observed()
		if err != nil || chsql.SameIndex(observed, resolved) {
			continue
		}
		replaced, err := commonReplacement(request, subject)
		if err != nil {
			return "", err
		}
		if !replaced && !slices.ContainsFunc(request.Changes, func(change schemaext.ChangeRecord) bool {
			return change.Subject.Key() == subject.Key() && change.Value.Kind() == chdiff.IndexKind
		}) {
			return "", fmt.Errorf("settings of index %s changed without a corresponding feature change", subject)
		}
	}
	return "retain skipping-index settings; a settings change replaces its index around dependent column changes", nil
}

func indexOf(index, table objectidentity.ID) bool {
	return table.Kind == objectidentity.KindTable && index.Catalog.Normalized == table.Catalog.Normalized &&
		index.Schema.Normalized == table.Schema.Normalized && index.Parent.Normalized == table.Name.Normalized
}
