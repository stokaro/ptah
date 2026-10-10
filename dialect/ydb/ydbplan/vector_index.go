package ydbplan

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/ydbindex"
	"ptah.run/internal/ydbtype"
)

// VectorIndexService plans changes to the settings of surviving vector
// indexes and accounts for the settings through table removal and rebuild.
// Its zero value supports concurrent use without database access.
//
// YDB changes no setting of a built vector index (`ALTER INDEX ... SET
// (levels = 2)` answers `Unknown table setting: levels`), so a change builds
// the index again: DROP INDEX, then ADD INDEX with the declared columns and
// the resolved settings. When the common plan already drops and adds the same
// index, because its kind or its columns change too, that replacement carries
// the declared settings and no second one is planned; a table rebuild does
// the same for every index of the table.
//
// A declaration YDB would refuse is refused before any statement: one that
// does not resolve (see [ydbindex.ResolveVector]), vector settings on an index
// of another kind, a unique or partitioned vector index, a vector index over
// a column it cannot read, and a target without the vector index keys. The old
// index is gone before the new one is added, so a refusal at apply time would
// leave the table without it.
type VectorIndexService struct{}

// PlanFeatures returns complete receipts or a completed refusal with no usable
// prefix. Errors describe invalid requests or cancellation. A successful reply
// must join the host's plan before any operation is rendered or executed.
func (VectorIndexService) PlanFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
	if ctx == nil {
		return featureplan.Result{}, fmt.Errorf("%w: planning requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return featureplan.Result{}, err
	}
	if request.Target != platform.YDB {
		return featureplan.Result{}, fmt.Errorf("%w: YDB vector index planning on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if len(request.ParentKinds) > 0 && !slices.Equal(request.ParentKinds, []schemaext.Kind{ydbschema.VectorIndexKind}) {
		return featureplan.Result{}, fmt.Errorf("%w: unsupported YDB vector index parent kinds", schemaext.ErrInvalidValue)
	}
	result := featureplan.Result{Complete: true}
	for i, record := range request.Changes {
		if err := ctx.Err(); err != nil {
			return featureplan.Result{}, err
		}
		contribution, plan, err := planVectorChange(request, record, i)
		if err != nil {
			return vectorRefusal(ydbdiff.VectorIndexKind, err, new(i), nil), nil
		}
		if len(contribution.Steps) > 0 {
			result.Contributions = append(result.Contributions, contribution)
		}
		result.Changes = append(result.Changes, plan)
	}
	if len(request.ParentKinds) == 0 {
		return result, ctx.Err()
	}
	for i, table := range request.Tables {
		if table.Action == "" {
			continue
		}
		strategy, err := vectorParentStrategy(table.Action)
		if err != nil {
			return vectorRefusal(ydbschema.VectorIndexKind, err, nil, new(i)), nil
		}
		result.Parents = append(result.Parents, featureplan.ParentPlan{Subject: table.Subject, Kind: ydbschema.VectorIndexKind, Action: table.Action, Strategy: strategy})
	}
	if err := ctx.Err(); err != nil {
		return featureplan.Result{}, err
	}
	return result, nil
}

func vectorRefusal(kind schemaext.Kind, err error, change, parent *int) featureplan.Result {
	refusal := ttlRefusal(kind, err, change, parent)
	refusal.Diagnostics[0].Problem.Feature = "YDB vector index planning"
	return refusal
}

// vectorParentStrategy says what becomes of a table's vector index settings
// through a common table operation.
func vectorParentStrategy(action featureplan.ParentAction) (string, error) {
	switch action {
	case featureplan.AlterTable:
		return "retain vector index settings; a settings change builds its index again", nil
	case featureplan.DropTable:
		return "remove vector indexes and their settings with the table", nil
	case featureplan.RebuildTable:
		return "the rebuilt table builds each vector index again with its declared settings", nil
	case featureplan.CreateTable:
		return "CREATE TABLE builds each vector index with its declared settings", nil
	default:
		return "", fmt.Errorf("YDB vector index settings have no plan for parent action %q", action)
	}
}

func planVectorChange(request featureplan.Request, record schemaext.ChangeRecord, position int) (plangraph.Contribution[featureplan.Operation], featureplan.ChangePlan, error) {
	var contribution plangraph.Contribution[featureplan.Operation]
	plan := featureplan.ChangePlan{Subject: record.Subject, Kind: ydbdiff.VectorIndexKind}
	change, ok := record.Value.(*ydbdiff.VectorIndex)
	if !ok || record.Subject.Kind != objectidentity.KindIndex || record.Subject.Name.Empty() || record.Subject.Parent.Empty() {
		return contribution, plan, fmt.Errorf("%w: expected a YDB vector index change on a table-owned index", schemaext.ErrInvalidValue)
	}
	if err := ydbdiff.ValidateVectorIndex(change); err != nil {
		return contribution, plan, err
	}
	tablePosition := slices.IndexFunc(request.Tables, func(table featureplan.Table) bool { return vectorIndexOf(record.Subject, table.Subject) })
	if tablePosition < 0 {
		return contribution, plan, fmt.Errorf("%w: a vector index change requires captured parent state", schemaext.ErrInvalidValue)
	}
	table := request.Tables[tablePosition]
	switch table.Action {
	case featureplan.RebuildTable:
		plan.Strategy = "the table rebuild builds the index again with the desired settings"
		return contribution, plan, nil
	case "", featureplan.AlterTable:
	default:
		return contribution, plan, fmt.Errorf("%w: a vector index settings change requires a surviving table", schemaext.ErrInvalidValue)
	}
	replaced, err := commonIndexReplacement(request, record.Subject)
	if err != nil {
		return contribution, plan, err
	}
	if replaced {
		plan.Strategy = "the common index replacement builds the index again with the desired settings"
		return contribution, plan, nil
	}
	current, desired, err := capturedVectorIndex(request, table, record.Subject)
	if err != nil {
		return contribution, plan, err
	}
	add, err := vectorIndexCreation(request, table, desired, change.After)
	if err != nil {
		return contribution, plan, err
	}
	contribution.Owner = ydbschema.Owner
	drop := &ydbast.DropVectorIndex{Name: current.Name}
	add.Name = current.Name
	// Zero-padded, because a scheduler orders independent steps by name and
	// the statements should keep the order of the changes.
	dropID := plangraph.StepID{Owner: contribution.Owner, Name: fmt.Sprintf("vector-index/%06d/drop", position)}
	addID := plangraph.StepID{Owner: contribution.Owner, Name: fmt.Sprintf("vector-index/%06d/add", position)}
	contribution.Steps = []plangraph.Step[featureplan.Operation]{
		{
			ID: dropID, Payload: featureplan.Operation{Role: ast.AlterExtension, Parent: table.Subject, Payload: drop},
			Effects:     []plangraph.Effect{{Subject: table.Subject, Action: plangraph.Read}, {Subject: record.Subject, Action: plangraph.Drop}},
			Transaction: plangraph.TransactionForbidden, Impact: drop.Effect(),
		},
		{
			ID: addID, Payload: featureplan.Operation{Role: ast.AlterExtension, Parent: table.Subject, Payload: add},
			Effects:     []plangraph.Effect{{Subject: table.Subject, Action: plangraph.Read}, {Subject: record.Subject, Action: plangraph.Create}},
			Transaction: plangraph.TransactionForbidden, Impact: add.Effect(),
		},
	}
	contribution.Dependencies = []plangraph.Dependency{{Before: dropID, After: addID}}
	plan.Strategy = "drop the index and add it again with the desired settings; YDB builds it from the table's rows"
	plan.Steps = []plangraph.StepID{dropID, addID}
	return contribution, plan, nil
}

// commonIndexReplacement reports whether common operations drop and add the
// index. A common operation that only drops it or only adds it contradicts
// the comparison that found it on both sides.
func commonIndexReplacement(request featureplan.Request, subject objectidentity.ID) (bool, error) {
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
			default:
			}
		}
	}
	if dropped != created {
		return false, fmt.Errorf("%w: the common plan removes or adds %s, whose vector settings change", schemaext.ErrInvalidValue, subject)
	}
	return dropped, nil
}

// capturedVectorIndex finds the index on both captured sides of its table.
func capturedVectorIndex(request featureplan.Request, table featureplan.Table, subject objectidentity.ID) (catalog.Index, schemamodel.Index, error) {
	if !table.Current.HasTable() || !table.Desired.HasTable() {
		return catalog.Index{}, schemamodel.Index{}, fmt.Errorf("%w: a vector index settings change requires both captured table sides", schemaext.ErrInvalidValue)
	}
	builder := objectidentity.NewBuilder(request.Identifiers)
	current := slices.IndexFunc(table.Current.Indexes, func(index catalog.Index) bool {
		return builder.IndexParts(table.Subject.Schema.Source, table.Subject.Name.Source, index.Name).Key() == subject.Key()
	})
	desired := slices.IndexFunc(table.Desired.Indexes, func(index schemamodel.Index) bool {
		return builder.IndexParts(table.Subject.Schema.Source, table.Subject.Name.Source, index.Name).Key() == subject.Key()
	})
	if current < 0 || desired < 0 {
		return catalog.Index{}, schemamodel.Index{}, fmt.Errorf("%w: index %s is not captured on both sides of its table", schemaext.ErrInvalidValue, subject)
	}
	return table.Current.Indexes[current], table.Desired.Indexes[desired], nil
}

// vectorIndexCreation is the ADD INDEX that builds index with the declared
// settings, refusing what YDB would refuse for it.
func vectorIndexCreation(request featureplan.Request, table featureplan.Table, index schemamodel.Index, declared *ydbschema.DesiredVectorIndex) (*ydbast.AddVectorIndex, error) {
	subject := fmt.Sprintf("index %q", index.Name)
	kind, err := ydbindex.KindOf(index.Type)
	switch {
	case err != nil:
		return nil, fmt.Errorf("%s: %w", subject, err)
	case kind != ydbindex.Vector:
		return nil, fmt.Errorf("%s: it declares vector settings and is a %s index; declare type %q for a vector index",
			subject, kind, ydbindex.VectorMethod)
	case declared == nil:
		return nil, fmt.Errorf("%s: a %s index declares none of its settings", subject, ydbindex.VectorMethod)
	case !request.Capabilities.Has(capability.VectorIndexes):
		return nil, fmt.Errorf("%s is a vector index, which requires target capability %s, unavailable on this %s target",
			subject, capability.VectorIndexes, platform.YDB)
	case index.Unique:
		return nil, fmt.Errorf("%s: a vector index is not unique (`VECTOR_KMEANS_TREE index can only be GLOBAL [SYNC]`)", subject)
	case !index.Partitioning.IsZero():
		return nil, fmt.Errorf("%s: a vector index keeps the partitioning YDB gives it "+
			"(`ALTER INDEX ... SET` answers `Only index with one impl table is supported`)", subject)
	}
	if names := slices.Sorted(maps.Keys(index.StorageParams)); len(names) > 0 {
		return nil, fmt.Errorf("%s: %s", subject, ydbindex.StorageParameterRefusal(names[0]))
	}
	if err := ydbindex.CheckVectorOperator(declared.Settings(), index.Operator); err != nil {
		return nil, fmt.Errorf("%s: %w", subject, err)
	}
	settings, err := ydbindex.ResolveVector(new(declared.Settings()), "")
	if err != nil {
		return nil, fmt.Errorf("%s: %w", subject, err)
	}
	if settings.VectorType == ydbindex.BitVectorType && !request.Capabilities.Has(capability.VectorBitType) {
		return nil, fmt.Errorf("%s stores bit vectors, which requires target capability %s, unavailable on this %s target",
			subject, capability.VectorBitType, platform.YDB)
	}
	columns := indexColumns(index)
	if reason := vectorShapeRefusal(request.Capabilities, table, columns, index.IncludeColumns, settings.Dimension); reason != "" {
		return nil, fmt.Errorf("%s: %s", subject, reason)
	}
	return &ydbast.AddVectorIndex{Columns: columns, Cover: slices.Clone(index.IncludeColumns), Settings: settings}, nil
}

// indexColumns are the columns an index is keyed on, from its structured
// parts where it has them.
func indexColumns(index schemamodel.Index) []string {
	if len(index.Parts) == 0 {
		return slices.Clone(index.Fields)
	}
	columns := make([]string, 0, len(index.Parts))
	for _, part := range index.Parts {
		columns = append(columns, part.Name)
	}
	return columns
}

// vectorShapeRefusal asks [ydbindex.ShapeRefusal] about the vector index on
// the declared table: its key from the table or from its key fields, and each
// column's type through the map the renderer writes with. A column whose type
// the map refuses is answered as orderable, because that refusal is the
// column's and is reported where the column is written.
func vectorShapeRefusal(caps capability.Capabilities, table featureplan.Table, columns, cover []string, dimension uint64) string {
	declared := make(map[string]ydbindex.Column, len(table.Desired.Fields))
	var fieldKey []string
	for _, field := range table.Desired.Fields {
		mapping, err := ydbtype.Map(field.Type, caps)
		if err != nil {
			mapping = ydbtype.Mapping{}
		}
		declared[field.Name] = ydbindex.Column{Type: mapping.Type, Dimension: mapping.Dimension}
		if field.Primary {
			fieldKey = append(fieldKey, field.Name)
		}
	}
	key := table.Desired.Table.PrimaryKey
	if len(key) == 0 {
		key = fieldKey
	}
	shape := ydbindex.Shape{Kind: ydbindex.Vector, Columns: columns, Cover: cover, Dimension: dimension}
	return ydbindex.ShapeRefusal(shape, key, func(name string) (ydbindex.Column, bool) {
		column, ok := declared[name]
		return column, ok
	})
}

func vectorIndexOf(index, table objectidentity.ID) bool {
	return table.Kind == objectidentity.KindTable && index.Catalog.Normalized == table.Catalog.Normalized &&
		index.Schema.Normalized == table.Schema.Normalized && index.Parent.Normalized == table.Name.Normalized
}
