package providerhost

import (
	"context"
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

type reader struct {
	session *session
	schemas []string
}

func (r reader) ReadSchema() (*catalog.Database, error) {
	return r.ReadSchemaContext(context.Background())
}
func (r reader) ReadSchemaContext(ctx context.Context) (*catalog.Database, error) {
	if r.session.closed.Load() {
		return nil, fmt.Errorf("provider session is closed")
	}
	request := &protocol.InspectRequest{SessionId: r.session.remote.GetId(), Schemas: slices.Clone(r.schemas)}
	snapshot, err := r.session.target.host.client.Inspect(ctx, request)
	if err != nil {
		return nil, err
	}
	if snapshot.GetEngine() != r.session.remote.GetEngine() || !slices.Equal(featureSet(snapshot.GetTargetFeatures()), featureSet(r.session.remote.GetFeatures())) {
		return nil, fmt.Errorf("%w: inspection target differs from session", provider.ErrProtocol)
	}
	if err := r.validateInspectionBindings(snapshot.GetScopeBindings()); err != nil {
		return nil, err
	}
	if len(snapshot.GetDispositions()) != 0 {
		return nil, unsupported("inspection dispositions")
	}
	values, err := r.observedValues(ctx, request, snapshot)
	if err != nil {
		return nil, err
	}
	result := &catalog.Database{DatabasePath: r.session.remote.GetDatabase()}
	result.ReservedNames, err = r.reservedNames(snapshot)
	if err != nil {
		return nil, err
	}
	result.UnreadTables, err = r.unreadTables(snapshot, result.ReservedNames)
	if err != nil {
		return nil, err
	}
	if err := r.features(ctx, result, request, snapshot, values); err != nil {
		return nil, err
	}
	if err := r.tables(result, snapshot, values[result.FeatureObjects.Len():]); err != nil {
		return nil, err
	}
	if err := r.commonObjects(result, snapshot.GetCommonObjects()); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// tables reads the snapshot's tables into result, each with its facets, its
// columns' facets and its indexes with theirs. remaining holds the facet values
// in the order the tables, their columns and their indexes list them.
func (r reader) tables(result *catalog.Database, snapshot *protocol.Schema, remaining []observedValue) error {
	seen := make(map[objectidentity.Key]bool)
	for _, table := range snapshot.GetTables() {
		captured, err := r.table(table)
		if err != nil {
			return err
		}
		key := objectidentity.NewBuilder(r.session.info.IdentifierSemantics).TablePartsVerbatim(captured.Schema, captured.Name).Key()
		if seen[key] {
			return fmt.Errorf("%w: duplicate normalized table identity", provider.ErrProtocol)
		}
		seen[key] = true
		captured.Facets, err = observedFacets(remaining[:len(table.GetFacets())], snapshot.GetEngine())
		if err != nil {
			return err
		}
		remaining = remaining[len(table.GetFacets()):]
		for i, column := range table.GetColumns() {
			captured.Columns[i].Facets, err = observedFacets(remaining[:len(column.GetFacets())], snapshot.GetEngine())
			if err != nil {
				return err
			}
			remaining = remaining[len(column.GetFacets()):]
		}
		result.Tables = append(result.Tables, captured)
		if key := r.primaryKey(table); key != nil {
			result.Constraints = append(result.Constraints, *key)
		}
		indexes, err := r.indexes(table)
		if err != nil {
			return err
		}
		// The SDK accepts index facets only with observed indexes negotiated,
		// so an index the read lists is one r.indexes returned.
		for i, index := range table.GetIndexes() {
			indexes[i].Facets, err = observedFacets(remaining[:len(index.GetFacets())], snapshot.GetEngine())
			if err != nil {
				return err
			}
			remaining = remaining[len(index.GetFacets()):]
		}
		result.Indexes = append(result.Indexes, indexes...)
	}
	return nil
}

// indexes reads the common indexes a table holds when the session negotiated
// them. Without the feature a table reports none, and the planner refuses an
// index change of a provider-owned table rather than compare against nothing.
func (r reader) indexes(value *protocol.Table) ([]catalog.Index, error) {
	if !slices.Contains(r.session.remote.GetFeatures(), provider.ObservedIndexesFeature) {
		return nil, nil
	}
	result := make([]catalog.Index, 0, len(value.GetIndexes()))
	for _, index := range value.GetIndexes() {
		observed, err := catalogIndex(index)
		if err != nil {
			return nil, err
		}
		result = append(result, observed)
	}
	return result, nil
}

// primaryKey reads the primary key a table holds, in key order, as the
// catalog's PRIMARY KEY constraint, when the session negotiated ordered keys.
// The SDK has checked the record against the table's columns. Without the
// feature a table reports no key but its columns' membership, which states no
// order, and nil is returned.
func (r reader) primaryKey(value *protocol.Table) *catalog.Constraint {
	key := value.GetPrimaryKey()
	if !slices.Contains(r.session.remote.GetFeatures(), provider.ObservedPrimaryKeysFeature) || key == nil {
		return nil
	}
	return &catalog.Constraint{
		Name: key.GetName(), TableName: key.GetTableName(), Schema: key.GetSchema(), Type: "PRIMARY KEY",
		// The singular name is the first member, as the catalog readers write it.
		ColumnName: key.GetColumnNames()[0], ColumnNames: slices.Clone(key.GetColumnNames()), IncludeColumns: slices.Clone(key.GetIncludeColumns()),
		Deferrable: key.GetDeferrable(), Initially: key.GetInitially(), NotEnforced: key.GetNotEnforced(), UsingMethod: copyPlanningPointer(key.UsingMethod),
		Comment: key.GetComment(), HashShardBuckets: int(key.GetHashShardBuckets()),
	}
}

// features reads the snapshot's feature objects, which lead values, their
// coverage, and the planning context a filtered read leaves out of its scope.
func (r reader) features(ctx context.Context, result *catalog.Database, request *protocol.InspectRequest, snapshot *protocol.Schema, values []observedValue) error {
	var err error
	result.FeatureCoverage, err = r.observedCoverage(snapshot.GetCoverage())
	if err != nil {
		return err
	}
	var objects []schemaext.Object
	for _, value := range values[:len(snapshot.GetNativeObjects())] {
		objects = append(objects, schemaext.Object{Ref: value.subject, Value: value.value})
	}
	result.FeatureObjects, err = schemaext.NewObjects(objects...)
	if err != nil {
		return err
	}
	result.PlanningContextObjects, result.PlanningContextCoverage, err = r.planningContext(ctx, request, snapshot)
	return err
}

func (r reader) table(value *protocol.Table) (catalog.Table, error) {
	ref := value.GetIdentity()
	if ref.GetEngine() != r.session.info.Dialect || ref.GetDatabase() != r.session.remote.GetDatabase() ||
		ref.GetCatalog() != "" || len(ref.GetPath()) != 0 || len(ref.GetOverload()) != 0 || ref.GetKind() != "table" ||
		(ref.GetDisplayName() != "" && ref.GetDisplayName() != ref.GetName()) {
		return catalog.Table{}, unsupported("this table identity")
	}
	if _, err := r.observedIdentity(ref, value.GetNormalizedIdentity()); err != nil {
		return catalog.Table{}, err
	}
	result := catalog.Table{Name: ref.GetName(), Schema: ref.GetSchema(), Type: "TABLE", RowStatsUnknown: true}
	seen := make(map[string]bool)
	for i, column := range value.GetColumns() {
		key := r.session.info.IdentifierSemantics.ColumnIdentityKey(column.GetName())
		if seen[key] {
			return catalog.Table{}, fmt.Errorf("%w: duplicate normalized column identity", provider.ErrProtocol)
		}
		seen[key] = true
		if slices.Contains(r.session.remote.GetFeatures(), provider.ObservedColumnsFeature) {
			observed, err := catalogColumn(column.GetObserved())
			if err != nil {
				return catalog.Table{}, err
			}
			result.Columns = append(result.Columns, observed)
			continue
		}
		nullable := "NO"
		if column.GetNullable() {
			nullable = "YES"
		}
		var defaultExpression *string
		if column.DefaultExpression != nil {
			defaultExpression = new(column.GetDefaultExpression())
		}
		result.Columns = append(result.Columns, catalog.Column{
			Name: column.GetName(), DataType: column.GetType(), FormattedType: column.GetType(),
			IsNullable: nullable, IsPrimaryKey: column.GetPrimaryKey(), ColumnDefault: defaultExpression, OrdinalPosition: i + 1,
		})
	}
	return result, nil
}

func (r reader) validateInspectionBindings(bindings []*protocol.InspectionScopeBinding) error {
	for _, binding := range bindings {
		for _, scope := range binding.GetNamespaces() {
			if err := r.validateInspectionNamespace(scope); err != nil {
				return err
			}
			if _, err := commonNamespaceScopes(r.session.info.IdentifierSemantics, r.session.remote.GetEngine(), scope); err != nil {
				return err
			}
		}
	}
	return nil
}
