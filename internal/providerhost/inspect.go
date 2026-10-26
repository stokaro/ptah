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
	result := &catalog.Database{}
	result.ReservedNames, err = r.reservedNames(snapshot)
	if err != nil {
		return nil, err
	}
	result.FeatureCoverage, err = r.observedCoverage(snapshot.GetCoverage())
	if err != nil {
		return nil, err
	}
	var objects []schemaext.Object
	for _, value := range values[:len(snapshot.GetNativeObjects())] {
		objects = append(objects, schemaext.Object{Ref: value.subject, Value: value.value})
	}
	result.FeatureObjects, err = schemaext.NewObjects(objects...)
	if err != nil {
		return nil, err
	}
	remaining := values[len(objects):]
	seen := make(map[objectidentity.Key]bool)
	for _, table := range snapshot.GetTables() {
		captured, err := r.table(table)
		if err != nil {
			return nil, err
		}
		key := objectidentity.NewBuilder(r.session.info.IdentifierSemantics).TablePartsVerbatim(captured.Schema, captured.Name).Key()
		if seen[key] {
			return nil, fmt.Errorf("%w: duplicate normalized table identity", provider.ErrProtocol)
		}
		seen[key] = true
		captured.Facets, err = observedFacets(remaining[:len(table.GetFacets())], snapshot.GetEngine())
		if err != nil {
			return nil, err
		}
		remaining = remaining[len(table.GetFacets()):]
		for i, column := range table.GetColumns() {
			captured.Columns[i].Facets, err = observedFacets(remaining[:len(column.GetFacets())], snapshot.GetEngine())
			if err != nil {
				return nil, err
			}
			remaining = remaining[len(column.GetFacets()):]
		}
		result.Tables = append(result.Tables, captured)
	}
	if err := r.commonObjects(result, snapshot.GetCommonObjects()); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
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
			if _, err := r.commonNamespaceScopes(scope); err != nil {
				return err
			}
		}
	}
	return nil
}
