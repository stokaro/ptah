package providerhost

import (
	"context"
	"fmt"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"

	"ptah.run/catalog"
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
	snapshot, err := r.session.target.host.client.Inspect(ctx, &protocol.InspectRequest{SessionId: r.session.remote.GetId(), Schemas: r.schemas})
	if err != nil {
		return nil, err
	}
	if len(snapshot.GetNativeObjects()) != 0 || len(snapshot.GetDispositions()) != 0 {
		return nil, unsupported("native objects or inspection dispositions")
	}
	result := &catalog.Database{}
	for _, table := range snapshot.GetTables() {
		captured, err := r.table(table)
		if err != nil {
			return nil, err
		}
		result.Tables = append(result.Tables, captured)
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
	if len(value.GetFacets()) != 0 {
		return catalog.Table{}, unsupported("table facets")
	}
	result := catalog.Table{Name: ref.GetName(), Schema: ref.GetSchema(), Type: "TABLE", RowStatsUnknown: true}
	for i, column := range value.GetColumns() {
		if len(column.GetFacets()) != 0 {
			return catalog.Table{}, unsupported("column facets")
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
