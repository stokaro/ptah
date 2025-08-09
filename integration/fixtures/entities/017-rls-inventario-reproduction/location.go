package entities

import (
	"context"
)

var (
	_ any = (*Location)(nil)
)

// Enable RLS for multi-tenant isolation
//migrator:schema:rls:enable table="locations" comment="Enable RLS for multi-tenant location isolation"
//migrator:schema:rls:policy name="location_tenant_isolation" table="locations" for="ALL" to="inventario_app" using="tenant_id = get_current_tenant_id()" comment="Ensures locations can only be accessed by their tenant"

//migrator:schema:table name="locations"
type Location struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64 `json:"id" db:"id"`

	//migrator:schema:field name="tenant_id" type="TEXT" not_null="true"
	TenantID string `json:"tenant_id" db:"tenant_id"`

	//migrator:schema:field name="name" type="TEXT" not_null="true"
	Name string `json:"name" db:"name"`
}

func (*Location) Validate() error {
	return nil
}

func (l *Location) ValidateWithContext(ctx context.Context) error {
	return nil
}
