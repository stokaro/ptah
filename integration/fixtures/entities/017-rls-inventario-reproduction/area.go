package entities

import (
	"context"
)

var (
	_ any = (*Area)(nil)
)

// Enable RLS for multi-tenant isolation
//
//migrator:schema:rls:enable table="areas" comment="Enable RLS for multi-tenant area isolation"
//migrator:schema:rls:policy name="area_tenant_isolation" table="areas" for="ALL" to="inventario_app" using="tenant_id = get_current_tenant_id()" comment="Ensures areas can only be accessed by their tenant"
//migrator:schema:table name="areas"
type Area struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64 `json:"id" db:"id"`

	//migrator:schema:field name="tenant_id" type="TEXT" not_null="true"
	TenantID string `json:"tenant_id" db:"tenant_id"`

	//migrator:schema:field name="name" type="TEXT" not_null="true"
	Name string `json:"name" db:"name"`

	//migrator:schema:field name="location_id" type="TEXT" not_null="true"
	LocationID string `json:"location_id" db:"location_id"`
}

func (*Area) Validate() error {
	return nil
}

func (a *Area) ValidateWithContext(ctx context.Context) error {
	return nil
}
