package entities

// Enable RLS for multi-tenant isolation
//
//ptah:schema:rls:enable table="commodities" comment="Enable RLS for multi-tenant commodity isolation"
//ptah:schema:rls:policy name="commodity_tenant_isolation" table="commodities" for="ALL" to="inventario_app" using="tenant_id = get_current_tenant_id()" with_check="tenant_id = get_current_tenant_id()" comment="Ensures commodities can only be accessed and modified by their tenant"
//ptah:schema:table name="commodities"
type Commodity struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64 `json:"id" db:"id"`

	//ptah:schema:field name="tenant_id" type="TEXT" not_null="true"
	TenantID string `json:"tenant_id" db:"tenant_id"`

	//ptah:schema:field name="name" type="TEXT" not_null="true"
	Name string `json:"name" db:"name"`

	//ptah:schema:field name="area_id" type="TEXT" not_null="true"
	AreaID string `json:"area_id" db:"area_id"`
}
