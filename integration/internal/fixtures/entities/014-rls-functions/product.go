package entities

// Enable RLS and create policies for products table with INSERT/UPDATE checks
//
//ptah:schema:rls:enable table="products" comment="Enable RLS for product isolation"
//ptah:schema:rls:policy name="product_tenant_isolation" table="products" for="ALL" to="PUBLIC" using="tenant_id = get_current_tenant_id()" with_check="tenant_id = get_current_tenant_id()" comment="Products isolated by tenant"
//ptah:schema:table name="products" comment="Product catalog table"
type Product struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64 `json:"id" db:"id"`

	//ptah:schema:field name="tenant_id" type="TEXT" not_null="true"
	TenantID string `json:"tenant_id" db:"tenant_id"`

	//ptah:schema:field name="name" type="VARCHAR(255)" not_null="true"
	Name string `json:"name" db:"name"`

	//ptah:schema:field name="description" type="TEXT"
	Description string `json:"description" db:"description"`

	//ptah:schema:field name="price" type="DECIMAL(10,2)" not_null="true"
	Price string `json:"price" db:"price"`

	//ptah:schema:field name="user_id" type="INTEGER" not_null="true"
	UserID int64 `json:"user_id" db:"user_id"`

	//ptah:schema:field name="created_at" type="TIMESTAMP" default_expr="NOW()"
	CreatedAt string `json:"created_at" db:"created_at"`
}

//ptah:schema:foreign_key table="products" name="fk_products_user_id" columns="user_id" ref_table="users" ref_columns="id" on_delete="CASCADE"
