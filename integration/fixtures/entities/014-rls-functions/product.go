package entities

// Enable RLS and create policies for products table with INSERT/UPDATE checks
//
//migrator:schema:rls:enable table="products" comment="Enable RLS for product isolation"
//migrator:schema:rls:policy name="product_tenant_isolation" table="products" for="ALL" to="PUBLIC" using="tenant_id = get_current_tenant_id()" with_check="tenant_id = get_current_tenant_id()" comment="Products isolated by tenant"
//migrator:schema:table name="products" comment="Product catalog table"
type Product struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64 `json:"id" db:"id"`

	//migrator:schema:field name="tenant_id" type="TEXT" not_null="true"
	TenantID string `json:"tenant_id" db:"tenant_id"`

	//migrator:schema:field name="name" type="VARCHAR(255)" not_null="true"
	Name string `json:"name" db:"name"`

	//migrator:schema:field name="description" type="TEXT"
	Description string `json:"description" db:"description"`

	//migrator:schema:field name="price" type="DECIMAL(10,2)" not_null="true"
	Price string `json:"price" db:"price"`

	//migrator:schema:field name="user_id" type="INTEGER" not_null="true"
	UserID int64 `json:"user_id" db:"user_id"`

	//migrator:schema:field name="created_at" type="TIMESTAMP" default_fn="NOW()"
	CreatedAt string `json:"created_at" db:"created_at"`
}

//migrator:schema:foreign_key table="products" name="fk_products_user_id" columns="user_id" ref_table="users" ref_columns="id" on_delete="CASCADE"
