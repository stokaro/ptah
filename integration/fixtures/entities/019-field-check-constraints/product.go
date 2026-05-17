package entities

import "time"

// 019-field-check-constraints exercises PR #123 / issue #112 on the natural
// fixture-driven migration path: a Product table with three field-level CHECK
// constraints. The framework migrates here from 000-initial — its Product
// table has `price` already, so the existing-column CHECK is synthesized into
// ALTER TABLE ADD CONSTRAINT, while the brand-new `stock` and `status`
// columns ship their CHECKs inline via ALTER TABLE ADD COLUMN.

//migrator:schema:table name="products"
type Product struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//migrator:schema:field name="name" type="VARCHAR(255)" not_null="true"
	Name string

	// `price > 0` exists in 000-initial without a CHECK; adding it here
	// should yield an ALTER TABLE ADD CONSTRAINT under the Postgres
	// auto-name `products_price_check`.
	//migrator:schema:field name="price" type="DECIMAL(10,2)" not_null="true" check="price > 0"
	Price float64

	// New column with a CHECK inlined into ALTER TABLE ADD COLUMN.
	//migrator:schema:field name="stock" type="INTEGER" not_null="true" default_expr="0" check="stock >= 0"
	Stock int

	// New column with an explicit `check_name=` so we can exercise the
	// "trust the name" matching during a later rename (in 020).
	//migrator:schema:field name="status" type="VARCHAR(32)" not_null="true" default="active" check="status IN ('active','draft','archived')" check_name="products_status_valid"
	Status string

	//migrator:schema:field name="created_at" type="TIMESTAMP" not_null="true" default_expr="CURRENT_TIMESTAMP"
	CreatedAt time.Time

	//migrator:schema:field name="updated_at" type="TIMESTAMP" not_null="true" default_expr="CURRENT_TIMESTAMP"
	UpdatedAt time.Time
}

//migrator:schema:index table="products" name="idx_products_name" columns="name"
