package entities

import "time"

// 020-field-check-constraints-modified evolves 019 to exercise the three
// non-trivial CHECK lifecycle paths:
//
//   - price: rename the constraint by introducing an explicit `check_name=`.
//     The comparator "trusts the name" (Postgres rewrites the stored
//     expression, so text compare is hostile to idempotency), so a rename is
//     what triggers a drop+add for a same-column same-expression CHECK.
//   - stock: drop the CHECK entirely.
//   - status: keep `check_name=` and expression identical, drop one allowed
//     value from the IN list — this case is what PR #112's "trust the name"
//     contract deliberately ignores; a same-name CHECK is treated as
//     unchanged. We assert below that no needless drop+add fires.

//migrator:schema:table name="products"
type Product struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//migrator:schema:field name="name" type="VARCHAR(255)" not_null="true"
	Name string

	// price: was auto-named `products_price_check` in 019; now explicitly
	// named `products_price_positive`. The diff should produce one
	// ConstraintsRemoved (old auto-name) + one ConstraintsAdded (new name).
	//migrator:schema:field name="price" type="DECIMAL(10,2)" not_null="true" check="price > 0" check_name="products_price_positive"
	Price float64

	// stock: CHECK annotation dropped. Diff should produce one
	// ConstraintsRemoved targeting `products_stock_check`.
	//migrator:schema:field name="stock" type="INTEGER" not_null="true" default_expr="0"
	Stock int

	// status: same `check_name=` as 019, but the expression is narrowed.
	// Because the comparator trusts the name, the diff should NOT emit a
	// drop+add — the constraint is treated as unchanged. This guards against
	// a future regression where someone reintroduces expression-text
	// comparison and accidentally regens every CHECK on every run.
	//migrator:schema:field name="status" type="VARCHAR(32)" not_null="true" default="active" check="status IN ('active','archived')" check_name="products_status_valid"
	Status string

	//migrator:schema:field name="created_at" type="TIMESTAMP" not_null="true" default_expr="CURRENT_TIMESTAMP"
	CreatedAt time.Time

	//migrator:schema:field name="updated_at" type="TIMESTAMP" not_null="true" default_expr="CURRENT_TIMESTAMP"
	UpdatedAt time.Time
}

//migrator:schema:index table="products" name="idx_products_name" columns="name"
