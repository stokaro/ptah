package entities

// Order carries a FIELD-LEVEL foreign key (foreign= on the column annotation,
// the issue #189 / PR #190 feature) with NO referential actions: the engine
// default is NO ACTION (reported as RESTRICT by MariaDB — the two are
// equivalent on the MySQL family). The 022-fk-actions-changed version of this
// fixture changes on_delete/on_update on the SAME constraint name, which the
// comparator expresses as a modification (drop + re-add).
//
// user_id is deliberately nullable: the changed version sets
// on_delete="SET NULL", which requires a nullable referencing column.

//migrator:schema:table name="orders"
type Order struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//migrator:schema:field name="user_id" type="INTEGER" foreign="users(id)" foreign_key_name="fk_orders_user"
	UserID *int64

	//migrator:schema:field name="note" type="VARCHAR(255)"
	Note string
}
