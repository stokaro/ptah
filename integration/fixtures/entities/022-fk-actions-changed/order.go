package entities

// Same schema as 021-fk-actions-initial except for the referential actions on
// fk_orders_user: on_delete gains SET NULL and on_update gains CASCADE. The
// constraint name is unchanged, so the comparator reports a modification
// (remove + add of the same name) and the generated UP must drop-and-re-add
// the FK with the new actions; the generated DOWN must restore the prior
// (default) actions from the introspected pre-change schema (PR #190).

//ptah:schema:table name="orders"
type Order struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//ptah:schema:field name="user_id" type="INTEGER" foreign="users(id)" foreign_key_name="fk_orders_user" on_delete="SET NULL" on_update="CASCADE"
	UserID *int64

	//ptah:schema:field name="note" type="VARCHAR(255)"
	Note string
}
