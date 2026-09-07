package entities

//ptah:schema:table name="left_nodes"
type LeftNode struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//ptah:schema:field name="right_id" type="INTEGER" not_null="true" foreign="right_nodes(id)"
	RightID int64
}
