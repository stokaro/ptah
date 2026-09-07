package entities

//ptah:schema:table name="right_nodes"
type RightNode struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//ptah:schema:field name="left_id" type="INTEGER" not_null="true" foreign="left_nodes(id)"
	LeftID int64
}
