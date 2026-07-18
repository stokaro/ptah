package entities

//migrator:schema:table name="right_nodes"
type RightNode struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//migrator:schema:field name="left_id" type="INTEGER" not_null="true" foreign="left_nodes(id)"
	LeftID int64
}
