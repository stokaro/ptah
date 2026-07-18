package entities

//migrator:schema:table name="left_nodes"
type LeftNode struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//migrator:schema:field name="right_id" type="INTEGER" not_null="true" foreign="right_nodes(id)"
	RightID int64
}
