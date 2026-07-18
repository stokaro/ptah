package entities

//migrator:schema:table name="teams"
type Team struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//migrator:schema:field name="account_id" type="INTEGER" not_null="true" foreign="accounts(id)"
	AccountID int64
}
