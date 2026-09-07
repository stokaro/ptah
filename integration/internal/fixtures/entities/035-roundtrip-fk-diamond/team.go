package entities

//ptah:schema:table name="teams"
type Team struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//ptah:schema:field name="account_id" type="INTEGER" not_null="true" foreign="accounts(id)"
	AccountID int64
}
