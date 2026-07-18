package entities

//migrator:schema:table name="memberships"
type Membership struct {
	//migrator:schema:field name="org_id" type="INTEGER" not_null="true"
	OrgID int64

	//migrator:schema:field name="user_id" type="INTEGER" not_null="true"
	UserID int64

	//migrator:schema:field name="role" type="TEXT" not_null="true"
	Role string
}
