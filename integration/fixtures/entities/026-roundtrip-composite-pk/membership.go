package entities

//ptah:schema:table name="memberships" primary_key="org_id,user_id"
type Membership struct {
	//ptah:schema:field name="org_id" type="INTEGER" not_null="true"
	OrgID int64

	//ptah:schema:field name="user_id" type="INTEGER" not_null="true"
	UserID int64

	//ptah:schema:field name="role" type="TEXT" not_null="true"
	Role string
}
