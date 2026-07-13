package entities

//migrator:schema:table name="users"
//migrator:schema:constraint name="users_email_check" type="CHECK" check="email <> ''" comment="Email must not be empty"
type User struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//migrator:schema:field name="email" type="VARCHAR(255)" not_null="true"
	Email string

	//migrator:schema:field name="name" type="VARCHAR(255)"
	Name string

	//migrator:schema:field name="deleted_at" type="TIMESTAMP"
	DeletedAt *string

	//migrator:schema:field name="updated_at" type="TIMESTAMP" default_expr="NOW()"
	UpdatedAt *string
}

//migrator:schema:role name="app_user" login="false" inherit="true" comment="App role for grants demo"
type AppRoleMarker struct{}

//migrator:schema:view name="active_users" body="SELECT id, email FROM users WHERE deleted_at IS NULL" with_check="false" comment="Active users view"
type ActiveUsersView struct{}

//migrator:schema:matview name="user_stats" body="SELECT COUNT(*) as cnt FROM users" refresh_strategy="manual" comment="User count matview"
type UserStatsMatView struct{}

//migrator:schema:trigger name="users_set_updated_at" table="users" timing="BEFORE" event="UPDATE" for="ROW" body="NEW.updated_at = NOW(); RETURN NEW;" comment="Auto update"
type UserTrigger struct{}

//migrator:schema:grant role="app_user" privilege="SELECT,INSERT,UPDATE,DELETE" on_table="users" comment="DML grants to app_user"
//migrator:schema:grant role="app_user" privilege="USAGE" on_schema="public" comment="Schema usage"
type AccessControlMarker struct{}
