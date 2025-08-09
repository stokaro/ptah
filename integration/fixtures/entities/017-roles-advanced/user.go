package entities

// Define PostgreSQL roles with advanced configurations
//
//migrator:schema:role name="app_user" login="true" comment="Application user role for regular users"
//migrator:schema:role name="admin_user" login="true" superuser="true" comment="Administrator role with full privileges"
//migrator:schema:role name="readonly_user" login="true" comment="Read-only user role for reporting"
//migrator:schema:role name="service_user" login="true" createdb="true" comment="Service user role for automated processes"
//migrator:schema:role name="backup_user" login="true" replication="true" comment="Backup user role for replication"
//migrator:schema:role name="api_user" login="true" inherit="false" comment="API user role with restricted inheritance"
//migrator:schema:table name="users" comment="User accounts table"
type User struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64 `json:"id" db:"id"`

	//migrator:schema:field name="email" type="VARCHAR(255)" not_null="true" unique="true"
	Email string `json:"email" db:"email"`

	//migrator:schema:field name="name" type="VARCHAR(255)" not_null="true"
	Name string `json:"name" db:"name"`

	//migrator:schema:field name="role_type" type="VARCHAR(50)" default="'app_user'"
	RoleType string `json:"role_type" db:"role_type"`

	//migrator:schema:field name="created_at" type="TIMESTAMP" default_fn="NOW()"
	CreatedAt string `json:"created_at" db:"created_at"`
}
