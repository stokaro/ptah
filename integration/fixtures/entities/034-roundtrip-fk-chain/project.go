package entities

//ptah:schema:table name="projects"
type Project struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//ptah:schema:field name="organization_id" type="INTEGER" not_null="true" foreign="organizations(id)"
	OrganizationID int64

	//ptah:schema:field name="name" type="VARCHAR(255)" not_null="true"
	Name string
}
