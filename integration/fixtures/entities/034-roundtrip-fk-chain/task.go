package entities

//ptah:schema:table name="tasks"
type Task struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//ptah:schema:field name="project_id" type="INTEGER" not_null="true" foreign="projects(id)"
	ProjectID int64

	//ptah:schema:field name="title" type="VARCHAR(255)" not_null="true"
	Title string
}
