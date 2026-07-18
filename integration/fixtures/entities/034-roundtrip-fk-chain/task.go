package entities

//migrator:schema:table name="tasks"
type Task struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//migrator:schema:field name="project_id" type="INTEGER" not_null="true" foreign="projects(id)"
	ProjectID int64

	//migrator:schema:field name="title" type="VARCHAR(255)" not_null="true"
	Title string
}
