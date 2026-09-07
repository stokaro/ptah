package entities

//ptah:schema:table name="assignments"
type Assignment struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//ptah:schema:field name="team_id" type="INTEGER" not_null="true" foreign="teams(id)"
	TeamID int64

	//ptah:schema:field name="project_id" type="INTEGER" not_null="true" foreign="projects(id)"
	ProjectID int64
}
