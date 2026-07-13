package entities

//migrator:schema:view name="active_users" body="SELECT id, email FROM users WHERE deleted_at IS NULL" comment="Duplicate should be ignored by dedup"
type DuplicateViewHost struct{}
