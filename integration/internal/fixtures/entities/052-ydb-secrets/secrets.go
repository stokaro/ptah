package entities

// Credentials holds the secrets an external data source reads its password
// from: one at the database root and one in a directory.
//
//ptah:schema:secret name="pg_password" value_env="PTAH_SECRET_PG_PASSWORD"
//ptah:schema:secret name="s3_key" schema="ext" value_env="PTAH_SECRET_S3_KEY"
type Credentials struct{}
