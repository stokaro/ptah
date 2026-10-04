// Package entities is integration fixture 053-ydb-external-sources: declares
// YDB external data sources, one naming its password by a secret's path, and
// an external table over files in object storage. It is a fixture of its own
// rather than part of 023-go-annotations-objects because every target but YDB
// refuses both objects, and fixture 023 is rendered for PostgreSQL.
package entities
