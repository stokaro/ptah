// Package entities is integration fixture 054-ydb-resource-pools: declares a
// YDB resource pool and a classifier that sends a group's queries to it,
// through Go annotations. It is a fixture of its own rather than part of
// 023-go-annotations-objects because every target but YDB refuses a resource
// pool, and fixture 023 is rendered for PostgreSQL.
package entities
