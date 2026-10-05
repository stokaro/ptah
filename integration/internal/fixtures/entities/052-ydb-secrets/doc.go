// Package entities is integration fixture 052-ydb-secrets: declares YDB secrets
// through Go annotations, each naming the environment variable its value comes
// from. It is a fixture of its own rather than part of
// 023-go-annotations-objects because every target but YDB refuses a secret, and
// fixture 023 is rendered for PostgreSQL.
package entities
