// Package entities is integration fixture 050-ydb-replication: declares a YDB
// async replication with its items, and a transfer from a changefeed's topic
// into a table, through Go annotations. It is a fixture of its own rather than
// part of 023-go-annotations-objects because every target but YDB refuses a
// replication and a transfer, and fixture 023 is rendered for PostgreSQL.
package entities
