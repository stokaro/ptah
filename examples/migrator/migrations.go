// Package migrator embeds a sample migrations directory demonstrating the
// Ptah migration file layout; migrator package examples run against this
// embedded filesystem.
package migrator

import (
	"embed"
)

//go:embed migrations
var exampleMigrations embed.FS

// GetExampleMigrations returns the embedded example migrations filesystem
func GetExampleMigrations() embed.FS {
	return exampleMigrations
}
