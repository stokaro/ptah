// Package examplemigrations embeds a sample migrations directory demonstrating
// the Ptah migration file layout; migrator package examples run against this
// embedded filesystem.
//
// It is not named for its directory, because `migrator` is taken by
// migration/migrator and the one file importing both had to alias this one to
// say which it meant -- three different spellings across the tree
// (stokaro/ptah#2246 section 2.2).
package examplemigrations

import (
	"embed"
)

//go:embed migrations
var exampleMigrations embed.FS

// GetExampleMigrations returns the embedded example migrations filesystem
func GetExampleMigrations() embed.FS {
	return exampleMigrations
}
