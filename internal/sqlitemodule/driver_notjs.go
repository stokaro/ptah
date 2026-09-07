//go:build !js

package sqlitemodule

import (
	_ "modernc.org/sqlite" // SQLite database/sql driver, for Registered
)
