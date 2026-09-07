//go:build !js

package sqlite

import (
	_ "modernc.org/sqlite" // SQLite database/sql driver
)
