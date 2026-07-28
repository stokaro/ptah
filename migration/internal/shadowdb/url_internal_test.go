package shadowdb

// White-box testing required: sqliteDatabaseURL normalizes OS-native temporary
// paths before they reach the public Open lifecycle, and a Windows path cannot
// be produced by os.MkdirTemp when this test runs on another operating system.

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestSQLiteDatabaseURL(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name string
		path string
		want string
	}{
		{
			name: "Unix absolute path",
			path: "/tmp/ptah tests/shadow.db",
			want: "sqlite:///tmp/ptah%20tests/shadow.db",
		},
		{
			name: "Windows absolute path",
			path: `C:\Users\runner\AppData\Local\Temp\ptah tests\shadow.db`,
			want: "sqlite://C:/Users/runner/AppData/Local/Temp/ptah%20tests/shadow.db",
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			c.Assert(sqliteDatabaseURL(tt.path), qt.Equals, tt.want)
		})
	}
}
