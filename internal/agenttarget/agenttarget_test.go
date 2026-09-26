package agenttarget_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/agentpolicy"
	"ptah.run/internal/agenttarget"
)

// TestDefaultName names a target after the database its URL selects. A
// MySQL-family URL is read by the parser the connection uses: in a +unix URL
// the path is the socket, and read from the path the target would be named
// run/mysqld/mysqld.sock.
func TestDefaultName(t *testing.T) {
	tests := []struct {
		name string
		url  string
		want string
	}{
		{name: "PostgreSQL", url: "postgres://app@db.internal/shop", want: "shop"},
		{name: "MySQL URL form", url: "mysql://app@db.internal:3306/shop", want: "shop"},
		{name: "MySQL driver form", url: "mariadb://app:secret@tcp(db.internal:3306)/shop", want: "shop"},
		{name: "socket URL", url: "mysql+unix://app:secret@/run/mysqld/mysqld.sock?database=shop", want: "shop"},
		{name: "socket URL naming no database", url: "maria+unix://app@/run/mysqld/mysqld.sock", want: "database"},
		{name: "PostgreSQL URL naming no database", url: "postgres://app@db.internal", want: "database"},
		{name: "a URL that cannot be read", url: "postgres://app@db.internal:notaport/shop", want: "database"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(agenttarget.DefaultName(test.url), qt.Equals, test.want)
		})
	}
}

// TestTargetDisplay_WithholdsTheCredential renders what an approval prompt
// shows. A socket URL shows its database, which is the `database` parameter;
// without it the prompt would name a socket and no database.
func TestTargetDisplay_WithholdsTheCredential(t *testing.T) {
	tests := []struct {
		name string
		url  string
		want string
	}{
		// #nosec G101 -- fixture URL with a made-up password, not a credential
		{
			name: "PostgreSQL",
			url:  "postgres://app:secret@db.internal/shop?sslmode=disable",
			want: "postgres://app@db.internal/shop",
		},
		// #nosec G101 -- fixture URL with a made-up password, not a credential
		{
			name: "MySQL URL form",
			url:  "mysql://app:secret@db.internal:3307/shop?parseTime=true",
			want: "mysql://app@db.internal:3307/shop",
		},
		{
			name: "MySQL driver form",
			url:  "mariadb://app:secret@tcp(db.internal:3307)/shop",
			want: "mariadb://app@db.internal:3307/shop",
		},
		{
			name: "socket URL",
			url:  "mysql+unix://app:secret@/run/mysqld/mysqld.sock?database=shop&parseTime=true",
			want: "mysql+unix://app@/run/mysqld/mysqld.sock?database=shop",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			target, err := agenttarget.New(agenttarget.Config{Name: "live", URL: test.url, Class: agentpolicy.ClassDev})

			c.Assert(err, qt.IsNil)
			c.Assert(target.Display(), qt.Equals, test.want)
		})
	}
}
