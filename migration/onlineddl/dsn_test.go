package onlineddl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/migration/onlineddl"
)

func TestParseDatabaseURL(t *testing.T) {
	tests := []struct {
		name    string
		url     string
		want    onlineddl.DSN
		wantErr string
	}{
		//nolint:gosec // fixture URL with a made-up password, not a credential
		{
			name: "plain mysql url",
			url:  "mysql://app:secret@db.internal:3307/shop",
			want: onlineddl.DSN{Host: "db.internal", Port: "3307", User: "app", Password: "secret", Database: "shop"},
		},
		{
			name: "tcp form",
			url:  "mysql://app:secret@tcp(127.0.0.1:3310)/shop",
			want: onlineddl.DSN{Host: "127.0.0.1", Port: "3310", User: "app", Password: "secret", Database: "shop"},
		},
		{
			// The @tcp() form is handed to go-sql-driver verbatim, which does
			// not percent-decode, so ptah connects with the literal value —
			// the tool must receive the same literal, not a decoded one.
			name: "tcp form is not percent-decoded",
			url:  "mysql://app:p%40ss@tcp(db:3306)/shop",
			want: onlineddl.DSN{Host: "db", Port: "3306", User: "app", Password: "p%40ss", Database: "shop"},
		},
		{
			name: "tcp form with query params",
			url:  "mariadb://root:pw@tcp(127.0.0.1:3307)/inventory?parseTime=true",
			want: onlineddl.DSN{Host: "127.0.0.1", Port: "3307", User: "root", Password: "pw", Database: "inventory"},
		},
		{
			name: "mariadb scheme with defaults",
			url:  "mariadb://root@localhost/inventory",
			want: onlineddl.DSN{Host: "localhost", Port: "3306", User: "root", Database: "inventory"},
		},
		//nolint:gosec // fixture URL with a made-up password, not a credential
		{
			name: "url-encoded password",
			url:  "mysql://app:p%40ss%2Fword@db:3306/shop",
			want: onlineddl.DSN{Host: "db", Port: "3306", User: "app", Password: "p@ss/word", Database: "shop"},
		},
		//nolint:gosec // fixture URL with a made-up password, not a credential
		{
			name:    "missing database name",
			url:     "mysql://app:secret@db:3306/",
			wantErr: ".*no database name.*",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			dsn, err := onlineddl.ParseDatabaseURL(tt.url)
			if tt.wantErr != "" {
				c.Assert(err, qt.ErrorMatches, tt.wantErr)
				return
			}
			c.Assert(err, qt.IsNil)
			c.Assert(dsn, qt.DeepEquals, tt.want)
		})
	}
}

func TestParseDatabaseURL_ErrorNeverEchoesCredentials(t *testing.T) {
	c := qt.New(t)

	_, err := onlineddl.ParseDatabaseURL("mysql://app:supersecret@db:3306/")
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Not(qt.Contains), "supersecret")
}
