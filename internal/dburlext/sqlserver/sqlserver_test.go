package sqlserver_test

import (
	"net/url"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/internal/dburlext"
	"ptah.run/internal/dburlext/sqlserver"
)

func TestSelect_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		rawURL string
		want   dburlext.Selection
	}{
		{
			name:   "no port, which neither the URL nor msdsn names",
			rawURL: "sqlserver://sa:secret@db.example?database=app",
			want:   dburlext.Selection{Endpoints: []dburlext.Endpoint{{Host: "db.example"}}, Database: "app"},
		},
		{
			name:   "a named instance, folded to lower case",
			rawURL: "mssql://sa@db.example:1500/Reporting?database=app",
			want: dburlext.Selection{
				Endpoints: []dburlext.Endpoint{{Host: "db.example", Port: "1500", Instance: "reporting"}}, Database: "app",
			},
		},
		{
			name:   "a failover partner",
			rawURL: "sqlserver://sa@primary:1433?database=app&failoverpartner=mirror&failoverport=1544",
			want: dburlext.Selection{
				Endpoints: []dburlext.Endpoint{{Host: "primary", Port: "1433"}, {Host: "mirror", Port: "1544"}}, Database: "app",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			described, found := platform.Lookup(platform.SQLServer)
			c.Assert(found, qt.IsTrue)

			got, err := sqlserver.Services().Endpoints.Select(must.Must(url.Parse(test.rawURL)), described)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

func TestSelect_FailurePath(t *testing.T) {
	c := qt.New(t)
	described, found := platform.Lookup(platform.SQLServer)
	c.Assert(found, qt.IsTrue)

	got, err := sqlserver.Services().Endpoints.Select(must.Must(url.Parse("sqlserver://sa:secret@h?connection+timeout=soon")), described)

	c.Assert(err, qt.ErrorMatches, `invalid SQL Server database URL`)
	c.Assert(got, qt.DeepEquals, dburlext.Selection{})
}
