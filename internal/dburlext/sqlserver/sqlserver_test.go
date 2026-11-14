package sqlserver_test

import (
	"net/url"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"
	"github.com/microsoft/go-mssqldb/msdsn"

	"ptah.run/core/platform"
	"ptah.run/internal/dburlext"
	"ptah.run/internal/dburlext/sqlserver"
)

func TestRename_SelectsDatabaseWithoutChangingInstanceOrOtherParameters(t *testing.T) {
	for _, raw := range []string{
		"sqlserver://sa:secret@db.example?database=master&app+name=one+two",
		"sqlserver://sa:secret@db.example/TESTINSTANCE?Database=master&app+name=one+two",
		"sqlserver://sa:secret@db.example/TESTINSTANCE?app+name=one+two",
	} {
		t.Run(raw, func(t *testing.T) {
			c := qt.New(t)
			parsed := must.Must(url.Parse(raw))
			got, err := sqlserver.Services().Rename.Rename(parsed, "scratch")
			c.Assert(err, qt.IsNil)
			original, err := msdsn.Parse(raw)
			c.Assert(err, qt.IsNil)
			config, err := msdsn.Parse(got)
			c.Assert(err, qt.IsNil)
			c.Assert(config.Database, qt.Equals, "scratch")
			c.Assert(config.Instance, qt.Equals, original.Instance)
			c.Assert(must.Must(url.Parse(got)).Path, qt.Equals, parsed.Path)
			c.Assert(must.Must(url.Parse(got)).Query().Get("app name"), qt.Equals, "one two")
			c.Assert(got, qt.Contains, "app+name=one+two")
		})
	}
}

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
