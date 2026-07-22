package atlasurl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/atlasurl"
)

func TestDialectFromURL_HappyPath(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name   string
		rawURL string
		want   string
	}{
		{name: "empty", rawURL: "", want: ""},
		{name: "postgres", rawURL: "postgres://localhost/dev", want: "postgres"},
		{name: "postgresql alias", rawURL: "postgresql://localhost/dev", want: "postgres"},
		{name: "sqlserver", rawURL: "sqlserver://localhost/dev", want: "sqlserver"},
		{name: "docker postgres", rawURL: "docker://postgres/16/dev", want: "postgres"},
		{name: "docker postgres port", rawURL: "docker://postgres:16/dev", want: "postgres"},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			got, err := atlasurl.DialectFromURL(test.rawURL)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

func TestDialectFromURL_FailurePath(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name    string
		rawURL  string
		wantErr string
	}{
		{name: "missing docker engine", rawURL: "docker:///dev", wantErr: `docker --dev-url is missing database engine`},
		{name: "unsupported", rawURL: "spanner://localhost/dev", wantErr: `unsupported --dev-url dialect "spanner://localhost/dev"`},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			got, err := atlasurl.DialectFromURL(test.rawURL)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got, qt.Equals, "")
		})
	}
}

func TestValidateDialectMatch_HappyPath(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name          string
		rawURL        string
		targetDialect string
	}{
		{name: "empty dev url", rawURL: "", targetDialect: "postgres"},
		{name: "exact dialect", rawURL: "mysql://localhost/dev", targetDialect: "mysql"},
		{name: "target alias", rawURL: "postgres://localhost/dev", targetDialect: "postgresql"},
		{name: "docker dialect", rawURL: "docker://mariadb/11/dev", targetDialect: "mariadb"},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			err := atlasurl.ValidateDialectMatch(test.rawURL, test.targetDialect)
			c.Assert(err, qt.IsNil)
		})
	}
}

func TestValidateDialectMatch_FailurePath(t *testing.T) {
	c := qt.New(t)

	c.Run("unsupported dev url", func(c *qt.C) {
		err := atlasurl.ValidateDialectMatch("spanner://localhost/dev", "postgres")
		c.Assert(err, qt.ErrorMatches, `unsupported --dev-url dialect "spanner://localhost/dev"`)
	})

	c.Run("mismatched dialect", func(c *qt.C) {
		err := atlasurl.ValidateDialectMatch("mysql://localhost/dev", "postgres")
		c.Assert(err, qt.ErrorMatches, `--dev-url dialect "mysql" does not match --url dialect "postgres"`)
	})
}
