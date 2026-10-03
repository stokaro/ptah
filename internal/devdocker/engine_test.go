package devdocker_test

import (
	"fmt"
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasurl"
	"ptah.run/internal/devdocker"
)

// The rows below were measured against the pinned community binary v1.3.0 on
// 2026-10-03 on a Linux host with a local daemon, reading the container it
// started with `docker inspect`; the package documentation carries the table
// (stokaro/ptah#4066).

func TestParsePostGISAndPgvector_HappyPath(t *testing.T) {
	tests := []struct {
		name            string
		rawURL          string
		wantImage       string
		wantDatabase    string
		wantCreates     bool
		wantReadyTarget string
	}{
		{
			// The binary starts postgis/postgis:<tag> and creates the database
			// itself, so the image cannot fill it with its extensions.
			name:            "postgis",
			rawURL:          "docker://postgis/16-3.4/dev",
			wantImage:       "postgis/postgis:16-3.4",
			wantDatabase:    "dev",
			wantCreates:     true,
			wantReadyTarget: "/postgres?sslmode=disable",
		},
		{
			// The image's own database is the one it filled; nothing is
			// created, and the claim judges what the image put there.
			name:            "postgis naming postgres",
			rawURL:          "docker://postgis/16-3.4/postgres",
			wantImage:       "postgis/postgis:16-3.4",
			wantDatabase:    "postgres",
			wantCreates:     false,
			wantReadyTarget: "/postgres?sslmode=disable",
		},
		{
			// The binary starts pgvector/pgvector:<tag> with POSTGRES_DB, as it
			// starts postgres:<tag>.
			name:            "pgvector",
			rawURL:          "docker://pgvector/pg16/dev",
			wantImage:       "pgvector/pgvector:pg16",
			wantDatabase:    "dev",
			wantCreates:     false,
			wantReadyTarget: "/dev?sslmode=disable",
		},
		{
			// Measured, `docker://postgis/dev` looks for postgis/postgis:dev.
			name:            "one segment is the tag",
			rawURL:          "docker://postgis/dev",
			wantImage:       "postgis/postgis:dev",
			wantDatabase:    "dev",
			wantCreates:     true,
			wantReadyTarget: "/postgres?sslmode=disable",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			spec, err := devdocker.Parse(test.rawURL)
			c.Assert(err, qt.IsNil)
			c.Check(spec.Dialect, qt.Equals, "postgres")
			c.Check(spec.Image, qt.Equals, test.wantImage)
			c.Check(spec.Database, qt.Equals, test.wantDatabase)
			c.Check(spec.CreatesDatabase(), qt.Equals, test.wantCreates)
			c.Check(spec.ReadyURL("127.0.0.1:15432", testPassword), qt.Matches,
				`postgres://postgres:`+testPassword+`@127\.0\.0\.1:15432`+regexp.QuoteMeta(test.wantReadyTarget))
		})
	}
}

// TestParseEnvOfPostGISAndPgvector pins the container environment: the
// binary passes POSTGRES_DB to pgvector and not to PostGIS, whose init would
// install its extensions and schemas in that database.
func TestParseEnvOfPostGISAndPgvector(t *testing.T) {
	tests := []struct {
		rawURL string
		want   []string
	}{
		{rawURL: "docker://postgis/16-3.4/dev", want: []string{"POSTGRES_PASSWORD=" + testPassword}},
		{rawURL: "docker://pgvector/pg16/dev", want: []string{"POSTGRES_PASSWORD=" + testPassword, "POSTGRES_DB=dev"}},
	}
	for _, test := range tests {
		t.Run(test.rawURL, func(t *testing.T) {
			c := qt.New(t)
			spec, err := devdocker.Parse(test.rawURL)
			c.Assert(err, qt.IsNil)
			c.Check(spec.Env(testPassword), qt.DeepEquals, test.want)
		})
	}
}

// TestParseRefusesAnEngineNotWrittenAsTheBinaryWritesIt pins the case of the
// engine. Measured, the pinned binary refuses `docker://POSTGRES/16-alpine/dev`,
// `docker://Postgres/16-alpine/dev` and `docker://MYSQL/8.4.11/dev` with
// `unsupported docker image`, as it refuses an engine it does not know.
func TestParseRefusesAnEngineNotWrittenAsTheBinaryWritesIt(t *testing.T) {
	tests := []struct {
		rawURL  string
		wantErr string
	}{
		{rawURL: "docker://POSTGRES/16-alpine/dev", wantErr: `unsupported docker image "POSTGRES"`},
		{rawURL: "docker://Postgres/16-alpine/dev", wantErr: `unsupported docker image "Postgres"`},
		{rawURL: "docker://MYSQL/8.4.11/dev", wantErr: `unsupported docker image "MYSQL"`},
		{rawURL: "docker://POSTGIS/16-3.4/dev", wantErr: `unsupported docker image "POSTGIS"`},
		{rawURL: "docker://clickhouse/24.8/dev", wantErr: `unsupported docker image "clickhouse"`},
		{rawURL: "docker://sqlserver/2022-latest/dev", wantErr: `unsupported docker image "sqlserver"`},
	}
	for _, test := range tests {
		t.Run(test.rawURL, func(t *testing.T) {
			c := qt.New(t)
			spec, err := devdocker.Parse(test.rawURL)
			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(test.wantErr))
			c.Check(spec.Image, qt.Equals, "")
		})
	}
}

// TestParseAndDialectFromURLAgreeOnEveryEngine joins the two places an engine
// is recognized: atlasurl answers the dialect before anything is started, and
// devdocker starts the container. An engine one of them knows and the other
// does not would pin a dialect for a URL the provisioner refuses, or provision
// one the dialect preflight refused first. Every engine the pinned binary
// starts is a row, and so are values both must refuse.
func TestParseAndDialectFromURLAgreeOnEveryEngine(t *testing.T) {
	tests := []string{
		"docker://postgres/16/dev",
		"docker://postgis/16-3.4/dev",
		"docker://pgvector/pg16/dev",
		"docker://mysql/8.4/dev",
		"docker://maria/11.8/dev",
		"docker://mariadb/11.8/dev",
		"docker://sqlite/dev",
		"docker://clickhouse/24.8/dev",
		"docker://sqlserver/2022-latest/dev",
		"docker://POSTGRES/16/dev",
		"docker://postgres:16/dev",
		"docker:///dev",
	}
	for _, rawURL := range tests {
		t.Run(rawURL, func(t *testing.T) {
			c := qt.New(t)
			spec, parseErr := devdocker.Parse(rawURL)
			dialect, dialectErr := atlasurl.DialectFromURL(rawURL)
			c.Check(spec.Dialect, qt.Equals, dialect)
			// fmt.Sprint renders nil as `<nil>` on both sides.
			c.Check(fmt.Sprint(parseErr), qt.Equals, fmt.Sprint(dialectErr))
		})
	}
}
