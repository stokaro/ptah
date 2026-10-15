// Package pgdocker declares the PostgreSQL images internal/devdocker starts
// from a docker URL: the official image and the pgvector and PostGIS images
// built on it.
package pgdocker

import (
	"fmt"
	"net/url"

	"ptah.run/internal/devdocker/dockerengine"
)

// Engines returns the PostgreSQL engines, each under the name a docker URL
// gives it. Each call returns new values.
//
// The names are the spellings the pinned community binary accepts, measured
// one at a time. Only `postgres` has an atlas.hcl docker block.
func Engines() []dockerengine.Engine {
	return []dockerengine.Engine{
		{
			Name:            "postgres",
			Image:           "postgres",
			Port:            "5432",
			Params:          params(),
			Env:             env,
			URL:             connectURL,
			ServerDatabase:  "postgres",
			BlockAttributes: []string{"image", "schema", "database", "baseline", "env", "timeout"},
		},
		// The pgvector project's image is the official one with the extension
		// installed and not created. Measured, the pinned binary starts
		// `pgvector/pgvector:<tag>` with POSTGRES_PASSWORD and POSTGRES_DB, as it
		// starts `postgres:<tag>`.
		{
			Name:           "pgvector",
			Image:          "pgvector/pgvector",
			Port:           "5432",
			Params:         params(),
			Env:            env,
			URL:            connectURL,
			ServerDatabase: "postgres",
		},
		// The PostGIS image creates postgis, postgis_topology, fuzzystrmatch and
		// postgis_tiger_geocoder, and the tiger and topology schemas, in the
		// database POSTGRES_DB names. Measured, the pinned binary starts
		// `postgis/postgis:<tag>` with POSTGRES_PASSWORD alone and creates the
		// URL's database itself, which leaves it empty: `CREATE EXTENSION postgis`
		// succeeds there.
		{
			Name:   "postgis",
			Image:  "postgis/postgis",
			Port:   "5432",
			Params: params(),
			Env: func(_, password string) []string {
				return []string{"POSTGRES_PASSWORD=" + password}
			},
			URL:             connectURL,
			ServerDatabase:  "postgres",
			CreatesDatabase: true,
		},
	}
}

// env is the container environment of an image built on the official
// PostgreSQL one.
//
// POSTGRES_USER is the image's. The official image's default is postgres, and
// an image that names another user runs its init scripts as that user: the
// Supabase image declares supabase_admin, and with the variable overridden its
// init fails and the container exits (stokaro/ptah#4053). The pinned binary
// passes these two variables and no user, measured on 2026-10-03.
func env(database, password string) []string {
	return []string{
		"POSTGRES_PASSWORD=" + password,
		"POSTGRES_DB=" + database,
	}
}

// connectURL is the connectable URL of a server an image built on the
// official PostgreSQL one runs.
func connectURL(hostPort, database, password, query string) string {
	return fmt.Sprintf("postgres://postgres:%s@%s/%s%s", password, hostPort, url.PathEscape(database), dockerengine.QuerySuffix(query))
}

// params are the connection parameters of such a server. The container
// publishes on loopback and speaks no TLS, so the default disables it. An
// operator who writes `sslmode` on the docker URL replaces this value rather
// than being silently overruled.
func params() map[string]string {
	return map[string]string{"sslmode": "disable"}
}
