// Package postgres reads what a URL of a PostgreSQL-protocol engine selects:
// PostgreSQL, CockroachDB, YugabyteDB and Spanner, whose URLs pgx's pgconn
// reads.
package postgres

import (
	"cmp"
	"errors"
	"net/url"
	"strconv"

	"github.com/jackc/pgx/v5/pgconn"

	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/dburlext"
)

// Services returns the services of every dialect whose URL pgconn reads.
func Services() dburlext.Services {
	return dburlext.Services{
		Dialects:  []string{platform.Postgres, platform.CockroachDB, platform.YugabyteDB, platform.Spanner},
		Endpoints: endpoints{},
	}
}

type endpoints struct{}

// Select reads the URL as pgconn reads it, so a fallback host the driver may
// reach is one of the URL's servers. The scheme is the dialect's wire scheme,
// or its own name: pgconn reads postgres:// and postgresql:// alone, so a
// pgx:// or cockroachdb:// URL is read under the name the driver knows.
func (endpoints) Select(parsed *url.URL, dialect descriptor.Dialect) (dburlext.Selection, error) {
	connectionURL := *parsed
	connectionURL.Scheme = cmp.Or(dialect.WireScheme, dialect.Name)
	config, err := pgconn.ParseConfig(connectionURL.String())
	if err != nil {
		return dburlext.Selection{}, errors.New("invalid PostgreSQL database URL")
	}
	selected := dburlext.Selection{
		Endpoints: []dburlext.Endpoint{{Host: config.Host, Port: strconv.Itoa(int(config.Port))}},
		Database:  config.Database,
	}
	for _, fallback := range config.Fallbacks {
		selected.Endpoints = append(selected.Endpoints, dburlext.Endpoint{Host: fallback.Host, Port: strconv.Itoa(int(fallback.Port))})
	}
	return selected, nil
}
