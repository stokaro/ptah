// Package clickhouse reads what a ClickHouse URL selects, as clickhouse-go
// reads it: every address the URL lists is a server it may reach.
package clickhouse

import (
	"errors"
	"net"
	"net/url"

	chdriver "github.com/ClickHouse/clickhouse-go/v2"

	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/dburlext"
)

// Services returns ClickHouse's services.
func Services() dburlext.Services {
	return dburlext.Services{Dialects: []string{platform.ClickHouse}, Endpoints: endpoints{}}
}

type endpoints struct{}

// Select reads the URL under the dialect's name, the scheme clickhouse-go
// reads. A URL that lists no address selects the host and port it names.
func (endpoints) Select(parsed *url.URL, dialect descriptor.Dialect) (dburlext.Selection, error) {
	connectionURL := *parsed
	connectionURL.Scheme = dialect.Name
	options, err := chdriver.ParseDSN(connectionURL.String())
	if err != nil {
		return dburlext.Selection{}, errors.New("invalid ClickHouse database URL")
	}
	selected := dburlext.Selection{Database: options.Auth.Database}
	for _, address := range options.Addr {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			host, port = address, ""
		}
		selected.Endpoints = append(selected.Endpoints, dburlext.Endpoint{Host: host, Port: port})
	}
	return selected, nil
}
