// Package sqlserver reads what a SQL Server URL selects, as go-mssqldb's
// msdsn reads it: a named instance and a failover partner are part of the
// servers the URL may reach.
package sqlserver

import (
	"errors"
	"net/url"
	"strconv"
	"strings"

	"github.com/microsoft/go-mssqldb/msdsn"

	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/dburlext"
)

// Services returns SQL Server's services.
func Services() dburlext.Services {
	return dburlext.Services{Dialects: []string{platform.SQLServer}, Endpoints: endpoints{}, Rename: renamer{}}
}

type endpoints struct{}

type renamer struct{}

// Rename changes the database query parameter. The URL path names an instance,
// so replacing it would leave the connection on the original database.
func (renamer) Rename(parsed *url.URL, name string) (string, error) {
	changed := *parsed
	var pairs []string
	if changed.RawQuery != "" {
		pairs = strings.Split(changed.RawQuery, "&")
	}
	found := false
	for i, pair := range pairs {
		rawKey, _, _ := strings.Cut(pair, "=")
		key, err := url.QueryUnescape(rawKey)
		if err != nil || !strings.EqualFold(key, "database") {
			continue
		}
		pairs[i] = rawKey + "=" + url.QueryEscape(name)
		found = true
	}
	if !found {
		pairs = append(pairs, "database="+url.QueryEscape(name))
	}
	changed.RawQuery = strings.Join(pairs, "&")
	return changed.String(), nil
}

// Select reads the URL under the dialect's name, the scheme msdsn reads, so
// an mssql:// URL is read as the sqlserver:// URL it is. An instance name is
// compared without regard to case, as the server resolves it.
func (endpoints) Select(parsed *url.URL, dialect descriptor.Dialect) (dburlext.Selection, error) {
	connectionURL := *parsed
	connectionURL.Scheme = dialect.Name
	config, err := msdsn.Parse(connectionURL.String())
	if err != nil {
		return dburlext.Selection{}, errors.New("invalid SQL Server database URL")
	}
	selected := dburlext.Selection{
		Endpoints: []dburlext.Endpoint{{Host: config.Host, Port: port(config.Port), Instance: strings.ToLower(config.Instance)}},
		Database:  config.Database,
	}
	if config.FailOverPartner != "" {
		selected.Endpoints = append(selected.Endpoints, dburlext.Endpoint{Host: config.FailOverPartner, Port: port(config.FailOverPort)})
	}
	return selected, nil
}

// port spells a port msdsn read, and nothing for the zero it reads when the
// URL names none.
func port(value uint64) string {
	if value == 0 {
		return ""
	}
	return strconv.FormatUint(value, 10)
}
