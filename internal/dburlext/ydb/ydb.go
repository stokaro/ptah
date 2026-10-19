// Package ydb reads what a YDB URL selects and the database root it names, as
// internal/ydburl reads them, and refuses to rename the database one names.
package ydb

import (
	"errors"
	"fmt"
	"net/url"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/dburlext"
	"ptah.run/internal/ydburl"
)

// ErrSourceRoot is the refusal of the URL a YDB schema document is read
// against when it names no database root.
var ErrSourceRoot = errors.New("YDB source context requires a valid YDB database or Docker dev URL")

// ErrDiffRoot is the refusal of the dev URL of a YDB diff between two schema
// documents when it names no database root.
var ErrDiffRoot = errors.New("YDB diff requires a valid database or Docker dev URL")

// ErrDatabaseName is the refusal a rename of a YDB URL gives.
var ErrDatabaseName = errors.New("a YDB database cannot be created through SQL, so a URL renamed to a new " +
	"database names one nothing can create; a YDB dev or scratch database is a dev realm in the database the URL names")

// Services returns YDB's services.
func Services() dburlext.Services {
	return dburlext.Services{
		Dialects:  []string{platform.YDB},
		Endpoints: endpoints{},
		Rename:    rename{},
		Roots:     roots{},
	}
}

type endpoints struct{}

// Select reads the endpoint and the database root the URL names.
//
// The port defaults by scheme rather than by dialect: both schemes are YDB,
// and a server listens for plaintext on one port and for TLS on another.
//
// A dev realm is a database of its own to every reader and writer a
// connection to it builds, and the database that holds it leaves the realm
// out of everything it reads and resets. A comparison that may address one
// database leaves the endpoint out, and on YDB a host proves least: every
// node of a cluster answers for each of its databases. Two servers that both
// name /local are told apart by the dev realm a dev URL resolves to, and
// after connecting by their live identity.
func (endpoints) Select(parsed *url.URL, _ descriptor.Dialect) (dburlext.Selection, error) {
	ydbURL, err := ydburl.FromURL(parsed)
	if err != nil {
		return dburlext.Selection{}, fmt.Errorf("invalid YDB database URL: %w", err)
	}
	return dburlext.Selection{
		Endpoints: []dburlext.Endpoint{{Host: ydbURL.Host, Port: ydbURL.Port}},
		Database:  ydbURL.Root(),
	}, nil
}

type rename struct{}

// Rename refuses with [ErrDatabaseName]. A YDB database cannot be created
// with SQL, so a URL naming a new one names a database nothing can make, and
// its name may also sit in a database parameter that a new path would not
// replace.
func (rename) Rename(*url.URL, string) (string, error) {
	return "", ErrDatabaseName
}

type roots struct{}

// Root reads the database a YDB URL names, from its path or its database
// parameter, and the dev realm it names in that database, as
// internal/ydburl reads them.
func (roots) Root(parsed *url.URL) (dburlext.DatabaseRoot, error) {
	ydbURL, err := ydburl.FromURL(parsed)
	if err != nil {
		return dburlext.DatabaseRoot{}, err
	}
	return dburlext.DatabaseRoot{Path: ydbURL.Database, Realm: ydbURL.Realm}, nil
}

// DatabaseRoot returns the path of the database a Docker dev URL creates: its
// name with one leading slash, and the bare slash for an empty name.
func (roots) DatabaseRoot(name string) string {
	return "/" + strings.Trim(name, "/")
}

// SourceRefusal returns [ErrSourceRoot].
func (roots) SourceRefusal() error { return ErrSourceRoot }

// DiffRefusal returns [ErrDiffRoot].
func (roots) DiffRefusal() error { return ErrDiffRoot }
