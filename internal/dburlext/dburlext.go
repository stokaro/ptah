// Package dburlext declares what an engine does differently with a database
// URL before anything is connected: which servers and which database the URL
// selects, whether two of its URLs address one database, and how a URL is
// renamed to another database. internal/atlasurl answers those questions
// through these services, so it names no engine, and internal/dburlext/builtin
// lists the services of the engines Ptah ships.
//
// What an engine only states about its URLs, such as its default port or the
// dialect its URLs are compared as, is descriptor data instead
// (core/platform/descriptor.URLs). A service reads the descriptor where the
// two meet, such as the scheme its driver reads.
package dburlext

import (
	"net/url"

	"ptah.run/core/platform/descriptor"
)

// Services is what one engine does differently with a database URL. A nil
// field takes the generic behavior its documentation names, which is also
// the behavior of a dialect no service serves.
type Services struct {
	// Dialects are the canonical names of the dialects these services serve.
	Dialects []string
	// Endpoints reads the servers and the database a URL selects. Nil reads
	// the host, the port and the path the URL names.
	Endpoints EndpointReader
	// Databases decides whether two URLs of the engine address one database,
	// for an engine whose URL does not name a server, such as one whose
	// database is a file. Nil compares the servers and databases the URLs
	// select.
	Databases DatabaseComparer
	// Rename renames the database a URL names. Nil replaces the URL's path.
	Rename Renamer
}

// Endpoint is one server a URL may reach.
type Endpoint struct {
	// Host is the host name, address or socket path, as the driver reads it.
	Host string
	// Port is the port, or empty where neither the URL nor the driver names
	// one.
	Port string
	// Instance names one of several servers on one host and port, such as a
	// SQL Server named instance, in the case the engine compares it in. Empty
	// names none.
	Instance string
}

// Selection is what a database URL selects.
type Selection struct {
	// Endpoints are the servers the URL may reach: the one it names first,
	// then each one the driver falls back to. Empty means the host and port
	// the URL names.
	Endpoints []Endpoint
	// Database is the database the URL selects on them, or empty for none.
	Database string
}

// EndpointReader reads what a database URL selects.
type EndpointReader interface {
	// Select reads what parsed selects. dialect is the descriptor of the
	// dialect its scheme names. An error refuses the URL in words that do
	// not repeat it, since it may carry a password.
	Select(parsed *url.URL, dialect descriptor.Dialect) (Selection, error)
}

// DatabaseComparer decides whether two URLs of one engine address one
// database.
type DatabaseComparer interface {
	// SameDatabase reports whether left and right address one database. The
	// answer settles both questions internal/atlasurl asks: whether the URLs
	// are one endpoint, and whether they may address one database.
	SameDatabase(left, right *url.URL) (bool, error)
}

// Renamer renames the database a URL names.
type Renamer interface {
	// Rename returns parsed addressing the database name instead, or an
	// error refusing the rename. It does not change parsed.
	Rename(parsed *url.URL, name string) (string, error)
}
