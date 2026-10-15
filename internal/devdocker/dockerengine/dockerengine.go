// Package dockerengine declares what internal/devdocker needs to know to
// start one database engine in a throwaway container: the image, the port,
// the environment that creates the database and the URL that reaches it.
//
// Each engine family declares its engines in a package of its own below
// internal/devdocker, and devdocker indexes them by [Engine.Name]. So the
// table of engines a docker URL may name holds no engine's name in shared
// code, and an engine is added by declaring it rather than by editing
// devdocker.
package dockerengine

import "context"

// Engine is one database this package's caller can start. A field a docker
// URL does not use is left at its zero value.
type Engine struct {
	// Name is the host segment of a `docker://<name>/...` URL and the driver
	// of a `docker+<name>://` one that starts this engine, matched as
	// written. Two names may start the same container; each is its own
	// Engine.
	Name string
	// Image is the container image, without a tag.
	Image string
	// Port is the port the server listens on inside the container.
	Port string
	// Params are the connection parameters the provisioned URL needs by
	// default. Parameters written on the docker URL are merged over these, so
	// an operator can override one and cannot lose one they did not mention.
	Params map[string]string
	// Env builds the container environment that creates database.
	Env func(database, password string) []string
	// URL builds a directly connectable URL for the published hostPort. query
	// is the merged parameter string, already encoded and without a leading
	// `?`; it is empty when there are no parameters. [QuerySuffix] renders
	// it.
	URL func(hostPort, database, password, query string) string
	// ServerDatabase is what a URL naming an image connects to when it names
	// no database, and what the readiness wait probes on such an image:
	// `postgres` on PostgreSQL, and no database -- the whole server -- on the
	// MySQL family. It is the pinned community binary's own default for each.
	ServerDatabase string
	// CreatesDatabase reports an image that fills the database its variable
	// names with objects of its own, so Env leaves the variable unset and the
	// database is created once the server is ready.
	CreatesDatabase bool
	// DefaultDatabase is the database a URL that names none connects to, when
	// it is not devdocker's default.
	DefaultDatabase string
	// Databases, when set, lists the only databases the image serves; a URL
	// naming another is refused.
	Databases []string
	// Routing are the engine's own parameters that would point the
	// connection away from the container, refused as devdocker refuses the
	// routing parameters every engine shares.
	Routing []string
	// Anonymous reports a server that takes every connection without a
	// credential, which no generated password can change. Such a server is
	// started only by a container runtime on this machine, where its port is
	// published on loopback.
	Anonymous bool
	// Ready, when set, is the engine's readiness probe in place of
	// devdocker's default, which opens the URL and closes it again.
	Ready func(ctx context.Context, rawURL string) error
	// BlockAttributes are the attributes an atlas.hcl `docker "<name>"` block
	// may carry for this engine. Nil means the engine has no such block.
	BlockAttributes []string
}

// QuerySuffix renders an encoded query for the end of a URL: `?` and the
// query, or nothing when the query is empty.
//
// It exists so each engine's URL template can carry parameters without every
// one of them repeating the empty-string case, and so a `?` never appears with
// nothing after it -- the MySQL driver reads its DSN by cutting on `?` and a
// bare one leaves it parsing an empty parameter list.
func QuerySuffix(query string) string {
	if query == "" {
		return ""
	}
	return "?" + query
}
