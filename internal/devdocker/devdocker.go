// Package devdocker turns an Atlas-style `docker://` dev-database URL into a
// directly connectable one by starting a throwaway container and removing it
// again.
//
// Ptah's planning, replay and inspection paths all take a dev database as a URL
// they can open. Atlas takes the same flag but accepts a `docker://` value and
// provisions the database itself, so an Atlas project configuration that names
// one could not be run by `ptah-compat` at all: every consumer refused the value
// with its own sentence (stokaro/ptah#844). This package is the missing half.
// It is deliberately not coupled to cobra or to any one verb -- [Resolve] takes
// a URL and returns a URL, so a consumer that already knows how to open a dev
// database needs one line and no new concepts.
//
// # The URL form is measured, not guessed
//
// Everything the parser accepts or refuses below was measured against the
// pinned community binary v1.3.0 on 2026-08-13, each exit status read from an
// unpiped invocation:
//
//	docker://postgres/16/dev     exit 0, provisions postgres:16
//	docker://postgres/dev        exit 1, `Unable to find image 'postgres:dev'`
//	docker://postgres            exit 0, provisions a default tag
//	docker://postgres:16/dev     exit 1, `unsupported docker image "postgres:16"`
//	docker://nosuchengine/1/dev  exit 1, `unsupported docker image "nosuchengine"`
//	docker:///dev                exit 1, `unsupported docker image ""`
//	docker://sqlite/dev          exit 1, `unsupported docker image "sqlite"`
//
// Two of those rows are the reason this parser does not reuse
// [atlasurl.DialectFromURL], which answers a dialect for both `docker://sqlite`
// and `docker://postgres:16/dev`. Provisioning on either would make `ptah-compat`
// exit 0 where the pinned binary exits 1, which AGENTS.md compatibility rule (a)
// forbids outright. The host segment is therefore matched against an explicit
// engine table and a colon in it is refused, in the pinned binary's own words.
//
// The path is `/<tag>` or `/<tag>/<database>`: measured, `docker://postgres/dev`
// resolves `dev` as an image TAG and not as a database name, which is why a
// one-segment path is not read as a database.
//
// # Images
//
// The pinned binary pulls `postgres:<tag>` for PostgreSQL but the vendor's own
// `arigaio/mysql:<tag>` and `arigaio/mariadb:<tag>` for the MySQL family --
// measured from its `Unable to find image` diagnostics. Ptah uses the official
// `mysql` and `mariadb` images instead. That is a deliberate divergence: it
// removes a dependency on one vendor's registry account, and it is in the
// direction rule (b) permits, since a run that reaches a database at all is
// strictly more than one that cannot pull the image. It is recorded in
// docs/conformance.md.
package devdocker

import (
	"fmt"
	"maps"
	"net/url"
	"strings"

	"go.5x5.cz/ptah/core/platform"
)

// Scheme is the URL scheme this package provisions.
const Scheme = "docker"

// DefaultTag is the image tag used when the URL names none. The pinned binary
// accepts a bare `docker://postgres` and provisions a default; ptah spells that
// default `latest` so the tag is always visible in the image name it reports.
const DefaultTag = "latest"

// DefaultDatabase is the database created inside the container when the URL
// names none. It matches the name Atlas project configurations conventionally
// use (`docker://postgres/16/dev`).
const DefaultDatabase = "dev"

// passwordBytes is the entropy of the superuser password set inside each
// throwaway container.
//
// The password is generated per instance, not fixed. It used to be the constant
// `ptah-dev`, justified by the container publishing on loopback only -- and that
// premise stopped being true the moment a remote daemon began publishing on
// every interface of its host, which is a machine other peers can reach. A
// known superuser password on a reachable ephemeral port lets any of them read
// the replayed schema, or write to it and quietly corrupt a lint or diff result.
//
// The fix is the credential rather than the binding, because the binding cannot
// be tightened: a daemon can only publish on interfaces it owns, and the one
// this process needs to reach is not that host's loopback.
const passwordBytes = 24

// IsURL reports whether rawURL names a dev database this package provisions.
//
// It answers on the scheme alone, so a malformed docker URL is still routed
// here and refused with the diagnostic that names what is wrong with it, rather
// than falling through to a connector that would report an unknown dialect.
func IsURL(rawURL string) bool {
	trimmed := strings.TrimSpace(rawURL)
	return strings.HasPrefix(trimmed, Scheme+"://")
}

// engine describes one database this package can start.
type engine struct {
	// image is the container image, without a tag.
	image string
	// dialect is the Ptah dialect the provisioned database speaks.
	dialect string
	// port is the port the server listens on inside the container.
	port string
	// params are the connection parameters the provisioned URL needs by
	// default. Parameters written on the docker URL are merged over these, so
	// an operator can override one and cannot lose one they did not mention.
	params map[string]string
	// env builds the container environment that creates database.
	env func(database, password string) []string
	// url builds a directly connectable URL for the published hostPort. query
	// is the merged parameter string, already encoded and without a leading
	// `?`; it is empty when there are no parameters.
	url func(hostPort, database, password, query string) string
}

// engines maps the host segment of a docker URL onto the container to start.
//
// The keys are the spellings the pinned binary accepts, measured one at a time:
// `postgres`, `mysql`, and both `maria` and `mariadb`. Anything else is refused,
// including schemes [platform.NormalizeDialect] would happily name -- `sqlite`
// is a dialect Ptah has and an image the pinned binary refuses, so it must not
// appear here.
var engines = map[string]engine{
	"postgres": {
		image:   "postgres",
		dialect: platform.Postgres,
		port:    "5432",
		// The container publishes on loopback and speaks no TLS, so the
		// default disables it. An operator who writes `sslmode` on the docker
		// URL replaces this value rather than being silently overruled.
		params: map[string]string{"sslmode": "disable"},
		env: func(database, password string) []string {
			return []string{
				"POSTGRES_PASSWORD=" + password,
				"POSTGRES_USER=postgres",
				"POSTGRES_DB=" + database,
			}
		},
		url: func(hostPort, database, password, query string) string {
			return fmt.Sprintf("postgres://postgres:%s@%s/%s%s", password, hostPort, url.PathEscape(database), querySuffix(query))
		},
	},
	"mysql": {
		image:   "mysql",
		dialect: platform.MySQL,
		port:    "3306",
		env: func(database, password string) []string {
			return []string{
				"MYSQL_ROOT_PASSWORD=" + password,
				"MYSQL_DATABASE=" + database,
			}
		},
		// The database is NOT escaped here, unlike the PostgreSQL URL above.
		// A MySQL DSN is not a URL: the driver reads the name between the last
		// `/` and the `?` literally and never percent-decodes it, so escaping
		// would connect to a database called `foo%23bar` while the container
		// created `foo#bar`. The one delimiter that would break the DSN, `?`,
		// is refused in [Parse] instead.
		url: func(hostPort, database, password, query string) string {
			return fmt.Sprintf("mysql://root:%s@tcp(%s)/%s%s", password, hostPort, database, querySuffix(query))
		},
	},
	"maria":   mariaEngine,
	"mariadb": mariaEngine,
}

// mariaEngine is shared by both spellings the pinned binary accepts for
// MariaDB, so the two cannot drift apart.
var mariaEngine = engine{
	image:   "mariadb",
	dialect: platform.MariaDB,
	port:    "3306",
	env: func(database, password string) []string {
		return []string{
			"MARIADB_ROOT_PASSWORD=" + password,
			"MARIADB_DATABASE=" + database,
		}
	},
	url: func(hostPort, database, password, query string) string {
		return fmt.Sprintf("mariadb://root:%s@tcp(%s)/%s%s", password, hostPort, database, querySuffix(query))
	},
}

// querySuffix renders an encoded parameter string as a URL suffix.
//
// It exists so each engine's URL template can carry parameters without every
// one of them repeating the empty-string case, and so a `?` never appears with
// nothing after it -- the MySQL driver reads its DSN by cutting on `?` and a
// bare one leaves it parsing an empty parameter list.
func querySuffix(query string) string {
	if query == "" {
		return ""
	}
	return "?" + query
}

// mergeParams overlays the parameters written on the docker URL onto the
// engine's defaults and encodes the result.
//
// The operator's value wins on a key both name. That direction is deliberate:
// the defaults exist to make a throwaway container reachable, and an operator
// who writes one has a reason the container cannot know. Keys the operator did
// not mention survive, which is the half a naive "use the operator's query if
// there is one" would drop.
func mergeParams(defaults map[string]string, rawQuery string) (string, error) {
	operator, err := url.ParseQuery(rawQuery)
	if err != nil {
		return "", fmt.Errorf("parse docker --dev-url parameters %q: %w", rawQuery, err)
	}
	merged := url.Values{}
	for key, value := range defaults {
		merged.Set(key, value)
	}
	// Copied wholesale rather than appended: a key the operator wrote REPLACES
	// the default for that key, and appending would send both values.
	maps.Copy(merged, operator)
	return merged.Encode(), nil
}

// Spec is a parsed `docker://` dev URL: everything needed to start the
// container, with no reference left to the text it came from.
type Spec struct {
	// Engine is the host segment as written, kept for diagnostics.
	Engine string
	// Dialect is the Ptah dialect the provisioned database speaks.
	Dialect string
	// Image is the fully tagged container image to run.
	Image string
	// Database is the database created inside the container.
	Database string
	// Query is the encoded connection parameter string the provisioned URL
	// carries: the engine's defaults with the operator's written over them.
	Query string

	engine engine
}

// URL is the directly connectable URL for a server published at hostPort. It
// carries the operator's parameters.
func (s Spec) URL(hostPort, password string) string {
	return s.engine.url(hostPort, s.Database, password, s.Query)
}

// ReadyURL is the URL the readiness wait probes: the same server, with the
// engine's own parameters only.
//
// Readiness asks whether the CONTAINER is up, and the operator's parameters can
// make that question unanswerable. Measured: `docker://postgres/16/dev?search_path=app`
// probed with the full URL fails every attempt with `schema "app" does not
// exist` -- a verdict no amount of waiting changes -- so the wait spent its
// whole two-minute budget before reporting a deterministic error. Probing the
// server and letting the consumer open the full URL turns that into an
// immediate refusal naming the schema, which is also when the pinned community
// binary v1.3.0 reports it.
func (s Spec) ReadyURL(hostPort, password string) string {
	return s.engine.url(hostPort, s.Database, password, defaultParams(s.engine.params))
}

// Env is the container environment that creates the database with password as
// its superuser credential.
func (s Spec) Env(password string) []string {
	return s.engine.env(s.Database, password)
}

// Port is the port the provisioned server listens on inside the container.
func (s Spec) Port() string {
	return s.engine.port
}

// defaultParams encodes an engine's own parameters, with nothing merged in.
func defaultParams(params map[string]string) string {
	values := url.Values{}
	for key, value := range params {
		values.Set(key, value)
	}
	return values.Encode()
}

// unsupportedImageError is the pinned binary's refusal for a host segment that
// names no image it can start. The value it quotes is the host as written, so
// `docker://postgres:16/dev` reports `"postgres:16"` and not `"postgres"`.
func unsupportedImageError(host string) error {
	return fmt.Errorf("unsupported docker image %q", host)
}

// Parse interprets rawURL as `docker://<engine>[/<tag>[/<database>]]`.
//
// The host is matched whole: a colon in it is part of the name being refused,
// not a port to strip, because the pinned binary refuses `docker://postgres:16`
// outright rather than reading `16` as a tag. Splitting it would make Ptah
// provision a database for a URL the pinned binary rejects.
func Parse(rawURL string) (Spec, error) {
	trimmed := strings.TrimSpace(rawURL)
	parsed, err := url.Parse(trimmed)
	if err != nil {
		return Spec{}, fmt.Errorf("parse docker --dev-url: %w", err)
	}
	if parsed.Scheme != Scheme {
		return Spec{}, fmt.Errorf("not a docker --dev-url: %q", rawURL)
	}
	host := parsed.Host
	found, ok := engines[strings.ToLower(host)]
	if !ok {
		return Spec{}, unsupportedImageError(host)
	}
	tag, database, err := splitDockerPath(parsed.Path)
	if err != nil {
		return Spec{}, err
	}
	// The parameters travel with the spec. Dropping them would accept a URL and
	// then ignore half of what it said -- and measurably so: the pinned
	// community binary v1.3.0 honors them, and a `?search_path=app` it answers
	// `schema "app" was not found` (exit 1) was answered here by inspecting
	// `public` and exiting 0 while they were being discarded.
	query, err := mergeParams(found.params, parsed.RawQuery)
	if err != nil {
		return Spec{}, err
	}
	return Spec{
		Engine:   host,
		Dialect:  found.dialect,
		Image:    found.image + ":" + tag,
		Database: database,
		Query:    query,
		engine:   found,
	}, nil
}

// splitDockerPath reads the tag and database name out of a docker URL path.
//
// One segment is a TAG, not a database: measured, `docker://postgres/dev` makes
// the pinned binary look for the image `postgres:dev`. Reading it as a database
// name would silently run a different image than the operator asked for.
func splitDockerPath(path string) (tag, database string, err error) {
	trimmed := strings.Trim(path, "/")
	if trimmed == "" {
		return DefaultTag, DefaultDatabase, nil
	}
	segments := strings.Split(trimmed, "/")
	if len(segments) > 2 {
		return "", "", fmt.Errorf("docker --dev-url path %q has more than <tag>/<database>", path)
	}
	tag = segments[0]
	if tag == "" {
		return "", "", fmt.Errorf("docker --dev-url image tag is empty")
	}
	database = DefaultDatabase
	if len(segments) == 2 {
		database = segments[1]
		if database == "" {
			return "", "", fmt.Errorf("docker --dev-url database name is empty")
		}
		// `?` can only reach here as `%3F`, because an unescaped one starts the
		// URL query. It is refused because a MySQL DSN carries the database
		// name literally and would read everything after it as connection
		// parameters, and there is no escaping that fixes that without renaming
		// the database. Every other delimiter is handled by escaping the
		// PostgreSQL path segment.
		if strings.Contains(database, "?") {
			return "", "", fmt.Errorf("docker --dev-url database name %q contains a query separator", database)
		}
	}
	return tag, database, nil
}
