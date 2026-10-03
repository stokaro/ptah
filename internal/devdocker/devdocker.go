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
// Measured on 2026-10-03 on a Linux host with a local daemon, reading the
// container the binary started with `docker inspect` (stokaro/ptah#4066):
//
//	docker://postgis/16-3.4/dev   exit 0, image postgis/postgis:16-3.4, env
//	                              POSTGRES_PASSWORD alone, database dev created
//	                              by the binary, `CREATE EXTENSION postgis` succeeds
//	docker://postgis/16-3.4       exit 1, database postgres, where the image put
//	                              PostGIS: `connected database is not clean`
//	docker://pgvector/pg16/dev    exit 0, image pgvector/pgvector:pg16, env
//	                              POSTGRES_PASSWORD and POSTGRES_DB=dev
//	docker://pgvector/pg16        exit 0, database postgres
//	docker://postgis/dev          exit 1, `Unable to find image 'postgis/postgis:dev'`
//	docker://POSTGRES/16-alpine/dev     exit 1, `unsupported docker image "POSTGRES"`
//	docker://clickhouse/24.8/dev        exit 1, `unknown driver "clickhouse"`
//	docker://sqlserver/2022-latest/dev  exit 1, `unknown driver "sqlserver"`
//
// The engine is matched as written, so `POSTGRES`, `Postgres` and `POSTGIS` are
// refused. Without a database Ptah creates `dev` for every engine, so
// `docker://postgis/16-3.4` exits 0 here where the binary connects to the
// database its own image filled and exits 1.
//
// The host segment is matched whole against an explicit engine table, and a
// colon in it is refused, in the pinned binary's own words. Reading it as a
// dialect instead would provision `docker://sqlite` and `docker://postgres:16/dev`
// -- `sqlite` is a dialect Ptah has, and `postgres:16` reads as an engine with a
// port -- where the pinned binary exits 1, which AGENTS.md compatibility rule
// (a) forbids outright. [atlasurl.DockerEngineDialect] is the one answer to
// which engines there are and what each speaks, and [atlasurl.DialectFromURL]
// refuses the same values with the same sentence.
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
//
// # An image the URL names
//
// `docker+<driver>://[<host>/]<image>[:<tag>][/<database>]` starts the image
// the URL names instead of an engine's own (stokaro/ptah#4040). Measured against
// the pinned community binary v1.3.0 on 2026-10-03 on a Linux host, each exit
// status read from an unpiped `schema inspect -u file://schema.sql --dev-url
// <value>`:
//
//	docker+postgres://_/postgres:17/dev                  exit 0, image postgres:17, database dev
//	docker+postgres://_/postgres:17                      exit 0, database postgres
//	docker+postgres:///postgres:17/dev                   exit 0, an empty host is `_`
//	docker+postgres://docker.io/library/postgres:17/dev  exit 0, the host leads the image
//	docker+postgres://_/library/postgres:17/dev          exit 0, the image may hold a slash
//	docker+postgres://_/img/dev                          exit 0, an image with no tag is `latest`
//	docker+postgres://_/img                              exit 0, one segment is the image
//	DOCKER+POSTGRES://_/postgres:17/dev                  exit 0, the scheme is not case-sensitive
//	docker+postgres://_/                                 exit 1, `invalid configuration`, no image
//	docker+postgres://postgres:17/dev                    exit 1, image `postgres:17/dev`
//	docker+postgres://_/postgres:17/dev/extra            exit 1, image `postgres:17/dev`
//	docker+sqlite://_/postgres:17/dev                    exit 1, `sql/sqlclient: unknown driver "docker+sqlite"`
//	docker+postgis://_/postgres:17/dev                   exit 1, the same, for `docker+postgis`
//	docker+mysql://_/mysql:8.4.11/dev                    exit 0, database dev
//	docker+mysql://_/mysql:8.4.11                        no database: the whole server
//	docker+maria://_/mariadb:11.8.9/dev                  exit 0, and `docker+mariadb` too
//
// The path is read as that binary reads it: the last segment is the database
// when there are two or more and it holds no colon, everything before it is the
// image, and a host other than `_` leads the image. So `docker+postgres://_/org/img`
// is the image `org` and the database `img`, and the run fails on the pull --
// loudly, as it does on that binary. Without a database, a PostgreSQL URL
// connects to `postgres` and a MySQL-family URL to the whole server.
//
// The image decides whether the database exists. The binary passes the
// database to a PostgreSQL image as `POSTGRES_DB` and to a MySQL-family image as
// `MYSQL_DATABASE`, and runs `CREATE DATABASE IF NOT EXISTS` for the MySQL family
// only. Measured with images that ignore those variables, the MySQL run exits 0
// and the PostgreSQL run exits 1 with `database "dev" does not exist`. Ptah
// creates the database for both: a run that fails because an image ignored a
// variable fails for a reason unrelated to what the operator asked for. So the
// readiness wait probes the server's own database, and the URL's database is
// created when the image did not create it.
//
// That binary also registers `docker+clickhouse` and `docker+sqlserver`. Ptah
// starts neither engine from a docker URL, and refuses both by name.
package devdocker

import (
	"fmt"
	"maps"
	"net/url"
	"path"
	"slices"
	"strings"

	"ptah.run/internal/atlasurl"
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
// The password is generated per instance, not fixed. A constant like `ptah-dev`
// rests on the container publishing on loopback only, and that premise fails
// the moment a remote daemon publishes on every interface of its host, which is
// a machine other peers can reach. A known superuser password on a reachable
// ephemeral port lets any of them read the replayed schema, or write to it and
// quietly corrupt a lint or diff result.
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
//
// It reads the bytes AS WRITTEN and normalizes nothing; see [Parse].
//
// The scheme is taken from [url.Parse] rather than matched as a prefix, so this
// and [Parse] answer the same question. A prefix match disagreed with it twice,
// in opposite directions: `DOCKER://postgres/16/dev` is a docker URL to
// url.Parse, which lowercases the scheme, and to the pinned binary, which
// provisions it -- but not to a case-sensitive prefix, so Resolve passed it
// through unprovisioned and the connector rejected the dialect. And
// ` docker://postgres/16/dev` is NOT a docker URL to url.Parse, which reads it
// as a relative path, but was one to a prefix match over a trimmed copy.
//
// A `docker+<driver>://` URL is a docker URL too, for each driver
// [atlasurl.DockerImageDriver] recognizes.
func IsURL(rawURL string) bool {
	parsed, err := url.Parse(rawURL)
	return err == nil && atlasurl.IsDockerScheme(parsed.Scheme)
}

// engine describes one database this package can start. The dialect it
// speaks is [atlasurl.DockerEngineDialect]'s, which is also the answer to
// which engines there are.
type engine struct {
	// image is the container image, without a tag.
	image string
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
	// serverDatabase is what a URL naming an image connects to when it names
	// no database, and what the readiness wait probes on such an image:
	// `postgres` on PostgreSQL, and no database -- the whole server -- on the
	// MySQL family. It is the pinned community binary's own default for each.
	serverDatabase string
	// createsDatabase reports an image that fills the database its variable
	// names with objects of its own, so env leaves the variable unset and the
	// database is created once the server is ready; see
	// [Spec.CreatesDatabase].
	createsDatabase bool
}

// postgresEnv is the container environment of an image built on the official
// PostgreSQL one.
//
// POSTGRES_USER is the image's. The official image's default is postgres, and
// an image that names another user runs its init scripts as that user: the
// Supabase image declares supabase_admin, and with the variable overridden its
// init fails and the container exits (stokaro/ptah#4053). The pinned binary
// passes these two variables and no user, measured on 2026-10-03.
func postgresEnv(database, password string) []string {
	return []string{
		"POSTGRES_PASSWORD=" + password,
		"POSTGRES_DB=" + database,
	}
}

// postgresURL is the connectable URL of a server an image built on the
// official PostgreSQL one runs.
func postgresURL(hostPort, database, password, query string) string {
	return fmt.Sprintf("postgres://postgres:%s@%s/%s%s", password, hostPort, url.PathEscape(database), querySuffix(query))
}

// postgresParams are the connection parameters of such a server. The
// container publishes on loopback and speaks no TLS, so the default disables
// it. An operator who writes `sslmode` on the docker URL replaces this value
// rather than being silently overruled.
func postgresParams() map[string]string {
	return map[string]string{"sslmode": "disable"}
}

// engines maps the host segment of a docker URL onto the container to start.
//
// The keys are the spellings the pinned binary accepts, measured one at a time,
// and match [atlasurl.DockerEngineDialect]'s. Anything else is refused,
// including schemes [ptah.run/core/platform.NormalizeDialect] would happily name -- `sqlite`
// is a dialect Ptah has and an image the pinned binary refuses, so it must not
// appear here.
var engines = map[string]engine{
	"postgres": {
		image:          "postgres",
		port:           "5432",
		params:         postgresParams(),
		env:            postgresEnv,
		url:            postgresURL,
		serverDatabase: "postgres",
	},
	// The pgvector project's image is the official one with the extension
	// installed and not created. Measured, the pinned binary starts
	// `pgvector/pgvector:<tag>` with POSTGRES_PASSWORD and POSTGRES_DB, as it
	// starts `postgres:<tag>`.
	"pgvector": {
		image:          "pgvector/pgvector",
		port:           "5432",
		params:         postgresParams(),
		env:            postgresEnv,
		url:            postgresURL,
		serverDatabase: "postgres",
	},
	// The PostGIS image creates postgis, postgis_topology, fuzzystrmatch and
	// postgis_tiger_geocoder, and the tiger and topology schemas, in the
	// database POSTGRES_DB names. Measured, the pinned binary starts
	// `postgis/postgis:<tag>` with POSTGRES_PASSWORD alone and creates the
	// URL's database itself, which leaves it empty: `CREATE EXTENSION postgis`
	// succeeds there.
	"postgis": {
		image:  "postgis/postgis",
		port:   "5432",
		params: postgresParams(),
		env: func(_, password string) []string {
			return []string{"POSTGRES_PASSWORD=" + password}
		},
		url:             postgresURL,
		serverDatabase:  "postgres",
		createsDatabase: true,
	},
	"mysql": {
		image: "mysql",
		port:  "3306",
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
	image: "mariadb",
	port:  "3306",
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

// routingParams are the connection parameters that decide WHICH server, and as
// which identity, a client connects to.
//
// libpq and pgx accept these in the query string and let them override the
// URL's own authority, so `docker://postgres/16/dev?host=prod.example` would
// provision a throwaway container, pass readiness against it -- the probe URL
// carries the engine's parameters only -- and then hand the consumer a URL
// pointing somewhere else entirely. What the consumer does next is drop every
// table and replay a migration directory. `service` and `servicefile` are in
// the set because they pull a whole connection definition, host included, out
// of a file.
//
// This is not caught anywhere else: the alias checks that ask whether the dev
// database IS the target are deliberately skipped for `docker://` URLs,
// precisely because a container that does not exist yet cannot be a database
// the operator already named.
var routingParams = []string{
	"host", "hostaddr", "port", "user", "password", "dbname",
	"passfile", "service", "servicefile",
}

// refuseRoutingParams rejects a dev URL that tries to redirect the connection
// away from the container it asked to provision.
//
// It refuses rather than dropping. Measured on 2026-08-13, the pinned community
// binary v1.3.0 exits 0 on `docker://postgres/16/dev?host=192.0.2.1&port=5432`
// and inspects the CONTAINER, so it ignores the parameter -- but silently
// discarding something the operator wrote is the defect this whole change
// exists to remove, and a routing parameter on a URL that provisions its own
// server is a contradiction worth naming rather than papering over. Ptah exits
// 1 where that binary exits 0 here, which is a capability gap and not a safety
// hole (AGENTS.md rule (a) runs the other way), and rule (b) is explicit that
// matching is the floor.
func refuseRoutingParams(operator url.Values) error {
	for _, name := range routingParams {
		if _, present := operator[name]; present {
			return fmt.Errorf(
				"docker --dev-url parameter %q would point the connection away from the"+
					" container this URL provisions; remove it, or pass a directly"+
					" connectable dev database URL instead",
				name,
			)
		}
	}
	return nil
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
	if err := refuseRoutingParams(operator); err != nil {
		return "", err
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
	// fromImage reports a `docker+<driver>://` URL, whose image is the one the
	// URL names rather than the engine's own.
	fromImage bool
	// declaration is what an atlas.hcl docker block adds to such a URL; see
	// [Declare]. Its zero value adds nothing.
	declaration Declaration
	// fromBlock reports that the URL names a declaration an atlas.hcl docker
	// block recorded, so the state the provisioning leaves is the dev
	// database's starting point; see [StartingPointDeclared].
	fromBlock bool
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
//
// On an image the URL names, the wait probes the server's own database instead,
// [Spec.CreatesDatabase] says why.
func (s Spec) ReadyURL(hostPort, password string) string {
	database := s.Database
	if s.databaseFollowsReadiness() {
		database = s.engine.serverDatabase
	}
	return s.engine.url(hostPort, database, password, defaultParams(s.engine.params))
}

// CreatesDatabase reports whether the database the URL names is created once
// the server is ready, rather than left to the image.
//
// It is for an image the URL names. Such an image decides for itself whether it
// honors the variable that names a database, and one that does not leaves the
// database missing: measured, the pinned community binary v1.3.0 then exits 1
// with `database "dev" does not exist` on PostgreSQL. It is also for the
// PostGIS engine, whose image fills the database its variable names; see
// engines. Every other engine's image honors the variable, so its database is
// never created here. Neither is the server's own database, which every image
// has.
func (s Spec) CreatesDatabase() bool {
	return s.databaseFollowsReadiness() && s.Database != s.engine.serverDatabase
}

// databaseFollowsReadiness reports a URL whose database may not exist when the
// server first answers, so the readiness wait probes the server's own.
func (s Spec) databaseFollowsReadiness() bool {
	return s.fromImage || s.engine.createsDatabase
}

// Env is the container environment that creates the database with password as
// its superuser credential. A docker block's own environment comes first, so
// the variables the provisioner depends on are the ones the container sees.
func (s Spec) Env(password string) []string {
	return append(slices.Clone(s.declaration.Env), s.engine.env(s.Database, password)...)
}

// BaselineURL is the URL a docker block's baseline runs on: the dev database
// the URL names, or the whole server when it names none on the MySQL family,
// with the engine's own parameters only. An operator's `search_path` may name
// a schema the baseline is about to create.
func (s Spec) BaselineURL(hostPort, password string) string {
	return s.engine.url(hostPort, s.Database, password, defaultParams(s.engine.params))
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
//
// # The bytes are read as written
//
// This function normalizes NOTHING. Trimming surrounding whitespace makes it
// accept a value the pinned binary cannot parse at all. Measured on
// the pinned community binary v1.3.0, exit statuses read from unpiped
// `schema inspect -u file://schema.sql --dev-url <value>` invocations:
//
//	<value>                          exit  what it says
//	"docker://postgres/16/dev"          0  provisions and inspects
//	" docker://postgres/16/dev"         1  sql/sqlclient: parse open url:
//	                                       first path segment in URL cannot
//	                                       contain colon
//	"docker://postgres/16/dev "         0  provisions, database "dev "
//
// The rule is not "whitespace is rejected" -- it is plain [url.Parse]. A LEADING
// space makes the whole value a relative path whose first segment is `docker:`,
// so there is no scheme and no docker URL; a TRAILING one is an ordinary
// character in the last path segment and names a database that ends in a space.
// Trimming gets both wrong in the same breath, and the first of them in the one
// direction compatibility policy (a) forbids: `ptah-compat schema inspect`
// exiting 0, having started a container, where the pinned binary exits 1.
//
// A surface that wants to be lenient about whitespace normalizes ONCE at its own
// boundary and says so -- `ptah schema inspect` does exactly that, deliberately,
// for every `--dev-url` value it takes. What it must not do, and what this
// function must not do for it, is normalize a value into a container.
func Parse(rawURL string) (Spec, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return Spec{}, fmt.Errorf("parse docker --dev-url: %w", err)
	}
	if driver, ok := atlasurl.DockerImageDriver(parsed.Scheme); ok {
		return parseImageURL(parsed, driver)
	}
	if parsed.Scheme != Scheme {
		return Spec{}, fmt.Errorf("not a docker --dev-url: %q", rawURL)
	}
	// Matched as written: measured, the pinned binary refuses
	// `docker://POSTGRES/16/dev` and `docker://Postgres/16/dev` with
	// `unsupported docker image`, as it refuses an engine it does not know.
	host := parsed.Host
	found, ok := engines[host]
	dialect, known := atlasurl.DockerEngineDialect(host)
	if !ok || !known {
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
		Dialect:  dialect,
		Image:    found.image + ":" + tag,
		Database: database,
		Query:    query,
		engine:   found,
	}, nil
}

// parseImageURL interprets a `docker+<driver>://` URL, whose path names the
// image to start; see the package documentation for the grammar and its
// measurement.
func parseImageURL(parsed *url.URL, driver string) (Spec, error) {
	found, ok := engines[driver]
	dialect, known := atlasurl.DockerEngineDialect(driver)
	if !ok || !known {
		return Spec{}, fmt.Errorf(
			"docker+%s --dev-url names an engine Ptah does not start from a docker URL;"+
				" pass a directly connectable dev database URL instead",
			driver,
		)
	}
	image, database := splitImagePath(parsed.Host, parsed.Path)
	if image == "" {
		return Spec{}, fmt.Errorf("docker+%s --dev-url names no image", driver)
	}
	if database == "" {
		database = found.serverDatabase
	}
	if strings.Contains(database, "?") {
		return Spec{}, fmt.Errorf("docker --dev-url database name %q contains a query separator", database)
	}
	query, err := mergeParams(found.params, parsed.RawQuery)
	if err != nil {
		return Spec{}, err
	}
	declaration, fromBlock := declared(parsed.Fragment)
	return Spec{
		Engine:      driver,
		Dialect:     dialect,
		Image:       image,
		Database:    database,
		Query:       query,
		engine:      found,
		fromImage:   true,
		declaration: declaration,
		fromBlock:   fromBlock,
	}, nil
}

// splitImagePath reads the image and the database out of a `docker+` URL, as
// the pinned community binary v1.3.0 reads them: the last path segment is the
// database when there are two or more and it holds no colon, the segments
// before it are the image, and a host other than `_` leads the image. A
// database the path does not name is empty.
func splitImagePath(host, urlPath string) (image, database string) {
	segments := strings.Split(strings.TrimPrefix(urlPath, "/"), "/")
	last := len(segments) - 1
	if last > 0 && !strings.Contains(segments[last], ":") {
		database = segments[last]
		segments = segments[:last]
	}
	image = path.Join(segments...)
	if host != "" && host != "_" {
		image = path.Join(host, image)
	}
	return strings.TrimSuffix(image, ":"), database
}

// splitDockerPath reads the tag and database name out of a docker URL path.
//
// One segment is a TAG, not a database: measured, `docker://postgres/dev` makes
// the pinned binary look for the image `postgres:dev`. Reading it as a database
// name would silently run a different image than the operator asked for.
func splitDockerPath(urlPath string) (tag, database string, err error) {
	trimmed := strings.Trim(urlPath, "/")
	if trimmed == "" {
		return DefaultTag, DefaultDatabase, nil
	}
	segments := strings.Split(trimmed, "/")
	if len(segments) > 2 {
		return "", "", fmt.Errorf("docker --dev-url path %q has more than <tag>/<database>", urlPath)
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
