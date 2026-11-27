// Package devreach opens the dev database a command was given, for the commands
// that would otherwise never open it.
//
// A command that takes --dev-url and plans without it still owes the operator a
// check that the URL works: a typo in it would otherwise pass CI and fail the
// first run that needs the database. The pinned Atlas community binary v1.3.0
// connects to the dev database on every `schema diff`, `schema apply` and
// `migrate diff` it is given one, and fails with the driver's error when nothing
// answers, measured with a PostgreSQL URL on a closed port:
//
//	Error: postgres: scanning system variables: dial tcp 127.0.0.1:1: connect: connection refused
//
// Both of Ptah's command trees call [Server] where they take the URL, so the
// native and the compatible commands refuse the same URLs, each in its own
// words. They differ on where the URL came from. ptah-compat opens every dev
// URL it is given, from a flag or from atlas.hcl, as the Atlas CLI does. A
// native command opens one only when the operator gave it for this run, as the
// flag or its PTAH_* variable: a project file's `dev:` key is a default for
// every command in the project, and a command that does not use the database
// has no reason to fail on it.
package devreach

import (
	"context"
	"fmt"
	"strings"
	"time"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/devdocker"
	"ptah.run/internal/providerurl"
)

// Server opens devURL and answers what its server can do, or the reason it
// could not be opened. source names devURL in an error the way the operator
// gave it, such as `--dev-url` or the dev database a project file names.
//
// A URL of only spaces answers nil and no error: the command was given none.
// Any other URL is taken as written, so a docker:// URL behind a space is no
// docker URL, as it is none to the pinned community binary.
//
// A docker URL whose engine an executable provider in ctx provisions answers
// nil unparsed, since the provider reads the image, not Ptah; the engine is
// still checked against dialect. Both forms name the engine, as
// `docker://<engine>/<tag>` and `docker+<engine>://...`, and
// [devdocker.ExternalEngine] reads both, so it is asked first: a
// `docker+<engine>` scheme of an engine Ptah does not start is no docker URL
// to [devdocker.IsURL].
//
// Any other docker URL is parsed and no container is started. Its server is
// the one the image tag names, and a tag no release line matches, such as
// `latest`, answers nil.
//
// A URL naming a SQLite file answers nil without opening it, since opening it
// creates the file and the engine is the one compiled into Ptah. A dev path
// that does not exist is therefore not refused here.
//
// Any other URL is checked against dialect when dialect is not empty, before
// anything is connected, in the words of [providerurl.ValidateDialectMatch], so
// a scheme an executable provider in ctx serves names that provider's target.
// It is then connected to and closed again, through that provider where one
// serves it, and a connection that fails within
// timeout is the error, wrapped as `connect to <source>: ...`. A timeout of
// zero leaves the deadline of ctx, if any, as the only bound.
func Server(ctx context.Context, dialect, source, devURL string, timeout time.Duration) (capability.Capabilities, error) {
	facts, err := reach(ctx, dialect, source, devURL, timeout, nil)
	return facts.Capabilities, err
}

// Facts is what a dev server answers for two schema files planned for it.
type Facts struct {
	// Capabilities is what the server can do, as [Server] answers it.
	Capabilities capability.Capabilities
	// CharsetDefaultCollations and CharsetAliases are the character set facts
	// of the session the URL opens, in the shape of
	// [ptah.run/catalog.Database.CharsetDefaultCollations] and
	// [ptah.run/catalog.Database.CharsetAliases]. A prediction of what the
	// server builds from a schema file reads them in place of the dialect
	// descriptor's static table. They are nil where nothing was connected to,
	// and where the dialect's descriptor states no character set defaults or
	// its reader cannot read them alone.
	CharsetDefaultCollations map[string]string
	CharsetAliases           map[string]string
	// IdentifierSemantics is how the server compares names, as the resolve
	// function given to [ServerFacts] read it over the connection, and nil
	// where that function was nil or nothing was connected to.
	IdentifierSemantics *identifier.Semantics
}

// Resolve asks a connected dev server how it compares names; see
// [ServerFacts].
type Resolve func(context.Context, *dbschema.DatabaseConnection) (identifier.Semantics, error)

// ServerFacts opens devURL as [Server] does and answers its capabilities and,
// over the same connection, the session's character set facts. A failed read
// of the facts is the error, wrapped as `read the character sets of
// <source>: ...`.
//
// Where resolve is not nil and a connection was opened, it is called over
// that connection too, and its answer is [Facts.IdentifierSemantics]; an
// error from it is the error, wrapped as `resolve identifiers on <source>:
// ...`.
func ServerFacts(ctx context.Context, dialect, source, devURL string, timeout time.Duration, resolve Resolve) (Facts, error) {
	return reach(ctx, dialect, source, devURL, timeout, func(ctx context.Context, conn *dbschema.DatabaseConnection, facts Facts) (Facts, error) {
		if resolve != nil {
			semantics, err := resolve(ctx, conn)
			if err != nil {
				return Facts{}, fmt.Errorf("resolve identifiers on %s: %w", source, err)
			}
			facts.IdentifierSemantics = &semantics
		}
		described, _ := platform.Lookup(conn.Info().Dialect)
		reader, reads := conn.Reader().(charsetFactsReader)
		if described.Columns.CollationNames.CharsetDefaults == nil || !reads {
			return facts, nil
		}
		var err error
		facts.CharsetDefaultCollations, facts.CharsetAliases, err = reader.ReadCharsetFacts(ctx)
		if err != nil {
			return Facts{}, fmt.Errorf("read the character sets of %s: %w", source, err)
		}
		return facts, nil
	})
}

// charsetFactsReader reads its session's character set facts without reading
// a schema; the MySQL-family reader is one.
type charsetFactsReader interface {
	ReadCharsetFacts(ctx context.Context) (defaults, aliases map[string]string, err error)
}

// reach is [Server] where read is nil. Where it is not, read adds to the
// capabilities what it reads over the connection reach opened, and is not
// called where reach opens none.
func reach(
	ctx context.Context,
	dialect, source, devURL string,
	timeout time.Duration,
	read func(context.Context, *dbschema.DatabaseConnection, Facts) (Facts, error),
) (Facts, error) {
	if strings.TrimSpace(devURL) == "" {
		return Facts{}, nil
	}
	if engine, provisioned := devdocker.ExternalEngine(ctx, devURL); provisioned {
		if dialect != "" && !devdocker.SameEngine(engine, dialect) {
			return Facts{}, fmt.Errorf("--dev-url dialect %q does not match --url dialect %q", engine, dialect)
		}
		return Facts{}, nil
	}
	switch {
	case devdocker.IsURL(devURL):
		spec, err := devdocker.Parse(devURL)
		if err != nil {
			return Facts{}, err
		}
		_, tag, tagged := strings.Cut(spec.Image, ":")
		resolution := capability.ResolveServerVersion(dialect, tag)
		if !tagged || !resolution.Recognized {
			return Facts{}, nil
		}
		return Facts{Capabilities: resolution.Capabilities}, nil
	case atlasurl.NamesAFile(devURL):
		return Facts{}, nil
	}
	if dialect != "" {
		if err := providerurl.ValidateDialectMatch(ctx, devURL, dialect); err != nil {
			return Facts{}, err
		}
	}
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	conn, err := dbschema.ConnectToServer(ctx, devURL)
	if err != nil {
		return Facts{}, fmt.Errorf("connect to %s: %w", source, err)
	}
	defer func() { _ = conn.Close() }()
	facts := Facts{Capabilities: conn.Info().Capabilities}
	if read == nil {
		return facts, nil
	}
	return read(ctx, conn, facts)
}
