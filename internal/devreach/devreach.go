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

	"ptah.run/core/platform/capability"
	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/devdocker"
	"ptah.run/internal/providerurl"
)

// Server opens devURL, the value of the command's --flag, and answers what its
// server can do, or the reason it could not be opened.
//
// A URL of only spaces answers nil and no error: the command was given none.
// Any other URL is taken as written, so a docker:// URL behind a space is no
// docker URL, as it is none to the pinned community binary.
//
// A docker:// URL is parsed and no container is started. Its server is the one
// the image tag names, and a tag no release line matches, such as `latest`,
// answers nil. A docker URL whose engine an executable provider in ctx
// provisions answers nil unparsed: the provider reads the image, not Ptah.
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
// timeout is the error, wrapped as `connect to --<flag>: ...`. A timeout of
// zero leaves the deadline of ctx, if any, as the only bound.
func Server(ctx context.Context, dialect, flag, devURL string, timeout time.Duration) (capability.Capabilities, error) {
	switch {
	case strings.TrimSpace(devURL) == "":
		return capability.Capabilities{}, nil
	case devdocker.IsURL(devURL) && providerProvisions(ctx, devURL):
		return capability.Capabilities{}, nil
	case devdocker.IsURL(devURL):
		spec, err := devdocker.Parse(devURL)
		if err != nil {
			return capability.Capabilities{}, err
		}
		_, tag, tagged := strings.Cut(spec.Image, ":")
		resolution := capability.ResolveServerVersion(dialect, tag)
		if !tagged || !resolution.Recognized {
			return capability.Capabilities{}, nil
		}
		return resolution.Capabilities, nil
	case atlasurl.NamesAFile(devURL):
		return capability.Capabilities{}, nil
	}
	if dialect != "" {
		if err := providerurl.ValidateDialectMatch(ctx, devURL, dialect); err != nil {
			return capability.Capabilities{}, err
		}
	}
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	conn, err := dbschema.ConnectToServer(ctx, devURL)
	if err != nil {
		return capability.Capabilities{}, fmt.Errorf("connect to --%s: %w", flag, err)
	}
	defer func() { _ = conn.Close() }()
	return conn.Info().Capabilities, nil
}

// providerProvisions reports whether an executable provider in ctx starts the
// dev database devURL names.
func providerProvisions(ctx context.Context, devURL string) bool {
	_, owned := devdocker.ExternalEngine(ctx, devURL)
	return owned
}
