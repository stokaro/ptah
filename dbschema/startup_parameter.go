package dbschema

import (
	"errors"
	"fmt"
	"strings"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/atlasurl"
	"go.5x5.cz/ptah/internal/schemaselection"
)

// protocolViolationSQLState is what PostgreSQL's wire protocol reports for a
// startup packet the server would not accept. PgBouncer answers it for a
// parameter outside its allow-list.
const protocolViolationSQLState = "08P01"

// explainRefusedStartupParameter turns a connection refused over a startup
// parameter into a message that names what to do about it.
//
// A `?search_path=` on a PostgreSQL URL is not a query the server runs: it is
// a STARTUP parameter, sent in the packet that opens the connection. PgBouncer
// forwards only the parameters on its allow-list and refuses the connection
// outright for anything else, so the operator sees
//
//	FATAL: unsupported startup parameter: search_path (SQLSTATE 08P01)
//
// which is the proxy's message about its own configuration, arriving where the
// operator asked to read a schema. It says nothing about the URL that caused
// it, nothing about which parameter Ptah put there, and nothing about the two
// ways out.
//
// Measured on PgBouncer 1.25.2 in transaction mode: the same URL without
// `search_path` connects and reads normally, and the description it produces
// is identical to the direct one (stokaro/ptah#1029).
//
// The message names no command-line flag on purpose. It is produced by the
// connection layer, which does not know which verb asked -- and the schema
// option is not on every one of them: `ptah schema inspect` has no --schema,
// while the compatibility surface does. A message naming a flag the caller's
// verb never registered is stokaro/ptah#1924's shape, and
// TestExplainRefusedStartupParameter_NamesNoFlag keeps it out.
//
// The recognition is on the SQLSTATE and the parameter name, not on the
// proxy's identity. Ptah does not need to know what sits in front of the
// database -- another pooler with another allow-list produces the same class
// of failure, and the same two ways out apply.
func explainRefusedStartupParameter(dbURL, dialect string, err error) error {
	if err == nil || !platform.IsPostgresFamily(dialect) {
		return err
	}
	parameter := refusedStartupParameter(err)
	if parameter == "" {
		return err
	}
	if strings.TrimSpace(schemaselection.FromURL(dbURL).Raw) == "" || parameter != searchPathParameter {
		return fmt.Errorf(
			"the database URL carries the startup parameter %q, which the server or the proxy in "+
				"front of it refuses: %w", parameter, err)
	}
	return fmt.Errorf(
		"the database URL selects a schema with %q, which is sent as a startup parameter and which "+
			"a transaction-pooling proxy such as PgBouncer refuses rather than ignores: remove it "+
			"from the URL, or configure the proxy to pass it through "+
			"(PgBouncer: track_extra_parameters = search_path): %w",
		parameter, err)
}

// refusedStartupParameter names the parameter a server refused at startup, or
// "" when the error is anything else.
//
// The SQLSTATE alone is not enough: 08P01 covers every protocol violation, and
// answering this message for an unrelated one would explain the wrong thing.
// The parameter name is taken from the message because there is nowhere else
// it appears -- the wire carries one error, and the name is inside it.
func refusedStartupParameter(err error) string {
	var stateErr interface{ SQLState() string }
	if !errors.As(err, &stateErr) || stateErr.SQLState() != protocolViolationSQLState {
		return ""
	}
	const marker = "unsupported startup parameter: "
	_, after, found := strings.Cut(err.Error(), marker)
	if !found {
		return ""
	}
	name, _, _ := strings.Cut(after, " ")
	return strings.TrimSpace(strings.Trim(name, `"`))
}

// urlWithoutSearchPath removes the schema selection from a PostgreSQL URL, so a
// connection refused over the startup parameter can be retried without it.
//
// It answers false whenever there is nothing to retry: another dialect, another
// refused parameter, a URL carrying no selection, or a URL that will not parse.
// The caller then reports the failure it already has rather than opening a
// second connection that fails the same way.
func urlWithoutSearchPath(dbURL, dialect string, err error) (string, bool) {
	if err == nil || !platform.IsPostgresFamily(dialect) {
		return "", false
	}
	if refusedStartupParameter(err) != searchPathParameter {
		return "", false
	}
	if strings.TrimSpace(schemaselection.FromURL(dbURL).Raw) == "" {
		return "", false
	}
	parsed, parseErr := atlasurl.Parse(dbURL)
	if parseErr != nil {
		return "", false
	}
	query := parsed.Query()
	query.Del(searchPathParameter)
	parsed.RawQuery = query.Encode()
	return parsed.String(), true
}

// searchPathParameter is the startup parameter a schema selection travels in.
const searchPathParameter = "search_path"
