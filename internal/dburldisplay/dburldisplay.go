// Package dburldisplay renders a database URL for display, with every secret
// it may carry redacted.
//
// It is a display concern and nothing else: the grammar of database URLs --
// parsing, dialect detection, endpoint comparison -- lives in
// internal/atlasurl, and connecting lives in dbschema. What this package owns
// is the one transformation both of those must never perform: rewriting the
// URL so a password, token or key cannot reach a terminal, a log line or an
// error message.
package dburldisplay

import (
	"net/url"
	"regexp"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/ydburl"
)

const redactedQueryValue = "redacted"

// mySQLTCPPasswordPattern matches the leading credentials of a MySQL-family
// URL in go-sql-driver's tcp() form. It accepts any scheme because Format asks
// atlasurl.CutMySQLScheme first, and that predicate is the one the connector
// asks: a spelling written here as well would have to be added twice, and the
// one that was missed would print its password.
var mySQLTCPPasswordPattern = regexp.MustCompile(`^([^:/?#]+://[^:@/?#]+):([^@/?#]+)@`)

// Format formats a database URL for display (hiding secrets).
func Format(dbURL string) string {
	// Handle MySQL/MariaDB URLs specially since they have a different format
	if _, mysqlFamily := atlasurl.CutMySQLScheme(dbURL); mysqlFamily && strings.Contains(dbURL, "@tcp(") {
		// For MySQL/MariaDB URLs like mysql://user:pass@tcp(host:port)/db?params
		// Redact only the leading authority credentials, not DSN-like values in query params.
		return redactURLQuery(mySQLTCPPasswordPattern.ReplaceAllString(dbURL, "$1:***@"))
	}

	parsedURL, err := url.Parse(dbURL)
	if err != nil {
		return dbURL
	}
	parsedURL.RawQuery = redactRawQuery(parsedURL.RawQuery, urlValuedParameter(parsedURL.Scheme))

	// Hide password
	if parsedURL.User != nil {
		if _, hasPassword := parsedURL.User.Password(); hasPassword {
			return formatURLWithRedactedUserPassword(parsedURL)
		}
	}

	return parsedURL.String()
}

func formatURLWithRedactedUserPassword(parsedURL *url.URL) string {
	displayURL := *parsedURL
	username := displayURL.User.Username()
	displayURL.User = nil

	prefix := displayURL.Scheme + "://"
	base := strings.TrimPrefix(displayURL.String(), prefix)
	return prefix + username + ":***@" + base
}

func redactURLQuery(displayURL string) string {
	prefix, rawQuery, ok := strings.Cut(displayURL, "?")
	if !ok {
		return displayURL
	}

	query, fragment, hasFragment := strings.Cut(rawQuery, "#")
	redactedQuery := redactRawQuery(query, noURLValuedParameter)
	if redactedQuery == "" {
		if hasFragment {
			return prefix + "#" + fragment
		}
		return prefix
	}

	result := prefix + "?" + redactedQuery
	if hasFragment {
		result += "#" + fragment
	}
	return result
}

func redactRawQuery(rawQuery string, urlValued func(key string) bool) string {
	if rawQuery == "" {
		return ""
	}

	query, err := url.ParseQuery(rawQuery)
	if err != nil {
		return ""
	}
	for key, values := range query {
		switch {
		case isSecretQueryParam(key):
			for idx := range values {
				values[idx] = redactedQueryValue
			}
		case urlValued(key):
			for idx := range values {
				values[idx] = redactUserInfo(values[idx])
			}
		}
	}
	return query.Encode()
}

// urlValuedParameter reports, for a URL of the given scheme, which query
// parameters hold a URL of their own. A YDB URL names the cluster's monitoring
// endpoint that way, and a command prints the URL before internal/ydburl
// parses it and refuses a user in that endpoint, so the display has to hide
// the user itself. The parameter name is matched in any case: the display may
// hide more than the parser reads, never less.
func urlValuedParameter(scheme string) func(key string) bool {
	if platform.NormalizeDialect(scheme) != platform.YDB {
		return noURLValuedParameter
	}
	return func(key string) bool {
		return strings.EqualFold(key, ydburl.MonitoringParameter)
	}
}

// noURLValuedParameter is the answer for a scheme whose parameters hold no
// URL.
func noURLValuedParameter(string) bool { return false }

// redactUserInfo hides the user info of a URL held in a parameter, the user
// name included: the endpoint takes no credential of its own, so none of it is
// needed to read the line, and a user name can itself be a token. A value
// that carries an @ where no user info can be read -- it does not parse, or it
// has no authority -- is hidden whole, because where its credentials end
// cannot be told.
func redactUserInfo(value string) string {
	parsed, err := url.Parse(value)
	switch {
	case err == nil && parsed.User != nil:
		parsed.User = url.User(redactedQueryValue)
		return parsed.String()
	case err == nil && parsed.Opaque == "":
		return value
	case strings.Contains(value, "@"):
		return redactedQueryValue
	default:
		return value
	}
}

func isSecretQueryParam(key string) bool {
	switch strings.ToLower(key) {
	case "access_token",
		"api_key",
		"apikey",
		"aws_secret_access_key",
		"aws_session_token",
		"client_secret",
		"id_token",
		"password",
		"passwd",
		"private_key",
		"pwd",
		"refresh_token",
		"secret",
		"sslcert",
		"sslkey",
		"sslpassword",
		"token":
		return true
	default:
		return false
	}
}
