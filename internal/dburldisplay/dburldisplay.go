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
	"unicode"

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

// Format formats a database URL for display, with its password and every
// secret query parameter hidden.
//
// It hides more than a URL parser reads, never less. A password with a
// reserved character written as it is -- `#`, `?`, `/`, or a `%` that starts
// no escape -- makes net/url refuse the URL or read part of the password as a
// host, a path, a fragment or a query, and the text before the last `@` is then
// hidden as credentials. A libpq keyword/value string, which the PostgreSQL drivers
// accept in place of a URL, keeps its text and loses the values of its
// secret keywords.
func Format(dbURL string) string {
	// Handle MySQL/MariaDB URLs specially since they have a different format
	if _, mysqlFamily := atlasurl.CutMySQLScheme(dbURL); mysqlFamily && strings.Contains(dbURL, "@tcp(") {
		// For MySQL/MariaDB URLs like mysql://user:pass@tcp(host:port)/db?params
		// Redact only the leading authority credentials, not DSN-like values in query params.
		return redactURLQuery(mySQLTCPPasswordPattern.ReplaceAllString(dbURL, "$1:***@"))
	}
	if keywords, ok := redactKeywordValue(dbURL); ok {
		return keywords
	}

	parsedURL, err := url.Parse(dbURL)
	if err != nil || misreadCredentials(parsedURL) {
		return redactUnparsedUserInfo(dbURL)
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

// misreadCredentials reports a parse that left an @ where a URL written with
// its password escaped has none: in the host, the path, the fragment, the
// opaque part or a query key. Each is the end of credentials net/url did not
// read as such, because a reserved character in the password ended the
// authority early. An @ in a query value is a value.
func misreadCredentials(parsedURL *url.URL) bool {
	if strings.Contains(parsedURL.Host+parsedURL.Path+parsedURL.Fragment+parsedURL.Opaque, "@") {
		return true
	}
	query, _ := url.ParseQuery(parsedURL.RawQuery)
	for key := range query {
		if strings.Contains(key, "@") {
			return true
		}
	}
	return false
}

// redactUnparsedUserInfo hides the credentials of a URL net/url cannot read
// as written: everything between the scheme and the last `@` when that text
// holds a password, that is a `:`. The last `@` rather than the first, since a
// password may hold one too. A query the text still has is redacted as the
// MySQL tcp() form's is.
func redactUnparsedUserInfo(dbURL string) string {
	scheme, rest, found := strings.Cut(dbURL, "://")
	if !found {
		return dbURL
	}
	at := strings.LastIndex(rest, "@")
	if at < 0 {
		return redactURLQuery(dbURL)
	}
	user, _, hasPassword := strings.Cut(rest[:at], ":")
	if !hasPassword {
		return redactURLQuery(dbURL)
	}
	return redactURLQuery(scheme + "://" + user + ":***@" + rest[at+1:])
}

// keywordValuePair is one `keyword=value` setting of a libpq connection
// string, with where its value starts and ends.
var keywordValuePair = regexp.MustCompile(`^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*('(?:[^'\\]|\\.)*'|(?:[^\s'\\]|\\.)*)`)

// redactKeywordValue hides the secret values of a libpq keyword/value
// connection string, such as `host=db user=app password=s3cret`, and keeps
// the rest of the text as it was written. It answers false for anything that
// does not read as such a string from end to end, a URL included.
func redactKeywordValue(dsn string) (string, bool) {
	if strings.TrimSpace(dsn) == "" || strings.Contains(dsn, "://") {
		return "", false
	}
	var out strings.Builder
	rest := dsn
	for strings.TrimSpace(rest) != "" {
		match := keywordValuePair.FindStringSubmatchIndex(rest)
		if match == nil {
			return "", false
		}
		end := match[1]
		if end < len(rest) && !unicode.IsSpace(rune(rest[end])) {
			return "", false
		}
		if isSecretQueryParam(rest[match[2]:match[3]]) {
			out.WriteString(rest[:match[4]] + redactedQueryValue)
		} else {
			out.WriteString(rest[:end])
		}
		rest = rest[end:]
	}
	out.WriteString(rest)
	return out.String(), true
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
