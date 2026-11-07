// Package dburldisplay renders a database URL for display, with every secret
// it may carry redacted.
//
// It is a display concern and nothing else: the grammar of database URLs --
// parsing, dialect detection, endpoint comparison -- lives in
// internal/atlasurl, and connecting lives in dbschema. What this package owns
// is the one transformation both of those must never perform: rewriting the
// URL so a password, token or key cannot reach a terminal, a log line or an
// error message. It reads an address as text and asks internal/atlasurl
// nothing, because atlasurl sits above it and names a URL in its errors through
// it.
package dburldisplay

import (
	"cmp"
	"errors"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"unicode"

	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/descriptortable"
)

const (
	redactedQueryValue = "redacted"
	redactedPassword   = "***"
	// shortestSecretPart is the length below which [RedactError] leaves a part
	// of a hidden value in a message: taking out every occurrence of a character
	// or two leaves a message nobody can read.
	shortestSecretPart = 3
)

// schemeWord is what can stand before a "://" as a scheme in a display: one word,
// including one net/url refuses as a scheme, since the password behind it is no
// less a password.
var schemeWord = regexp.MustCompile(`^[^\s:/?#@]+$`)

// driverNetwork is the network and address of go-sql-driver's grammar with the
// "@" that ends the credentials before it, in the places the grammar has them:
// before the "/" of the database, or at the end of a DSN that names none.
var driverNetwork = regexp.MustCompile(`@[A-Za-z0-9_+.-]*\([^()@]*\)(?:/|$)`)

// assignmentList is the start of a connection string that names its settings,
// as libpq's keyword/value strings and SQL Server's ADO and ODBC strings do: a
// name and an "=" before anything else, with `odbc:` allowed in front.
var assignmentList = regexp.MustCompile(`(?i)^\s*(?:odbc:)?\s*[a-z_][a-z0-9_ ]*=`)

// assignmentName is one name of such a string, with what ends the setting
// before it.
var assignmentName = regexp.MustCompile(`(?i)(?:^|[;&\s:])\s*([a-z_][a-z0-9_ ]*?)\s*=\s*`)

// keywordValuePair is one `keyword=value` setting of a libpq connection
// string, with where its value starts and ends.
var keywordValuePair = regexp.MustCompile(`^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*('(?:[^'\\]|\\.)*'|(?:[^\s'\\]|\\.)*)`)

// fileSchemes are the schemes of a URL that names a file
// (descriptor.URLs.FileSchemes), whose text may hold an "@" and a ":" with no
// credentials near them.
var fileSchemes = descriptortable.NewIndex(func(dialects []descriptor.Dialect) map[string]bool {
	schemes := make(map[string]bool)
	for _, described := range dialects {
		for _, scheme := range described.URLs.FileSchemes {
			schemes[scheme] = true
		}
	}
	return schemes
})

// errUnreadableCredentials is the reason [ParseURL] gives for a URL net/url
// refuses only because of what [Format] hides.
var errUnreadableCredentials = errors.New("the user name or password cannot be read as written; " +
	"percent-encode the characters a URL reserves, such as @ : / ? # and %")

// Format formats a database URL for display, with its password and every
// secret query parameter hidden.
//
// It hides more than a URL parser reads, never less. A password with a
// reserved character written as it is -- `#`, `?`, `/`, or a `%` that starts
// no escape -- makes net/url refuse the URL or read part of the password as a
// host, a path, a fragment or a query, and the text before the last `@` is then
// hidden as credentials. A go-sql-driver DSN, user:pass@tcp(host:3306)/db, loses
// its password behind any scheme or none. A libpq keyword/value string, which
// the PostgreSQL drivers accept in place of a URL, and SQL Server's ADO and ODBC
// strings keep their text and lose the values of their secret keywords.
func Format(dbURL string) string {
	var hidden hiddenText
	return hidden.format(dbURL)
}

// ParseURL is [url.Parse] for a database URL: its error repeats nothing [Format]
// hides.
//
// net/url's error quotes the URL it refused and often part of it, and `invalid
// port ":hunter" after host` is the start of a password with a "#" in it. The
// error here is a *url.Error naming the URL as Format does, with the reason
// net/url gives for that displayed URL, so whatever the reason quotes is text
// the display already shows. When the displayed URL parses, what net/url refused
// was in what Format hid, and the reason says so without quoting it.
func ParseURL(rawURL string) (*url.URL, error) {
	parsed, err := url.Parse(rawURL)
	if err == nil {
		return parsed, nil
	}
	display := Format(rawURL)
	reason := errUnreadableCredentials
	if _, displayErr := url.Parse(display); displayErr != nil {
		reason = errors.Unwrap(displayErr)
	}
	return nil, &url.Error{Op: "parse", URL: display, Err: reason}
}

// RedactError returns err with what [Format] hides in dbURL taken out of its
// text, for an error from something that was handed the address: a driver, or a
// server asked for what the driver read.
//
// A driver's error can quote the address, and it can quote what the address read
// as. With a "/" in the password, `postgres://app:12345/Xk9@db/shop` is host
// app, port 12345 and a database called `Xk9@db/shop`, which pgx names when it
// fails to connect. So each part of a hidden value is taken out as well as the
// whole.
//
// The result wraps err, so errors.Is and errors.As answer as they did. An error
// that holds none of it is returned as it is, and a nil err is nil.
func RedactError(dbURL string, err error) error {
	if err == nil {
		return nil
	}
	text := err.Error()
	redacted := text
	for _, secret := range secretParts(dbURL) {
		redacted = strings.ReplaceAll(redacted, secret, redactedPassword)
	}
	if redacted == text {
		return err
	}
	return redactedError{text: redacted, err: err}
}

// Secrets returns each value [Format] hides in dbURL, as it is written there and
// unescaped, longest first so that replacing them in that order leaves no part of
// a longer one behind.
func Secrets(dbURL string) []string {
	var hidden hiddenText
	hidden.format(dbURL)
	secrets := make([]string, 0, 2*len(hidden))
	for _, value := range hidden {
		secrets = append(secrets, value)
		if unescaped, err := url.QueryUnescape(value); err == nil && unescaped != value {
			secrets = append(secrets, unescaped)
		}
	}
	return longestFirst(secrets)
}

// longestFirst sorts and compacts values.
func longestFirst(values []string) []string {
	slices.SortFunc(values, func(left, right string) int {
		return cmp.Or(cmp.Compare(len(right), len(left)), cmp.Compare(left, right))
	})
	return slices.Compact(values)
}

// redactedError is an error with text taken out of its message.
type redactedError struct {
	text string
	err  error
}

func (e redactedError) Error() string { return e.text }
func (e redactedError) Unwrap() error { return e.err }

// hiddenText collects what a display left out.
type hiddenText []string

// hide records a value the display does not show.
func (h *hiddenText) hide(value string) {
	if value != "" {
		*h = append(*h, value)
	}
}

// secretParts is every secret of dbURL and each piece of it worth looking for,
// longest first so that a piece cannot break up a whole.
func secretParts(dbURL string) []string {
	var parts []string
	for _, secret := range Secrets(dbURL) {
		parts = append(parts, secret)
		for piece := range strings.FieldsFuncSeq(secret, stopsAPassword) {
			if len(piece) >= shortestSecretPart {
				parts = append(parts, piece)
			}
		}
	}
	return longestFirst(parts)
}

// stopsAPassword reports a character a parser may stop reading a password at, so
// that what follows it can surface on its own.
func stopsAPassword(r rune) bool {
	return unicode.IsSpace(r) || strings.ContainsRune(`@/:?#%&=;'"`, r)
}

// format renders dbURL for display, recording what it leaves out.
func (h *hiddenText) format(dbURL string) string {
	if keywords, ok := h.keywordValue(dbURL); ok {
		return keywords
	}
	prefix, rest, hasScheme := cutScheme(dbURL)
	if !hasScheme {
		return h.schemeless(dbURL)
	}
	scheme := strings.TrimSpace(strings.TrimSuffix(prefix, "://"))
	if strings.HasPrefix(rest, "tcp(") || strings.HasPrefix(rest, "unix(") || driverNetwork.MatchString(rest) {
		return prefix + h.driverDSN(rest, urlValuedParameter(scheme))
	}

	parsedURL, err := url.Parse(dbURL)
	if err != nil || misreadCredentials(parsedURL) {
		return prefix + h.unparsedCredentials(scheme, rest)
	}
	parsedURL.RawQuery = h.rawQuery(parsedURL.RawQuery, urlValuedParameter(parsedURL.Scheme))

	// Hide password
	if parsedURL.User != nil {
		if password, hasPassword := parsedURL.User.Password(); hasPassword {
			h.hide(password)
			return formatURLWithRedactedUserPassword(parsedURL)
		}
	}

	return parsedURL.String()
}

// cutScheme splits an address after its "://", when what stands before that is
// a scheme. A "://" after anything else is part of the text: `app:p://w@h/db` is
// a DSN with a password in it.
func cutScheme(dbURL string) (prefix, rest string, ok bool) {
	scheme, rest, found := strings.Cut(dbURL, "://")
	if !found || !schemeWord.MatchString(strings.TrimSpace(scheme)) {
		return "", dbURL, false
	}
	return scheme + "://", rest, true
}

// schemeless renders an address with no scheme to read it by.
//
// net/url reads `app:secret@tcp(h)/db` as a scheme named after the user and an
// opaque rest, so its parse says nothing here and the shape of the text decides.
// An "@" is read as go-sql-driver's DSN, in the form it takes behind a scheme or
// none, unless the text names a file: a file's name may hold an "@" and a ":"
// with no credentials near them.
func (h *hiddenText) schemeless(address string) string {
	switch {
	case assignmentList.MatchString(address):
		return h.assignments(address)
	case driverNetwork.MatchString(address), strings.Contains(address, "@") && !namesAFile(address):
		return h.driverDSN(address, noURLValuedParameter)
	default:
		return h.urlQuery(address, everyParameter)
	}
}

// namesAFile reports text that names a file by a scheme with no "://" or by a
// path: a word before its first ":" that is a file scheme, a "/" or a "\\" before
// that ":", or a drive letter. An address starting with "//" has only a host and
// credentials to name.
func namesAFile(address string) bool {
	word, afterColon, hasColon := strings.Cut(address, ":")
	switch {
	case strings.HasPrefix(address, "//"):
		return false
	case strings.ContainsAny(word, `/\`), fileSchemes.Get()[strings.ToLower(word)]:
		return true
	default:
		return hasColon && len(word) == 1 && (strings.HasPrefix(afterColon, `\`) || strings.HasPrefix(afterColon, "/"))
	}
}

// misreadCredentials reports a parse that left an @ where a URL written with
// its password escaped has none: in the host, the path, the fragment, the
// opaque part or a query key. Each is the end of credentials net/url did not
// read as such, because a reserved character in the password ended the
// authority early. An @ in a query value is a value. The keys are read from the
// raw text, because a query that does not parse says no more about its keys than
// one that does.
func misreadCredentials(parsedURL *url.URL) bool {
	if strings.Contains(parsedURL.Host+parsedURL.Path+parsedURL.Fragment+parsedURL.Opaque, "@") {
		return true
	}
	for pair := range strings.SplitSeq(parsedURL.RawQuery, "&") {
		key, _, _ := strings.Cut(pair, "=")
		if strings.Contains(key, "@") || strings.Contains(strings.ToLower(key), "%40") {
			return true
		}
	}
	return false
}

// unparsedCredentials hides the credentials of a URL net/url cannot read as
// written, given what follows its scheme: everything up to the last `@` when
// that text holds a password, that is a `:`. The last `@` rather than the first,
// since a password may hold one too. A query the text still has is redacted as a
// driver DSN's is.
//
// With no `@`, net/url refused what it read as a port. Text after a `:` that is
// not a number may be a password with no host behind it, so it goes whole. A URL
// naming a file is left alone: it has no credentials to find there, and its `:`
// may be a drive letter's.
func (h *hiddenText) unparsedCredentials(scheme, rest string) string {
	urlValued := urlValuedParameter(scheme)
	at := strings.LastIndex(rest, "@")
	if at < 0 {
		host, afterColon, hasColon := strings.Cut(rest, ":")
		port, _, _ := strings.Cut(afterColon, "/")
		if !hasColon || isNumber(port) || fileSchemes.Get()[strings.ToLower(scheme)] {
			return h.urlQuery(rest, urlValued)
		}
		h.hide(afterColon)
		return host + ":" + redactedPassword
	}
	user, password, hasPassword := strings.Cut(rest[:at], ":")
	if !hasPassword {
		return h.urlQuery(rest, urlValued)
	}
	h.hide(password)
	return user + ":" + redactedPassword + "@" + h.afterCredentials(rest[at+1:], urlValued)
}

// afterCredentials renders what follows the "@" of an address net/url could not
// read. A "#" or a "%" that starts no escape has no place in a host, a port or a
// database someone wrote out, so it says the "@" was in the password and nothing
// ended it, and the text goes whole.
func (h *hiddenText) afterCredentials(text string, urlValued func(key string) bool) string {
	address, _, _ := strings.Cut(text, "?")
	if _, err := url.PathUnescape(address); err != nil || strings.Contains(address, "#") {
		h.hide(text)
		return redactedPassword
	}
	return h.urlQuery(text, urlValued)
}

// isNumber reports whether text is digits and nothing else.
func isNumber(text string) bool {
	return text != "" && strings.Trim(text, "0123456789") == ""
}

// driverDSN hides the password of a DSN in go-sql-driver's grammar,
// [user[:password]@][net[(addr)]]/dbname[?params], and the secret parameters of
// its query. The password runs from the first ":" to the "@"
// [driverCredentialsEnd] finds.
func (h *hiddenText) driverDSN(dsn string, urlValued func(key string) bool) string {
	at := driverCredentialsEnd(dsn)
	if at < 0 {
		return h.urlQuery(dsn, urlValued)
	}
	user, password, hasPassword := strings.Cut(dsn[:at], ":")
	if !hasPassword {
		return dsn[:at+1] + h.urlQuery(dsn[at+1:], urlValued)
	}
	h.hide(password)
	if driverNetwork.MatchString(dsn) {
		return user + ":" + redactedPassword + "@" + h.urlQuery(dsn[at+1:], urlValued)
	}
	return user + ":" + redactedPassword + "@" + h.afterCredentials(dsn[at+1:], urlValued)
}

// driverCredentialsEnd finds the "@" that ends the credentials of a DSN, or -1.
//
// go-sql-driver reads it as the last "@" before the last "/", so a password may
// hold "@" and "/" and a database or a parameter may hold "@". That reading is
// not enough here, because a display also meets a DSN the driver refuses: with
// no database, the last "/" may be in the password, and an "@" before it too. So
// the last "@" a network and address follow ends the credentials, unless the
// driver reads a later one. Where no network follows one, only what follows the
// last "@" of the text is sure not to be password, and a database or parameter
// holding an "@" hides more than it had to.
func driverCredentialsEnd(dsn string) int {
	networks := driverNetwork.FindAllStringIndex(dsn, -1)
	if len(networks) == 0 {
		return strings.LastIndex(dsn, "@")
	}
	network := networks[len(networks)-1][0]
	if slash := strings.LastIndex(dsn, "/"); slash >= 0 {
		return max(network, strings.LastIndex(dsn[:slash], "@"))
	}
	return network
}

// keywordValue hides the secret values of a libpq keyword/value connection
// string, such as `host=db user=app password=s3cret`, and keeps the rest of the
// text as it was written. It answers false for anything that does not read as
// such a string from end to end, a URL included. A ";" or "&" in a value that is
// not a secret answers false too, since the string then reads as settings that
// end there, as in SQL Server's `server=h;user id=app;password=s3cret`.
func (h *hiddenText) keywordValue(dsn string) (string, bool) {
	if strings.TrimSpace(dsn) == "" {
		return "", false
	}
	var out strings.Builder
	var secrets []string
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
		value := rest[match[4]:end]
		switch {
		case IsSecretParameter(rest[match[2]:match[3]]):
			secrets = append(secrets, value)
			out.WriteString(rest[:match[4]] + redactedQueryValue)
		case strings.ContainsAny(value, ";&"):
			return "", false
		default:
			out.WriteString(rest[:end])
		}
		rest = rest[end:]
	}
	out.WriteString(rest)
	for _, secret := range secrets {
		h.hide(secret)
	}
	return out.String(), true
}

// assignments hides the secret values of a connection string that names its
// settings and does not read as libpq's from end to end: SQL Server's ADO and
// ODBC strings, or a keyword/value string with an error in it, such as a quote
// that never closes.
func (h *hiddenText) assignments(text string) string {
	var out strings.Builder
	rest := text
	for {
		match := assignmentName.FindStringSubmatchIndex(rest)
		if match == nil {
			out.WriteString(rest)
			return out.String()
		}
		name := rest[match[2]:match[3]]
		out.WriteString(rest[:match[1]])
		rest = rest[match[1]:]
		if IsSecretParameter(name) {
			end := assignmentValueEnd(rest)
			h.hide(rest[:end])
			out.WriteString(redactedQueryValue)
			rest = rest[end:]
		}
	}
}

// assignmentValueEnd is where the value at the start of text ends: at the next
// ";" outside the quotes or braces it may open, or at the end of the text. A
// quote that never closes hides what follows it, because nothing says where the
// value stops.
func assignmentValueEnd(text string) int {
	opened := 0
	if text != "" {
		closer := map[byte]byte{'{': '}', '\'': '\'', '"': '"'}[text[0]]
		for i := 1; closer != 0 && opened == 0; i++ {
			switch {
			case i >= len(text):
				return len(text)
			case text[i] != closer:
			case i+1 < len(text) && text[i+1] == closer:
				i++
			default:
				opened = i
			}
		}
	}
	if end := strings.IndexByte(text[opened:], ';'); end >= 0 {
		return opened + end
	}
	return len(text)
}

func formatURLWithRedactedUserPassword(parsedURL *url.URL) string {
	displayURL := *parsedURL
	username := displayURL.User.Username()
	displayURL.User = nil

	prefix := displayURL.Scheme + "://"
	base := strings.TrimPrefix(displayURL.String(), prefix)
	return prefix + username + ":" + redactedPassword + "@" + base
}

// urlQuery redacts the query of text that is not parsed as a URL: what follows
// its first "?", up to a "#".
func (h *hiddenText) urlQuery(text string, urlValued func(key string) bool) string {
	prefix, rawQuery, ok := strings.Cut(text, "?")
	if !ok {
		return text
	}

	query, fragment, hasFragment := strings.Cut(rawQuery, "#")
	redactedQuery := h.rawQuery(query, urlValued)
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

// rawQuery redacts a query, with the secrets of [IsSecretParameter] and the
// user info of the parameters urlValued names. A query that does not parse is
// left out whole, since nothing says what it holds.
func (h *hiddenText) rawQuery(rawQuery string, urlValued func(key string) bool) string {
	if rawQuery == "" {
		return ""
	}

	query, err := url.ParseQuery(rawQuery)
	if err != nil {
		h.hide(rawQuery)
		return ""
	}
	for key, values := range query {
		switch {
		case IsSecretParameter(key):
			for idx := range values {
				h.hide(values[idx])
				values[idx] = redactedQueryValue
			}
		case urlValued(key):
			for idx := range values {
				values[idx] = h.userInfo(values[idx])
			}
		}
	}
	return query.Encode()
}

// urlValuedParameter reports, for a URL of the given scheme, which query
// parameters hold a URL of their own: the ones the scheme's dialect
// descriptor names, matched in any case. A scheme the process registry does
// not describe belongs to an engine an executable provider serves, whose
// parameters this package does not know, so every parameter of it is read as
// one that may hold a URL: a YDB URL names the cluster's monitoring endpoint
// that way, and a command prints the URL before the provider parses it. The
// display may hide more than the parser reads, never less.
func urlValuedParameter(scheme string) func(key string) bool {
	described, found := platform.Lookup(scheme)
	if !found {
		return everyParameter
	}
	if len(described.URLValuedParameters) == 0 {
		return noURLValuedParameter
	}
	return func(key string) bool {
		return slices.ContainsFunc(described.URLValuedParameters, func(parameter string) bool {
			return strings.EqualFold(key, parameter)
		})
	}
}

// noURLValuedParameter is the answer for a scheme whose parameters hold no
// URL.
func noURLValuedParameter(string) bool { return false }

// everyParameter is the answer for a scheme whose parameters this package does
// not know.
func everyParameter(string) bool { return true }

// userInfo hides the user info of a URL held in a parameter, the user name
// included: the endpoint takes no credential of its own, so none of it is needed
// to read the line, and a user name can itself be a token. A value that carries
// an @ where no user info can be read -- it does not parse, or it has no
// authority -- is hidden whole, because where its credentials end cannot be
// told.
func (h *hiddenText) userInfo(value string) string {
	parsed, err := url.Parse(value)
	switch {
	case err == nil && parsed.User != nil && !misreadCredentials(parsed):
		h.hide(parsed.User.String())
		parsed.User = url.User(redactedQueryValue)
		return parsed.String()
	case err == nil && parsed.User == nil && parsed.Opaque == "" && !misreadCredentials(parsed):
		return value
	case strings.Contains(value, "@"):
		h.hide(value)
		return redactedQueryValue
	default:
		return value
	}
}

// secretParameterWords mark a parameter as a secret wherever they stand in its
// name.
var secretParameterWords = []string{"apikey", "credential", "jwt", "passhash", "passwd", "password", "pwd", "secret", "token"}

// IsSecretParameter reports a query parameter or connection-string keyword that
// holds a secret or says where one is.
//
// The name is compared without case or punctuation, so `authToken`, `auth_token`
// and go-ora's `WALLET PASSWORD` read alike, and a name ending in `key` or `pass`
// counts, as `sslkey` and go-ora's `OS PASS` do. It errs on the wide side: an
// engine's parameter a narrower list missed prints its value, and hiding a value
// that is no secret costs a word on a line.
func IsSecretParameter(key string) bool {
	name := strings.Map(func(r rune) rune {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			return unicode.ToLower(r)
		}
		return -1
	}, key)
	switch {
	case name == "sslcert", name == "oshash", strings.HasSuffix(name, "key"), strings.HasSuffix(name, "pass"):
		return true
	default:
		return slices.ContainsFunc(secretParameterWords, func(word string) bool { return strings.Contains(name, word) })
	}
}
