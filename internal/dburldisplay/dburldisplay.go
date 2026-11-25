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
	"ptah.run/internal/secretmask"
)

const (
	redactedQueryValue = "redacted"
	redactedPassword   = "***"
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

// serverOption is one run-time parameter in the value of libpq's `options`
// setting, `-c name=value` or `--name=value`, at its start, after a space, or
// after the quote a keyword/value string opens the setting with. Its value runs
// to the next space or quote that no backslash escapes, as libpq splits the
// setting.
var serverOption = regexp.MustCompile(`(?:^|[\s'])(?:-c\s*|--)([A-Za-z_][A-Za-z0-9_.$-]*)=((?:\\.|[^\s\\'])*)`)

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
//
// A hidden value the address repeats elsewhere, as a database, a host or a user
// named like the password, is taken out there too, by the rule [RedactError]
// takes it out of a driver's error with: a value of five characters or more
// wherever it occurs, a shorter one only where it stands as a credential, and a
// piece of one where it stands as a word. So the display and the error a driver
// answers with hide the same values of the address. The scheme, the names of
// the settings and the placeholder "redacted" are the exception: they are words
// of the grammar rather than values, and the display keeps their text, since a
// password taken out of `client_secret` would show by the gap which word it
// is. A driver's error is free text and takes such a password out wherever it
// stands, so a password that is one of these words can be told by setting the
// two lines side by side.
func Format(dbURL string) string {
	display, hidden := Layout(dbURL)
	return hidden.maskRepeats(display)
}

// Hidden is what [Format] hides in one address: each value as it is written
// and unescaped, each piece of the password a parser may name on its own, and
// the user names beside a hidden password.
type Hidden struct {
	secrets []string
	pieces  []string
	users   []string
}

// Layout returns the address [Format] shows with its password and its secret
// settings hidden, before Format takes a hidden value out of the other parts
// that repeat it, and what it hid. A caller that lays out the parts of an
// address itself, as an Atlas-shaped URL does, parses the display and takes
// what was hidden out of each part with [Hidden.Mask]: a part masked in the
// text Format returns can stop it parsing, as a port that repeats the password
// does.
func Layout(dbURL string) (string, Hidden) {
	var hidden hiddenText
	display := hidden.format(dbURL)
	secrets, pieces := hidden.parts()
	return display, Hidden{secrets: secrets, pieces: pieces, users: spellings(hidden.users)}
}

// Mask takes what h holds out of text, a part of the address, by the rule
// [Format] takes it out of the parts it shows with, writing replacement in its
// place; see [secretmask.MaskPieces].
func (h Hidden) Mask(text, replacement string) string {
	if len(h.secrets) == 0 {
		return text
	}
	return secretmask.MaskPieces(text, h.secrets, h.pieces, h.users, replacement)
}

// grammarWords matches what a display shows that is no value of the address:
// the scheme, the name of each setting with its "=" where a setting starts --
// at the start, or after a space, a "?", a "&" or a ";" -- and the placeholder
// a hidden setting shows, which a secret that is part of the word must not
// cut. A word that ends in "=" anywhere else, as a database or a password in
// base64 does, is a value.
var grammarWords = regexp.MustCompile(`^[^\s:/?#@]+://|(?:^|[\s?&;])[A-Za-z0-9_.%+-]+=|` + redactedQueryValue)

// maskRepeats takes what h holds out of display where it repeats it, as
// [Hidden.Mask] does, outside what [grammarWords] matches.
func (h Hidden) maskRepeats(display string) string {
	if len(h.secrets) == 0 {
		return display
	}
	var out strings.Builder
	last := 0
	for _, word := range grammarWords.FindAllStringIndex(display, -1) {
		out.WriteString(h.Mask(display[last:word[0]], redactedPassword))
		out.WriteString(display[word[0]:word[1]])
		last = word[1]
	}
	out.WriteString(h.Mask(display[last:], redactedPassword))
	return out.String()
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
// A value is taken out as [secretmask.MaskPieces] takes a secret and its pieces
// out. With the password "x", `database=execute` keeps its word.
//
// The result wraps err, so errors.Is and errors.As answer as they did. An error
// that holds none of it is returned as it is, and a nil err is nil.
func RedactError(dbURL string, err error) error {
	if err == nil {
		return nil
	}
	text := err.Error()
	_, hidden := Layout(dbURL)
	redacted := hidden.Mask(text, redactedPassword)
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
	return spellings(hidden.values)
}

// Users returns the user name dbURL gives with the password [Format] hides, as
// it is written there and unescaped, longest first. [secretmask.Mask] takes a
// short secret out after it and a ":".
func Users(dbURL string) []string {
	var hidden hiddenText
	hidden.format(dbURL)
	return spellings(hidden.users)
}

// spellings is each of values as it is and unescaped, longest first.
func spellings(values []string) []string {
	spelled := make([]string, 0, len(values))
	for _, value := range values {
		spelled = append(spelled, value)
		if unescaped, err := url.QueryUnescape(value); err == nil && unescaped != value {
			spelled = append(spelled, unescaped)
		}
	}
	return longestFirst(spelled)
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

// hiddenText collects what a display left out, and the user names it showed
// beside a hidden password.
type hiddenText struct {
	values    []string
	passwords []string
	users     []string
}

// hide records a value the display does not show.
func (h *hiddenText) hide(value string) {
	if value != "" {
		h.values = append(h.values, value)
	}
}

// hidePassword records the password of the address's user info, or the text
// it may run into where nothing ends it, as a value the display does not show.
// It is the one value a driver may misread, so it is the one whose pieces are
// looked for as well.
func (h *hiddenText) hidePassword(value string) {
	h.hide(value)
	if value != "" {
		h.passwords = append(h.passwords, value)
	}
}

// name records the user name of a password the display hides.
func (h *hiddenText) name(user string) {
	if user != "" {
		h.users = append(h.users, user)
	}
}

// parts is every value h hid, as it is written and unescaped, and apart from
// them each piece of the password of the user info that a parser may name on
// its own: what lies between the characters it may stop reading a password at.
// A parser reads a setting's value to its end, so a path a secret setting
// names, such as an sslkey file, gives no pieces, and a directory named like
// the user keeps the user's name in the display.
func (h *hiddenText) parts() (secrets, pieces []string) {
	secrets = spellings(h.values)
	for _, secret := range spellings(h.passwords) {
		for piece := range strings.FieldsFuncSeq(secret, stopsAPassword) {
			if piece != secret {
				pieces = append(pieces, piece)
			}
		}
	}
	return secrets, pieces
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
			h.hidePassword(password)
			h.name(parsedURL.User.Username())
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
		h.hidePassword(afterColon)
		return host + ":" + redactedPassword
	}
	user, password, hasPassword := strings.Cut(rest[:at], ":")
	if !hasPassword {
		return h.urlQuery(rest, urlValued)
	}
	h.hidePassword(password)
	h.name(user)
	return user + ":" + redactedPassword + "@" + h.afterCredentials(rest[at+1:], urlValued)
}

// afterCredentials renders what follows the "@" of an address net/url could not
// read. A "#" or a "%" that starts no escape has no place in a host, a port or a
// database someone wrote out, so it says the "@" was in the password and nothing
// ended it, and the text goes whole.
func (h *hiddenText) afterCredentials(text string, urlValued func(key string) bool) string {
	address, _, _ := strings.Cut(text, "?")
	if _, err := url.PathUnescape(address); err != nil || strings.Contains(address, "#") {
		h.hidePassword(text)
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
	h.hidePassword(password)
	h.name(user)
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
	var secrets, users []string
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
		case secretmask.IsSecretParameter(rest[match[2]:match[3]]):
			secrets = append(secrets, value)
			out.WriteString(rest[:match[4]] + redactedQueryValue)
		case strings.EqualFold(rest[match[2]:match[3]], serverOptionsSetting):
			shown, hidden := serverOptions(value)
			secrets = append(secrets, hidden...)
			out.WriteString(rest[:match[4]] + shown)
		case strings.ContainsAny(value, ";&"):
			return "", false
		default:
			if secretmask.IsUserParameter(rest[match[2]:match[3]]) {
				users = append(users, strings.Trim(value, `'"`))
			}
			out.WriteString(rest[:end])
		}
		rest = rest[end:]
	}
	out.WriteString(rest)
	for _, secret := range secrets {
		h.hide(secret)
	}
	for _, user := range users {
		h.name(user)
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
		if secretmask.IsUserParameter(name) {
			h.name(strings.Trim(rest[:assignmentValueEnd(rest)], `'"{}`))
		}
		if secretmask.IsSecretParameter(name) {
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

// rawQuery redacts a query, with the secrets of [secretmask.IsSecretParameter]
// and the user info of the parameters urlValued names. A query that does not parse is
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
		case secretmask.IsSecretParameter(key):
			for idx := range values {
				h.hide(values[idx])
				values[idx] = redactedQueryValue
			}
		case strings.EqualFold(key, serverOptionsSetting):
			for idx := range values {
				shown, hidden := serverOptions(values[idx])
				for _, secret := range hidden {
					h.hide(secret)
				}
				values[idx] = shown
			}
		case urlValued(key):
			for idx := range values {
				values[idx] = h.userInfo(values[idx])
			}
		}
	}
	return query.Encode()
}

// serverOptionsSetting is libpq's setting that passes run-time parameters to
// the server, in a URL's query or a keyword/value string.
const serverOptionsSetting = "options"

// serverOptions returns the value of libpq's `options` setting with the value
// of every secret-named parameter in it hidden, and the values it hid.
// PostgreSQL takes a custom parameter there, such as
// `-c app.jwt_secret=...`, and a parameter is a secret by its name as a
// query parameter is, by [secretmask.IsSecretParameter].
func serverOptions(value string) (shown string, hidden []string) {
	var out strings.Builder
	last := 0
	for _, match := range serverOption.FindAllStringSubmatchIndex(value, -1) {
		if match[4] == match[5] || !secretmask.IsSecretParameter(value[match[2]:match[3]]) {
			continue
		}
		hidden = append(hidden, value[match[4]:match[5]])
		out.WriteString(value[last:match[4]] + redactedQueryValue)
		last = match[5]
	}
	out.WriteString(value[last:])
	return out.String(), hidden
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
