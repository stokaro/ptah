package ydbreplication

import (
	"net"
	"net/url"
	"path"
	"strconv"
	"strings"
)

// Endpoint is a connection string as YDB keeps it: the scheme, the address
// and the database, which is how DescribeReplication and DescribeTransfer
// report a connection whatever spelling created it.
type Endpoint struct {
	// Secure is grpcs, TLS.
	Secure bool
	// Address is host:port.
	Address string
	// Database is the database's absolute path, such as /local.
	Database string
}

// String writes the endpoint the way YDB reads it back:
// `grpc://localhost:2136/?database=/local`, measured on 25.1.4.7 and 26.2.1.14
// for a replication created with that string, with ENDPOINT and DATABASE, and
// with the scheme left out.
func (e Endpoint) String() string {
	scheme := "grpc"
	if e.Secure {
		scheme = "grpcs"
	}
	return scheme + "://" + e.Address + "/?database=" + e.Database
}

// ParseConnectionString reads a connection string in the one form Ptah
// declares: `grpc://` or `grpcs://`, a host and a port, and the database in
// the database parameter, with nothing else.
//
// The form is stricter than YDB, deliberately. YDB takes a string with no
// scheme and adds grpc://, and 25.1.4.7 takes `grpc://host:port/local`, which
// 26.2.1.14 refuses (`Database is not specified`), and keeps it as an endpoint
// of `host:port/local` and no database, so the replication fails at its first
// connection. Credentials in the string would be a secret in clear, and YDB
// reads them from secrets.
func ParseConnectionString(text string) (Endpoint, error) {
	invalid := func(reason string) (Endpoint, error) {
		return Endpoint{}, &DeclarationError{Attribute: AttributeConnectionString, Value: text, Reason: reason}
	}
	const form = "takes grpc://host:port/?database=/path or grpcs://host:port/?database=/path"
	parsed, err := url.Parse(text)
	if err != nil {
		return invalid(form)
	}
	var endpoint Endpoint
	switch parsed.Scheme {
	case "grpc":
	case "grpcs":
		endpoint.Secure = true
	default:
		return invalid(form)
	}
	if parsed.User != nil {
		return invalid("carries credentials, which a replication reads from a secret; name the secret with " +
			AttributeTokenSecretName + " or " + AttributeUser + " and " + AttributePasswordSecretName)
	}
	host, port, err := net.SplitHostPort(parsed.Host)
	if number, convErr := strconv.Atoi(port); err != nil || host == "" || convErr != nil || number < 1 || number > 65535 {
		return invalid(form + "; the address needs a host and a port")
	}
	if parsed.Path != "" && parsed.Path != "/" {
		return invalid(form + "; the database goes in the database parameter, since 26.2.1.14 refuses a path " +
			"(`Database is not specified`)")
	}
	if parsed.Fragment != "" || parsed.Opaque != "" {
		return invalid(form)
	}
	query, err := url.ParseQuery(parsed.RawQuery)
	if err != nil || len(query) != 1 || len(query["database"]) != 1 {
		return invalid(form + "; the database parameter is the only one")
	}
	database := query.Get("database")
	if !strings.HasPrefix(database, "/") || path.Clean(database) != database || database == "/" {
		return invalid(form + "; the database is an absolute path such as /local")
	}
	endpoint.Address = parsed.Host
	endpoint.Database = database
	return endpoint, nil
}

// ParseConnection reads a connection string and a credential out of values,
// keyed by attribute name. A credential is a token secret, or a user with a
// password secret, each named by an object secret's name or by a scheme
// secret's path relative to the database the replication runs in; never a
// value, which YDB would keep and never return.
func ParseConnection(values map[string]string) (Connection, error) {
	var connection Connection
	if raw, ok := present(values, AttributeConnectionString); ok {
		if _, err := ParseConnectionString(raw); err != nil {
			return Connection{}, err
		}
		connection.ConnectionString = raw
	}
	fields := []struct {
		attribute string
		target    *string
	}{
		{AttributeTokenSecretName, &connection.TokenSecretName},
		{AttributeTokenSecretPath, &connection.TokenSecretPath},
		{AttributeUser, &connection.User},
		{AttributePasswordSecretName, &connection.PasswordSecretName},
		{AttributePasswordSecretPath, &connection.PasswordSecretPath},
	}
	for _, field := range fields {
		if raw, ok := present(values, field.attribute); ok {
			if raw == "" {
				return Connection{}, &DeclarationError{Attribute: field.attribute,
					Reason: "names nothing; leave the attribute out instead"}
			}
			*field.target = raw
		}
	}
	if err := CheckConnection(connection); err != nil {
		return Connection{}, err
	}
	return connection, nil
}

// CheckConnection holds a connection to the rules [ParseConnection] reads one
// with, so a spec built by hand is refused where a declaration would be.
func CheckConnection(connection Connection) error {
	if connection.ConnectionString != "" {
		if _, err := ParseConnectionString(connection.ConnectionString); err != nil {
			return err
		}
	}
	forms := 0
	for _, set := range []bool{
		connection.TokenSecretName != "", connection.TokenSecretPath != "",
		connection.PasswordSecretName != "", connection.PasswordSecretPath != "",
	} {
		if set {
			forms++
		}
	}
	switch {
	case forms > 1:
		return &DeclarationError{Attribute: "credentials",
			Reason: "names more than one secret; a connection takes one: a token secret, or a user's password " +
				"secret, each by name or by path"}
	case connection.User != "" && connection.PasswordSecretName == "" && connection.PasswordSecretPath == "":
		return &DeclarationError{Attribute: AttributeUser, Value: connection.User,
			Reason: "a user signs in with a password secret; declare " + AttributePasswordSecretName + " or " +
				AttributePasswordSecretPath + " (YDB answers `Neither PASSWORD nor PASSWORD_SECRET_NAME are provided`)"}
	case connection.User == "" && (connection.PasswordSecretName != "" || connection.PasswordSecretPath != ""):
		return &DeclarationError{Attribute: AttributeUser,
			Reason: "a password secret signs in as a user; declare " + AttributeUser}
	case forms > 0 && connection.ConnectionString == "":
		return &DeclarationError{Attribute: AttributeConnectionString,
			Reason: "a credential signs in to another database, which the connection string names; a transfer " +
				"of a topic in its own database takes neither"}
	}
	for _, name := range []struct{ attribute, value string }{
		{AttributeTokenSecretName, connection.TokenSecretName},
		{AttributePasswordSecretName, connection.PasswordSecretName},
	} {
		if strings.HasPrefix(name.value, "/") {
			return &DeclarationError{Attribute: name.attribute, Value: name.value,
				Reason: "a secret name holds no leading slash, which is how YDB tells a secret path from a name " +
					"when it reads one back; name a scheme secret with " + strings.Replace(name.attribute, "_name",
					"_path", 1)}
		}
	}
	for _, secretPath := range []struct{ attribute, value string }{
		{AttributeTokenSecretPath, connection.TokenSecretPath},
		{AttributePasswordSecretPath, connection.PasswordSecretPath},
	} {
		if secretPath.value == "" {
			continue
		}
		if err := checkPath(secretPath.attribute, secretPath.value, relativeOnly); err != nil {
			return err
		}
	}
	return nil
}

// UsesSecretPath reports a connection that names a secret by its path.
func UsesSecretPath(connection Connection) bool {
	return connection.TokenSecretPath != "" || connection.PasswordSecretPath != ""
}

// HasCredentials reports a connection that names any secret or user.
func HasCredentials(connection Connection) bool {
	return connection.TokenSecretName != "" || connection.TokenSecretPath != "" ||
		connection.User != "" || connection.PasswordSecretName != "" || connection.PasswordSecretPath != ""
}

// CanonicalConnectionString writes a connection string as YDB reads it back,
// or returns it unchanged where it does not parse.
func CanonicalConnectionString(text string) string {
	if text == "" {
		return ""
	}
	endpoint, err := ParseConnectionString(text)
	if err != nil {
		return text
	}
	return endpoint.String()
}

// ConnectionDatabase is the database a connection string names, or "" for
// none.
func ConnectionDatabase(text string) string {
	endpoint, err := ParseConnectionString(text)
	if err != nil {
		return ""
	}
	return endpoint.Database
}

// credentialsEqual compares two connections' credentials as YDB keeps them.
func credentialsEqual(a, b Connection) bool {
	return a.TokenSecretName == b.TokenSecretName &&
		cleanRelative(a.TokenSecretPath) == cleanRelative(b.TokenSecretPath) &&
		a.User == b.User &&
		a.PasswordSecretName == b.PasswordSecretName &&
		cleanRelative(a.PasswordSecretPath) == cleanRelative(b.PasswordSecretPath)
}

// pathForm is which spellings of a path an attribute takes.
type pathForm int

const (
	// relativeOnly is a path relative to the database root.
	relativeOnly pathForm = iota
	// relativeOrAbsolute is a path relative to the database root or one
	// starting with a slash, which names the database it lies in.
	relativeOrAbsolute
)

// checkPath refuses a path YDB would read as another one, or that leaves the
// database: an empty segment, `.` or `..`, a directory of the server's own
// (one starting with a dot), and, where form takes only a relative path, a
// leading slash.
func checkPath(attribute, value string, form pathForm) error {
	trimmed := value
	if strings.HasPrefix(value, "/") {
		if form != relativeOrAbsolute {
			return &DeclarationError{Attribute: attribute, Value: value,
				Reason: "takes a path relative to the database root, without a leading slash, so the " +
					"declaration names no database of its own"}
		}
		trimmed = value[1:]
	}
	for segment := range strings.SplitSeq(trimmed, "/") {
		if segment == "" || strings.HasPrefix(segment, ".") {
			return &DeclarationError{Attribute: attribute, Value: value,
				Reason: "takes a path of directory and object names, none empty and none starting with a dot"}
		}
	}
	return nil
}

// cleanRelative is a relative path without a leading or trailing slash.
func cleanRelative(value string) string {
	return strings.Trim(value, "/")
}
