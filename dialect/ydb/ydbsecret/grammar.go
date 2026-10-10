// Package ydbsecret owns YDB secrets: the desired and observed models, what a
// declaration may say, the statements that create, rotate and drop one, and
// the rule that keeps a secret's value out of every model, file and log. The
// common schema, catalog, AST and diff types hold no secret.
//
// A YDB secret is a scheme object whose value the server keeps and never
// returns: `CREATE SECRET <path> WITH (value = '...')`, `ALTER SECRET` to set a
// new value and `DROP SECRET`. Measured on local-ydb 26.2.1.14:
//
//   - The scheme service lists a secret as an entry of type SECRET, and
//     describing its path returns the entry and its permissions. Nothing
//     returns the value: SELECT, SHOW CREATE TABLE and DROP TABLE on the path
//     answer `Path is not a table or topic`, and the secret service's
//     DescribeSecret answers Unimplemented.
//   - A value is a String literal or a named expression holding one. A query
//     parameter is not: `DECLARE $v AS String; CREATE SECRET s WITH (value =
//     $v)` fails at compile time (`GetParameterValue(): requirement ...
//     failed`), so the value has to be in the query text.
//   - CREATE SECRET honors PRAGMA TablePathPrefix and creates the directories
//     of its path, and DROP SECRET leaves them. A secret cannot take a path
//     another object holds (`unexpected path type`).
//   - 26.2.1.14 has no IF NOT EXISTS, IF EXISTS or OR REPLACE on these
//     statements, though the upstream documentation describes them, and ALTER
//     SECRET takes only the value (`parameter VALUE must be set`).
//
// 25.4.1.15 and later have these statements by default, and 25.3.1.25 behind
// YDB's EnableSchemaSecrets flag. 25.1.4.7 and 25.2.1.24 have only the
// deprecated `CREATE OBJECT name (TYPE SECRET)`, which keeps the value in
// `.metadata/secrets/values` and every value it ever held in
// `values_history`, both readable in clear by the database administrator, and
// which the user who created the secret cannot list. Ptah neither reads nor
// writes that form, and a line without [capability.Secrets] refuses a declared
// secret.
//
// So a declaration never carries the value. It names an environment variable
// whose name starts with [ValuePrefix], every statement Ptah writes refers to
// that variable as the named expression `$<variable>`, and the YDB connection
// defines the expression from the environment as the statement is sent
// (ptah.run/internal/ydbsecretvalue). A migration file, a plan and a log hold
// the variable's name, and a statement run without Ptah fails loudly on an
// unknown name instead of creating a secret with some other value.
//
// The server never returns a value, so a desired and an observed secret
// compare by path alone. A changed value cannot be observed; a plan writes
// ALTER SECRET only for a secret a comparison request names (see
// [RotationRequests]).
//
// Every spelling of a secret is its path relative to the database root, and
// [ParsePath] reads it: a slash separates directories and a dot is part of a
// name, so `pg.pw` is one secret at the root and `ext/pg` is pg in ext.
package ydbsecret

import (
	"errors"
	"fmt"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/internal/sqlident"
)

// ValuePrefix starts the name of every environment variable a secret's value
// may come from. A migration file names the variable, and Ptah reads it on the
// machine that applies the file; the prefix keeps a file from copying an
// arbitrary variable of that machine, such as a cloud credential, into a
// secret an external data source could then send elsewhere. An operator
// exposes a value to Ptah by exporting it under this prefix.
const ValuePrefix = "PTAH_SECRET_"

// The attributes that declare a secret. The annotation parser, the YAML reader
// and the annotation registry read these names.
const (
	// AttributeName is the secret's name, the last segment of its path.
	AttributeName = "name"
	// AttributeSchema is the directory that holds the secret, relative to
	// the database root.
	AttributeSchema = "schema"
	// AttributeValueEnv names the environment variable that holds the value.
	AttributeValueEnv = "value_env"
	// AttributeValue is refused wherever it is written: a schema file never
	// holds a secret's value.
	AttributeValue = "value"
)

// DeclarationError is an attribute whose value a secret declaration cannot
// carry.
type DeclarationError struct {
	// Attribute is the attribute's name.
	Attribute string
	// Reason says what the attribute takes.
	Reason string
}

func (e *DeclarationError) Error() string {
	return fmt.Sprintf("invalid %s: %s", e.Attribute, e.Reason)
}

// LiteralValueRefusal is why a declaration that writes the value is refused.
// It names no value, so it can be printed whatever the declaration held.
const LiteralValueRefusal = "a secret's value is never written in a schema file; name the environment " +
	"variable that holds it with " + AttributeValueEnv

// ParseValueEnv reads the variable a secret's value comes from out of values,
// keyed by attribute name, and refuses a declaration that writes the value
// itself. Every other key is the caller's.
func ParseValueEnv(values map[string]string) (string, error) {
	if _, written := values[AttributeValue]; written {
		return "", &DeclarationError{Attribute: AttributeValue, Reason: LiteralValueRefusal}
	}
	name := strings.TrimSpace(values[AttributeValueEnv])
	if err := CheckValueEnv(name); err != nil {
		return "", err
	}
	return name, nil
}

// CheckValueEnv reports why name cannot be the variable a secret's value comes
// from, or nil: it starts with [ValuePrefix], has a name after it, and is
// spelled as YQL spells a named expression, so `$<name>` refers to it.
func CheckValueEnv(name string) error {
	rest, prefixed := strings.CutPrefix(name, ValuePrefix)
	switch {
	case name == "":
		return &DeclarationError{Attribute: AttributeValueEnv,
			Reason: "a secret names the environment variable that holds its value"}
	case !prefixed || rest == "":
		return &DeclarationError{Attribute: AttributeValueEnv,
			Reason: fmt.Sprintf("%q does not start with %s and a name after it; Ptah reads a secret's value only "+
				"from a variable under that prefix, so a migration file cannot copy any other variable of the "+
				"machine that applies it", name, ValuePrefix)}
	case !variableName(name):
		return &DeclarationError{Attribute: AttributeValueEnv,
			Reason: fmt.Sprintf("%q holds a character other than an ASCII letter, a digit or an underscore", name)}
	default:
		return nil
	}
}

// variableName reports a name of ASCII letters, digits and underscores that
// does not start with a digit.
func variableName(name string) bool {
	for i := range len(name) {
		c := name[i]
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c == '_':
		case c >= '0' && c <= '9' && i > 0:
		default:
			return false
		}
	}
	return name != ""
}

// DuplicateError is a second declaration of one secret path. It wraps
// [schemaext.ErrDuplicate].
type DuplicateError struct {
	// Path is the secret's path relative to the database root.
	Path string
}

func (e *DuplicateError) Error() string { return "secret " + e.Path + " is declared twice" }

// Unwrap returns [schemaext.ErrDuplicate].
func (e *DuplicateError) Unwrap() error { return schemaext.ErrDuplicate }

// Declare returns objects with the secret name in the directory schema added,
// declared by the Go struct holder (empty for every other source format) with
// its value read from valueEnv. Every source format declares a secret through
// it, so they agree on what a declaration may hold and on what a repeated one
// is told. objects is not modified.
//
// The name is one segment of the secret's path: it holds no slash, and a dot
// is part of it. The directory is relative to the database root and takes no
// leading or trailing slash; surrounding space is ignored. A name, directory or
// variable a secret cannot take is refused with a [DeclarationError] naming the
// attribute, and a secret declared twice with a [DuplicateError] naming its
// path. Two declarations that meet only when sources are merged are refused by
// the merge, with [schemaext.ErrDuplicate] and the secret's identity.
func Declare(objects schemaext.Objects, schema, name, holder, valueEnv string) (schemaext.Objects, error) {
	name = strings.TrimSpace(name)
	switch {
	case name == "":
		return objects, &DeclarationError{Attribute: AttributeName, Reason: "a secret needs a name"}
	case strings.Contains(name, "/"):
		return objects, &DeclarationError{Attribute: AttributeName,
			Reason: fmt.Sprintf("%q holds a slash; name the directory with %s", name, AttributeSchema)}
	case ValidateIdentity(Ref("", name)) != nil:
		return objects, &DeclarationError{Attribute: AttributeName, Reason: fmt.Sprintf("%q is not a path segment", name)}
	}
	if err := CheckValueEnv(valueEnv); err != nil {
		return objects, err
	}
	schema = strings.TrimSpace(schema)
	if strings.HasPrefix(schema, "/") {
		return objects, &DeclarationError{Attribute: AttributeSchema,
			Reason: fmt.Sprintf("%q starts with a slash; name the directory relative to the database root, "+
				"without the database's own path", schema)}
	}
	object := DesiredObject(schema, name, holder, valueEnv)
	if ValidateIdentity(object.Ref) != nil {
		return objects, &DeclarationError{Attribute: AttributeSchema,
			Reason: fmt.Sprintf("%q is not a directory path relative to the database root", schema)}
	}
	declared, err := objects.With(object)
	if errors.Is(err, schemaext.ErrDuplicate) {
		return objects, &DuplicateError{Path: Display(object.Ref.Schema.Source, name)}
	}
	if err != nil {
		return objects, err
	}
	return declared, nil
}

// DefaultValueEnv is the variable Ptah names for a secret it did not declare
// itself: one it read from a database and writes out as a declaration, or one
// a rollback creates again. It is [ValuePrefix] followed by the secret's path
// in upper case, with every character a variable name cannot hold written as
// an underscore.
func DefaultValueEnv(schema, name string) string {
	path := strings.Trim(schema, "/")
	if path != "" {
		path += "/"
	}
	path += name
	var b strings.Builder
	b.WriteString(ValuePrefix)
	for _, r := range strings.ToUpper(path) {
		if r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' {
			b.WriteRune(r)
			continue
		}
		b.WriteByte('_')
	}
	return b.String()
}

// Refuse returns a capability error naming subject unless caps holds
// [capability.Secrets]. A target that built nothing for a secret would report
// the declaration applied, and an external data source that names the secret
// would then fail at its first read.
func Refuse(dialect string, caps capability.Capabilities, subject string) error {
	if caps.Has(capability.Secrets) {
		return nil
	}
	normalized := platform.NormalizeDialect(dialect)
	return &ptaherr.CapabilityError{
		Dialect: normalized,
		Feature: string(capability.Secrets),
		Err:     ptaherr.ErrUnsupportedFeature,
		Message: fmt.Sprintf("%s, which requires target capability %s, unavailable on this %s target",
			subject, capability.Secrets, normalized),
	}
}

// Path writes the secret in the directory schema, relative to the database
// root, as one quoted YDB path: `<directory>/<secret>`, or the name alone at
// the root. A dot in either part stays literal.
func Path(schema, name string) string {
	return sqlident.Qualified(platform.YDB, schema, name)
}

// Display names the secret by the path YDB writes for it, unquoted, for
// messages and reports: `dir/name`, or `name` at the database root.
func Display(schema, name string) string {
	if schema == "" {
		return name
	}
	return strings.TrimRight(schema, "/") + "/" + name
}

// Reference is the named expression a statement writes in place of the value
// valueEnv holds.
func Reference(valueEnv string) string {
	return "$" + valueEnv
}

// CreateStatement writes what creates the secret with the value valueEnv
// holds when the statement runs. The secret takes YDB's default permissions:
// it inherits only DESCRIBE SCHEMA from its directory.
func CreateStatement(schema, name, valueEnv string) string {
	return fmt.Sprintf("CREATE SECRET %s WITH (value = %s);", Path(schema, name), Reference(valueEnv))
}

// AlterStatement writes what gives the secret the value valueEnv holds when
// the statement runs.
func AlterStatement(schema, name, valueEnv string) string {
	return fmt.Sprintf("ALTER SECRET %s WITH (value = %s);", Path(schema, name), Reference(valueEnv))
}

// DropStatement writes what drops the secret and its value, which nothing can
// read back.
func DropStatement(schema, name string) string {
	return "DROP SECRET " + Path(schema, name) + ";"
}

// The consequences of the three statements, in the words every report uses:
// a change's effect, a statement's safety assessment and the classification
// of migration SQL text.
const (
	// CreateReason is why creating a secret is additive.
	CreateReason = "CREATE SECRET creates a YDB secret with the value its variable holds when the statement runs"
	// RotateReason is why rotating a secret changes behavior.
	RotateReason = "ALTER SECRET replaces the value every external data source naming the secret uses"
	// DropReason is why dropping a secret is destructive.
	DropReason = "DROP SECRET removes a YDB secret whose value nothing can read back"
)
