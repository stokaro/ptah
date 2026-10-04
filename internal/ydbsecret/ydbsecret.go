// Package ydbsecret owns the rules of a YDB secret: what a declaration may
// say, the statements that create, rotate and drop one, and how a secret's
// value reaches the server without Ptah ever writing, printing or reading it.
//
// A YDB secret is a scheme object whose value the server keeps and never
// returns: `CREATE SECRET <path> WITH (value = '...')`, `ALTER SECRET` to set a
// new value and `DROP SECRET`. Measured on local-ydb 26.2.1.14:
//
//   - The scheme service lists a secret as an entry of type SECRET, and
//     describing its path returns the entry and its permissions. Nothing
//     returns the value: SELECT, SHOW CREATE and DROP TABLE on the path answer
//     `Path is not a table or topic`, and the secret service's DescribeSecret
//     answers Unimplemented.
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
// 25.1.4.7 to 25.4.1.15 have only the deprecated `CREATE OBJECT name (TYPE
// SECRET)`, which keeps the value in `.metadata/secrets/values` and every value
// it ever held in `values_history`, both readable in clear by the database
// administrator, and not listable by the user who created the secret.
// Ptah neither reads nor writes that form, and a line without
// [capability.Secrets] refuses a declared secret.
//
// So a declaration never carries the value. It names an environment variable
// whose name starts with [ValuePrefix], every statement Ptah writes refers to
// that variable as the named expression `$<variable>`, and the YDB connection
// defines the expression from the environment just before the statement is
// sent, through [Expand]. A migration file, a plan and a log hold the
// variable's name, and a statement run without Ptah fails loudly on an unknown
// name instead of creating a secret with some other value.
package ydbsecret

import (
	"fmt"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/sqlident"
	"ptah.run/internal/tableref"
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

// CheckName reports why name cannot be a secret's name, or nil. A name is one
// path segment; the directories of a path are the secret's schema.
func CheckName(name string) error {
	switch {
	case strings.TrimSpace(name) == "":
		return &DeclarationError{Attribute: AttributeName, Reason: "a secret needs a name"}
	case strings.Contains(name, "/"):
		return &DeclarationError{Attribute: AttributeName,
			Reason: fmt.Sprintf("%q holds a slash; name the directory with %s", name, AttributeSchema)}
	default:
		return nil
	}
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

// Refusal says why a secret cannot be written on a target: Key is the
// capability it needs and the target lacks, or empty when the declaration is
// wrong whatever the target, and Reason then says why.
type Refusal struct {
	// Subject names what is refused.
	Subject string
	// Key is the capability the target lacks.
	Key capability.Capability
	// Reason is why the declaration is refused, for a refusal without a key.
	Reason string
}

// Check reports why the secret name, a canonical reference, cannot be created
// or rotated with its value taken from valueEnv on a target holding caps, or
// nil. An empty valueEnv checks a drop, which needs no value.
func Check(name, valueEnv string, caps capability.Capabilities) *Refusal {
	subject := "secret " + name
	if !caps.Has(capability.Secrets) {
		return &Refusal{Subject: subject, Key: capability.Secrets}
	}
	if strings.TrimSpace(name) == "" {
		return &Refusal{Subject: "a secret", Reason: "a secret needs a name"}
	}
	if valueEnv == "" {
		return nil
	}
	if err := CheckValueEnv(valueEnv); err != nil {
		return &Refusal{Subject: subject, Reason: err.Error()}
	}
	return nil
}

// Path writes name, a secret's canonical reference, as one quoted YDB path:
// `<directory>/<secret>`, or the name alone at the database root.
func Path(name string) string {
	ref, ok := tableref.Parse(name)
	if !ok {
		return sqlident.Quote(platform.YDB, name)
	}
	return sqlident.Qualified(platform.YDB, ref.Schema, ref.Name)
}

// Reference is the named expression a statement writes in place of the value
// valueEnv holds.
func Reference(valueEnv string) string {
	return "$" + valueEnv
}

// CreateStatement writes what creates the secret name with the value valueEnv
// holds when the statement runs. The secret takes YDB's default permissions:
// it inherits only DESCRIBE SCHEMA from its directory.
func CreateStatement(name, valueEnv string) string {
	return fmt.Sprintf("CREATE SECRET %s WITH (value = %s);", Path(name), Reference(valueEnv))
}

// AlterStatement writes what gives the secret name the value valueEnv holds
// when the statement runs.
func AlterStatement(name, valueEnv string) string {
	return fmt.Sprintf("ALTER SECRET %s WITH (value = %s);", Path(name), Reference(valueEnv))
}

// DropStatement writes what drops the secret name and its value, which nothing
// can read back.
func DropStatement(name string) string {
	return "DROP SECRET " + Path(name) + ";"
}
