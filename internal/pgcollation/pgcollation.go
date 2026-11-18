// Package pgcollation reads and writes the collation a PostgreSQL column
// names.
//
// A column's collation reaches the schema model from several places: the SQL
// frontend, which reads `COLLATE "C"` or `COLLATE app."MixedCase"`, an HCL or
// YAML document, which states a name, and the catalog reader. The model holds
// one spelling of it, and the renderer and the comparison read that spelling
// here, so a collation the frontend wrote and one the reader wrote compare
// equal when they name one collation.
//
// The model spelling is the collation's name, exactly as the catalog holds it,
// for a collation found without a schema: `C`, `en_US.utf8`, `MixedCase`. A
// collation that names its schema, and a name that holds a double quote, are
// written as SQL with each part quoted: `"app"."MixedCase"`. A double quote is
// what tells the two apart, so a name such as `en_US.utf8` is one name and
// never a schema and a name.
package pgcollation

import (
	"errors"
	"fmt"
	"strings"
)

// ErrInvalidName is the error every malformed collation name wraps.
var ErrInvalidName = errors.New("invalid collation name")

// Name is a collation a column names. Schema is empty for a collation the
// server finds through the search path, pg_catalog's among them.
type Name struct {
	Schema string
	Name   string
}

// catalogSchema is the schema of the collations PostgreSQL ships.
const catalogSchema = "pg_catalog"

// Read reads the model spelling of a collation. A value with no double quote
// is a name as written; a value with one is read as SQL, the way
// [FromWritten] reads the parts of a COLLATE clause.
func Read(model string) (Name, error) {
	if model == "" {
		return Name{}, fmt.Errorf("%w: the name is empty", ErrInvalidName)
	}
	if !strings.Contains(model, `"`) {
		return Name{Name: model}, nil
	}
	parts, err := splitWritten(model)
	if err != nil {
		return Name{}, err
	}
	return FromWritten(parts...)
}

// FromWritten reads a collation name from the parts a COLLATE clause wrote,
// the schema first where there is one. A quoted part is the name inside the
// quotes, with each doubled quote read as one; an unquoted part is folded to
// lower case, as PostgreSQL folds an unquoted identifier. One part or two are
// accepted.
func FromWritten(parts ...string) (Name, error) {
	if len(parts) == 0 || len(parts) > 2 {
		return Name{}, fmt.Errorf("%w: %q names %d parts, and a collation is a name or a schema and a name",
			ErrInvalidName, strings.Join(parts, "."), len(parts))
	}
	read := make([]string, len(parts))
	for i, part := range parts {
		name, err := readPart(part)
		if err != nil {
			return Name{}, err
		}
		read[i] = name
	}
	if len(read) == 1 {
		return Name{Name: read[0]}, nil
	}
	return Name{Schema: read[0], Name: read[1]}, nil
}

// Model is the spelling the schema model holds: the name alone where no schema
// is named, and both parts quoted otherwise. A name that holds a double quote
// is quoted too, since a quote is what marks the quoted spelling.
func (n Name) Model() string {
	if n.Schema == "" && !strings.Contains(n.Name, `"`) {
		return n.Name
	}
	return n.SQL()
}

// SQL is the collation as a COLLATE clause writes it, each part quoted.
func (n Name) SQL() string {
	if n.Schema == "" {
		return quote(n.Name)
	}
	return quote(n.Schema) + "." + quote(n.Name)
}

// Same reports whether a declared collation is the one a catalog read names.
// A declaration that names no schema is the collation of that name the
// server found, in whichever schema it was. A read that names no schema is
// pg_catalog's, where the collations PostgreSQL ships are.
func Same(declared, read Name) bool {
	if declared.Name != read.Name {
		return false
	}
	if declared.Schema == "" {
		return true
	}
	readSchema := read.Schema
	if readSchema == "" {
		readSchema = catalogSchema
	}
	return declared.Schema == readSchema
}

func quote(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// readPart reads one part as written.
func readPart(part string) (string, error) {
	if part == "" {
		return "", fmt.Errorf("%w: a part of the name is empty", ErrInvalidName)
	}
	if !strings.HasPrefix(part, `"`) {
		if strings.ContainsAny(part, "\". \t\r\n[]`'") {
			return "", fmt.Errorf("%w: %q is not one identifier", ErrInvalidName, part)
		}
		return foldASCII(part), nil
	}
	if len(part) < 2 || !strings.HasSuffix(part, `"`) {
		return "", fmt.Errorf("%w: %s is not one quoted identifier", ErrInvalidName, part)
	}
	inner := part[1 : len(part)-1]
	if strings.ReplaceAll(inner, `""`, "") != strings.ReplaceAll(inner, `"`, "") {
		return "", fmt.Errorf("%w: %s holds a quote that is not doubled", ErrInvalidName, part)
	}
	name := strings.ReplaceAll(inner, `""`, `"`)
	if name == "" {
		return "", fmt.Errorf("%w: %s is a zero-length name", ErrInvalidName, part)
	}
	return name, nil
}

// splitWritten splits SQL text at the dots outside quotes.
func splitWritten(written string) ([]string, error) {
	var parts []string
	var current strings.Builder
	quoted := false
	for i := 0; i < len(written); i++ {
		char := written[i]
		switch {
		case char == '"' && quoted && i+1 < len(written) && written[i+1] == '"':
			current.WriteString(`""`)
			i++
		case char == '"':
			quoted = !quoted
			current.WriteByte(char)
		case char == '.' && !quoted:
			parts = append(parts, current.String())
			current.Reset()
		default:
			current.WriteByte(char)
		}
	}
	if quoted {
		return nil, fmt.Errorf("%w: %s has no closing quote", ErrInvalidName, written)
	}
	return append(parts, current.String()), nil
}

// foldASCII folds A to Z to lower case and leaves every other byte alone, as
// PostgreSQL folds an unquoted identifier in a multibyte encoding.
func foldASCII(name string) string {
	var folded strings.Builder
	folded.Grow(len(name))
	for i := range len(name) {
		char := name[i]
		if 'A' <= char && char <= 'Z' {
			char += 'a' - 'A'
		}
		folded.WriteByte(char)
	}
	return folded.String()
}
