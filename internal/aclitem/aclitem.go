// Package aclitem parses the text form of a PostgreSQL access-control list,
// the value of a column such as pg_default_acl.defaclacl.
//
// The server can explode such a list itself with aclexplode, and PostgreSQL,
// YugabyteDB and CockroachDB v26.2 and later do. CockroachDB v25.4.16 does not:
// it stores defaclacl as text[] rather than aclitem[], and aclexplode over it
// answers no rows, so a read built on it finds no default privilege there
// (stokaro/ptah#3802). The text form is the one every line answers, through
// array_to_json, so the PostgreSQL reader and writer ask for that and explode
// it here.
//
// One element reads `grantee=privileges/grantor`. An empty grantee is PUBLIC.
// Each privilege is one letter, followed by `*` when it carries the grant
// option. PostgreSQL quotes a name holding anything other than letters, digits
// and underscores, doubling a quote inside it; CockroachDB v25.4 does not quote
// at all, and v26.3 quotes as PostgreSQL does. CockroachDB leaves the grantor
// empty. Measured on PostgreSQL 18.6, YugabyteDB 2026.1.2 and CockroachDB
// v25.4.16, v26.2.7 and v26.3.1:
//
//	PostgreSQL 18.6         "\"odd role\"=w*/r_owner", "=r/r_owner", "Upper=X*/r_owner"
//	CockroachDB v25.4.16    "odd-role.x=w*/", "r_reader=ar/"
//	CockroachDB v26.3.1     "\"odd-role.x\"=w*/", "r_reader=ra/"
package aclitem

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// ErrMalformed reports an ACL element that does not have the
// `grantee=privileges/grantor` shape, or names a privilege letter PostgreSQL
// does not define.
var ErrMalformed = errors.New("malformed ACL item")

// Item is one element of an access-control list: the privileges one grantee
// holds, and the role that granted them.
type Item struct {
	// Grantee is the role holding the privileges, or the empty string for
	// PUBLIC.
	Grantee string
	// Privileges are the privileges held, in the order the element lists them.
	Privileges []Privilege
	// Grantor is the role that granted them, or the empty string where the
	// server records none, as CockroachDB does.
	Grantor string
}

// Privilege is one privilege of an [Item] and whether it carries the grant
// option.
type Privilege struct {
	// Name is the privilege as aclexplode names it, such as SELECT or
	// ALTER SYSTEM.
	Name string
	// Grantable reports the grant option, the `*` after the letter.
	Grantable bool
}

// privilegeNames maps each letter to the name aclexplode reports for it. It is
// PostgreSQL 18's whole ACL_ALL_RIGHTS_STR, "arwdDxtXUCTcsAm", so a letter
// outside it is not a privilege this server family defines.
var privilegeNames = map[byte]string{
	'a': "INSERT",
	'r': "SELECT",
	'w': "UPDATE",
	'd': "DELETE",
	'D': "TRUNCATE",
	'x': "REFERENCES",
	't': "TRIGGER",
	'X': "EXECUTE",
	'U': "USAGE",
	'C': "CREATE",
	'T': "TEMPORARY",
	'c': "CONNECT",
	's': "SET",
	'A': "ALTER SYSTEM",
	'm': "MAINTAIN",
}

// ParseJSON parses an access-control list as array_to_json renders it: a JSON
// array of element texts, or null for a column holding no list. JSON rather
// than the array literal, because a quoted name inside an array literal is
// escaped a second time.
func ParseJSON(encoded string) ([]Item, error) {
	var elements []string
	if err := json.Unmarshal([]byte(encoded), &elements); err != nil {
		return nil, fmt.Errorf("%w: %q is not a JSON array of strings: %w", ErrMalformed, encoded, err)
	}
	items := make([]Item, 0, len(elements))
	for _, element := range elements {
		item, err := Parse(element)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, nil
}

// Parse parses one element of an access-control list.
func Parse(text string) (Item, error) {
	grantee, rest, err := readName(text, '=')
	if err != nil {
		return Item{}, fmt.Errorf("%w: %q: %w", ErrMalformed, text, err)
	}
	if !strings.HasPrefix(rest, "=") {
		return Item{}, fmt.Errorf("%w: %q has no '=' after the grantee", ErrMalformed, text)
	}
	letters, grantorText, found := strings.Cut(rest[1:], "/")
	if !found {
		return Item{}, fmt.Errorf("%w: %q has no '/' before the grantor", ErrMalformed, text)
	}
	privileges, err := parsePrivileges(letters)
	if err != nil {
		return Item{}, fmt.Errorf("%w: %q: %w", ErrMalformed, text, err)
	}
	grantor, trailing, err := readName(grantorText, 0)
	if err != nil {
		return Item{}, fmt.Errorf("%w: %q: %w", ErrMalformed, text, err)
	}
	if trailing != "" {
		return Item{}, fmt.Errorf("%w: %q has %q after the grantor", ErrMalformed, text, trailing)
	}
	return Item{Grantee: grantee, Privileges: privileges, Grantor: grantor}, nil
}

// readName reads a role name from the start of text and returns it with what
// follows. A quoted name ends at its closing quote, with a doubled quote inside
// it standing for one. An unquoted name runs to stop, or to the end when stop
// is 0: CockroachDB v25.4 writes names such as odd-role.x unquoted, and no role
// name it accepts holds '=' or '/'.
func readName(text string, stop byte) (name, rest string, err error) {
	if !strings.HasPrefix(text, `"`) {
		if stop == 0 {
			return text, "", nil
		}
		end := strings.IndexByte(text, stop)
		if end < 0 {
			return text, "", nil
		}
		return text[:end], text[end:], nil
	}
	var b strings.Builder
	for i := 1; i < len(text); i++ {
		if text[i] != '"' {
			b.WriteByte(text[i])
			continue
		}
		if i+1 < len(text) && text[i+1] == '"' {
			b.WriteByte('"')
			i++
			continue
		}
		return b.String(), text[i+1:], nil
	}
	return "", "", errors.New("a quoted name is not closed")
}

// parsePrivileges reads the privilege letters between '=' and '/'.
func parsePrivileges(letters string) ([]Privilege, error) {
	privileges := make([]Privilege, 0, len(letters))
	for i := 0; i < len(letters); i++ {
		name, known := privilegeNames[letters[i]]
		if !known {
			return nil, fmt.Errorf("privilege letter %q is not one PostgreSQL defines", letters[i])
		}
		privilege := Privilege{Name: name}
		if i+1 < len(letters) && letters[i+1] == '*' {
			privilege.Grantable = true
			i++
		}
		privileges = append(privileges, privilege)
	}
	return privileges, nil
}
