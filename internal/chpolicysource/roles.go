package chpolicysource

import (
	"fmt"
	"regexp"
	"slices"
	"strings"

	"ptah.run/core/ptaherr"
	"ptah.run/dialect/clickhouse/chschema"
)

// roleKeywords are the words a TO clause reads as keywords. A name spelled
// like one is quoted to be a name.
var roleKeywords = []string{"ALL", "EXCEPT", "NONE", "CURRENT_USER"}

// ParseRoles reads a TO clause as ClickHouse writes one: a list of names, ALL,
// ALL EXCEPT a list of names, or NONE. A name is bare, or quoted with
// backticks or double quotes, a doubled quote standing for one; quoted, it is
// a name even when it spells a keyword. Empty and NONE name nobody.
// CURRENT_USER is refused: ClickHouse records the user it resolves to, so a
// declaration holding it would never match what a read reports.
func ParseRoles(text string) (chschema.RoleSelection, error) {
	text = strings.TrimSpace(text)
	if text == "" || strings.EqualFold(text, "NONE") {
		return chschema.RoleSelection{}, nil
	}
	if strings.EqualFold(text, "ALL") {
		return chschema.RoleSelection{All: true}, nil
	}
	if rest, found := cutWords(text, "ALL", "EXCEPT"); found {
		except, err := parseNames(rest)
		if err != nil {
			return chschema.RoleSelection{}, rolesError(text, err)
		}
		return chschema.RoleSelection{All: true, Except: except}, nil
	}
	names, err := parseNames(text)
	if err != nil {
		return chschema.RoleSelection{}, rolesError(text, err)
	}
	return chschema.RoleSelection{Names: names}, nil
}

// FormatRoles writes a selection as the TO clause [ParseRoles] reads back,
// the names in the order they are held. A selection naming nobody is empty.
func FormatRoles(roles chschema.RoleSelection) string {
	quoted := func(names []string) string {
		parts := make([]string, len(names))
		for i, name := range names {
			parts[i] = quoteName(name)
		}
		return strings.Join(parts, ", ")
	}
	switch {
	case roles.All && len(roles.Except) > 0:
		return "ALL EXCEPT " + quoted(roles.Except)
	case roles.All:
		return "ALL"
	default:
		return quoted(roles.Names)
	}
}

// cutWords reports whether text starts with the two keywords, in any letter
// case and separated by white space, and returns what follows them.
func cutWords(text, first, second string) (string, bool) {
	fields := strings.Fields(text)
	if len(fields) < 2 || !strings.EqualFold(fields[0], first) || !strings.EqualFold(fields[1], second) {
		return "", false
	}
	rest := strings.TrimSpace(strings.TrimSpace(text)[len(fields[0]):])
	return rest[len(fields[1]):], true
}

// parseNames splits text at the commas outside quotes and reads each entry
// as one name.
func parseNames(text string) ([]string, error) {
	var names []string
	var current strings.Builder
	var quote rune
	flush := func() error {
		name, err := parseName(strings.TrimSpace(current.String()))
		if err != nil {
			return err
		}
		names = append(names, name)
		current.Reset()
		return nil
	}
	for _, char := range text {
		switch {
		case quote == 0 && (char == '`' || char == '"'):
			quote = char
			current.WriteRune(char)
		case char == quote:
			quote = 0
			current.WriteRune(char)
		case quote == 0 && char == ',':
			if err := flush(); err != nil {
				return nil, err
			}
		default:
			current.WriteRune(char)
		}
	}
	if err := flush(); err != nil {
		return nil, err
	}
	return names, nil
}

// parseName reads one entry: a quoted name, or a bare one that spells no
// keyword. A doubled quote inside a quoted name is one quote.
func parseName(entry string) (string, error) {
	if entry == "" {
		return "", fmt.Errorf("an empty entry")
	}
	if quote := entry[0]; quote == '`' || quote == '"' {
		inner := entry[1:]
		if len(inner) == 0 || inner[len(inner)-1] != quote {
			return "", fmt.Errorf("%s is not one quoted name", entry)
		}
		inner = inner[:len(inner)-1]
		doubled := string([]byte{quote, quote})
		if strings.Contains(strings.ReplaceAll(inner, doubled, ""), string(quote)) || inner == "" {
			return "", fmt.Errorf("%s is not one quoted name", entry)
		}
		return strings.ReplaceAll(inner, doubled, string(quote)), nil
	}
	if slices.ContainsFunc(roleKeywords, func(keyword string) bool { return strings.EqualFold(entry, keyword) }) {
		return "", fmt.Errorf("%s is a keyword; quote it to name a role", entry)
	}
	if !bareName.MatchString(entry) {
		return "", fmt.Errorf("%q must be quoted to be a name", entry)
	}
	return entry, nil
}

// bareName is a name ClickHouse reads unquoted.
var bareName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// quoteName writes a name bare where [parseName] reads it back unchanged, and
// in backticks otherwise.
func quoteName(name string) string {
	if bareName.MatchString(name) && !slices.ContainsFunc(roleKeywords, func(keyword string) bool { return strings.EqualFold(name, keyword) }) {
		return name
	}
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

func rolesError(text string, err error) error {
	return fmt.Errorf("%w: role selection %q: %v", ptaherr.ErrInvalidAttributeValue, text, err)
}
