// Package pgprivilege says what the PostgreSQL privilege keyword ALL names on
// each kind of object a grant can target.
//
// The answer is needed in two places that have to agree: the SQL schema reader
// expands ALL when it folds GRANT and REVOKE statements, and the grant
// comparator decides whether a declared ALL is held by the privileges a
// catalog read reports, one row per privilege. Both ask here.
//
// The lists are measured with aclexplode after GRANT ALL on each kind. A
// table's ALL depends on the server version: PostgreSQL 17 added MAINTAIN, and
// PostgreSQL 16 refuses the word (`unrecognized privilege type "maintain"`).
// So [All] names every privilege ALL grants on the newest release, and
// [Portable] names the ones every supported release grants.
package pgprivilege

import "strings"

// all is what ALL names for each target kind on PostgreSQL 17 and later. The
// plural keys are the object classes of ALTER DEFAULT PRIVILEGES, which name
// the same lists for the objects they cover.
var all = map[string][]string{
	"TABLE":     {"SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER", "MAINTAIN"},
	"SCHEMA":    {"USAGE", "CREATE"},
	"SEQUENCE":  {"USAGE", "SELECT", "UPDATE"},
	"FUNCTION":  {"EXECUTE"},
	"PROCEDURE": {"EXECUTE"},
	"ROUTINE":   {"EXECUTE"},
	"TABLES":    {"SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER", "MAINTAIN"},
	"SEQUENCES": {"USAGE", "SELECT", "UPDATE"},
	"FUNCTIONS": {"EXECUTE"},
	"ROUTINES":  {"EXECUTE"},
	"TYPES":     {"USAGE"},
	"SCHEMAS":   {"USAGE", "CREATE"},
}

// versionDependent are the privileges some supported release does not have.
var versionDependent = map[string]bool{"MAINTAIN": true}

// All returns every privilege ALL names on objectType, the keyword a grant
// uses for its target, in any case. It returns nil for a kind it does not
// know, and a new slice each time.
func All(objectType string) []string {
	return append([]string(nil), all[strings.ToUpper(strings.TrimSpace(objectType))]...)
}

// Portable returns the privileges ALL names on objectType on every supported
// PostgreSQL release: [All] without the ones a release added. A role holding
// these holds ALL as far as a comparison that cannot see the server version
// can tell.
func Portable(objectType string) []string {
	var portable []string
	for _, privilege := range All(objectType) {
		if !versionDependent[privilege] {
			portable = append(portable, privilege)
		}
	}
	return portable
}
