package postgres_test

import "regexp"

var simplePostgresIdentifierQuoteRE = regexp.MustCompile(`"([a-z_][a-z0-9_]*)"`)

func legacyPostgresSQL(sql string) string {
	return simplePostgresIdentifierQuoteRE.ReplaceAllString(sql, "$1")
}
