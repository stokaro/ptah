package schemaprep

import "strings"

// UniqueIndexMethodRefused reports whether the server of dialect builds no
// UNIQUE index with the access method method, as its descriptor states
// ([descriptor.Preparation.NonUniqueIndexMethods]). The method is matched
// without regard to case or surrounding space, the way the server reads an
// unquoted one. An empty method is the server's default, which builds a
// unique index on every engine, and a dialect the registry does not describe
// refuses none.
func UniqueIndexMethodRefused(dialect, method string) bool {
	method = strings.TrimSpace(method)
	if method == "" {
		return false
	}
	for _, refused := range describe(dialect).preparation.NonUniqueIndexMethods {
		if strings.EqualFold(refused, method) {
			return true
		}
	}
	return false
}
