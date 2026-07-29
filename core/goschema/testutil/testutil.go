// Package testutil provides shared utilities for testing Go schema parsing
package testutil

import (
	"os"
	"path/filepath"
	"testing"
)

// CreateTempGoFile creates a temporary Go file with the given content for testing
func CreateTempGoFile(t *testing.T, content string) string {
	t.Helper()

	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "test.go")

	err := os.WriteFile(tempFile, []byte(content), 0600)
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	return tempFile
}

// RemoveTempFile removes a temporary file (though TempDir automatically cleans up)
func RemoveTempFile(t *testing.T, filename string) {
	t.Helper()
	// TempDir automatically cleans up, but we can be explicit
	os.Remove(filename)
}

// BuildGoStructWithTag builds a Go struct definition with the given struct tag
func BuildGoStructWithTag(packageName, structName, fieldName, fieldType, tag string) string {
	return `package ` + packageName + `

type ` + structName + ` struct {
	//ptah:schema:field name="` + fieldName + `" type="` + fieldType + `" primary="true"
	` + fieldName + ` int64 ` + "`" + tag + "`" + `
}`
}

// BuildGoStructWithRLS builds a Go struct with RLS annotations
func BuildGoStructWithRLS(packageName, tableName, structName, policyName, rlsFor, rlsTo, rlsUsing, comment string) string {
	result := `package ` + packageName + `

//ptah:schema:rls:enable table="` + tableName + `" comment="Enable RLS for ` + tableName + `"
//ptah:schema:rls:policy name="` + policyName + `" table="` + tableName + `" for="` + rlsFor + `" to="` + rlsTo + `" using="` + rlsUsing + `" comment="` + comment + `"
//ptah:schema:table name="` + tableName + `"
type ` + structName + ` struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64 ` + "`json:\"id\" db:\"id\"`" + `
}`

	return result
}
